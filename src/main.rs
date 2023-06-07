use std::collections::HashMap;
use std::env;
use std::{pin::Pin, sync::Arc, vec};

use bollard::Docker;
use bytes::Bytes;
use clap::Parser;
use futures::io::AsyncRead;
use futures_core::Stream;
use futures_util::TryStreamExt;
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use ring::digest::{Context, SHA256};
use tokio::io::BufReader;
use tokio_util::compat::FuturesAsyncReadCompatExt;
use tokio_util::io::ReaderStream;

#[derive(Parser, Clone)]
struct Cli {
    #[clap(short, long)]
    source_image: String,

    #[clap(short, long)]
    registry_url: String,

    #[clap(short, long)]
    image_name: String,

    #[clap(long)]
    image_tag: String,

    #[clap(long)]
    skip_upload: bool,
}

/// A wrapper around a `Sha256` that implements `std::io::Write`.
/// It will hash all data written to it, but also pass the data
/// into the wrapped writer.
struct PassThroughHashWriter<T: std::io::Write> {
    hash_context: Context,
    inner: T,
}

impl<T: std::io::Write> PassThroughHashWriter<T> {
    fn new(inner: T) -> Self {
        Self {
            hash_context: Context::new(&SHA256),
            inner,
        }
    }

    fn finish(self) -> (String, T) {
        let hash = data_encoding::HEXLOWER.encode(self.hash_context.finish().as_ref());
        (hash, self.inner)
    }
}

impl<T: std::io::Write> std::io::Write for PassThroughHashWriter<T> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.hash_context.update(buf);
        self.inner.write(buf)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.inner.flush()
    }
}

struct PassThroughProgressWriter<T: std::io::Write> {
    inner: T,
    pbar: Arc<ProgressBar>,
}

impl<T: std::io::Write> PassThroughProgressWriter<T> {
    fn new(inner: T, pbar: Arc<ProgressBar>) -> Self {
        Self { inner, pbar }
    }

    fn finish(self) -> T {
        if !self.pbar.is_finished() {
            self.pbar.finish();
        }
        self.inner
    }
}

impl<T: std::io::Write> std::io::Write for PassThroughProgressWriter<T> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let n = self.inner.write(buf)?;
        self.pbar.inc(n as u64);
        Ok(n)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.inner.flush()
    }
}

struct AsyncReadPassThrough<T: AsyncRead> {
    inner: T,
    pbar: Arc<ProgressBar>,
}

impl<T: AsyncRead> AsyncReadPassThrough<T> {
    fn new(inner: T, pbar: Arc<ProgressBar>) -> Self {
        Self { inner, pbar }
    }
}

impl<T: AsyncRead> Drop for AsyncReadPassThrough<T> {
    fn drop(&mut self) {
        if !self.pbar.is_finished() {
            self.pbar.finish();
        }
    }
}

impl<T: AsyncRead + std::marker::Unpin> AsyncRead for AsyncReadPassThrough<T> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut [u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        let fut = Pin::new(&mut self.inner).poll_read(cx, buf);
        match fut {
            std::task::Poll::Ready(Ok(n)) => {
                self.pbar.inc(n as u64);
            }
            _ => {}
        }
        fut
    }
}

struct RetrySyncWrite<T: std::io::Write> {
    inner: T,
}

impl<T: std::io::Write> RetrySyncWrite<T> {
    #[allow(dead_code)]
    fn new(inner: T) -> Self {
        Self { inner }
    }
}

impl<T: std::io::Write> std::io::Write for RetrySyncWrite<T> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let mut n = 0;
        while n == 0 {
            // we can just loop once. but it can also be changed to `while n < buf.len()`.
            match self.inner.write(&buf[n..]) {
                Ok(m) => {
                    n += m;
                }
                Err(e) => {
                    if e.kind() == std::io::ErrorKind::WouldBlock {
                        // yield to other tasks
                        std::thread::yield_now();
                        continue;
                    }
                    return Err(e);
                }
            }
        }
        Ok(n)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.inner.flush()
    }
}

#[tokio::main]
async fn main() {
    let docker = Docker::connect_with_local_defaults().unwrap();

    // healthcheck
    docker.version().await.expect("Can't talk to dockerd");

    let args = Cli::parse();
    let image_info = docker
        .inspect_image(&args.source_image)
        .await
        .expect("Unable to find image");
    println!("Image: {:?}", image_info);

    let graph_driver = image_info.graph_driver.expect("No graph driver");
    assert!(graph_driver.name == "overlay2");
    let upper_layer = graph_driver
        .data
        .get("UpperDir")
        .expect("No UpperDir")
        .to_owned();
    let lower_layers: Vec<String> = graph_driver
        .data
        .get("LowerDir")
        .expect("No LowerDir")
        .split(':')
        .map(|s| s.to_owned())
        .collect();
    let layers: Vec<String> = vec![upper_layer].into_iter().chain(lower_layers).collect();

    let m = Arc::new(MultiProgress::with_draw_target(
        indicatif::ProgressDrawTarget::stdout_with_hz(2),
    ));
    let sty = ProgressStyle::with_template("{spinner:.green} ({msg}) [{elapsed_precise}] [{wide_bar:.cyan/blue}] {bytes:>12}/{total_bytes:>12} ({bytes_per_sec:>15}, {eta:>5})")
        .unwrap()
        .progress_chars("#>-");

    // TODO: the next step is re-use the logic from sky-packer to generate a "plan"  for the
    // split files.
    // TODO: upload the oci config as well. ensure we can actually pull it.
    // TODO: add some progress bar niceties: remove the completed pbar, make the as logs

    let mut joinset = tokio::task::JoinSet::new();
    let semaphore = Arc::new(tokio::sync::Semaphore::new(20));

    let mut layer_descripters: Vec<oci_spec::image::Descriptor> = Vec::new();

    for layer in layers {
        let cache_loc = "/tmp/sky-uploader-split";
        // find the parent directory of layer
        let layer_id = std::path::Path::new(&layer)
            .parent()
            .expect("No parent dir")
            .file_name()
            .expect("No file name")
            .to_str()
            .expect("Invalid parent dir")
            .to_owned();

        // find all file under cache_loc/layer_id*
        let mut files: Vec<(String, String, u64)> = vec![];
        for entry in std::fs::read_dir(cache_loc).unwrap() {
            let entry = entry.unwrap();
            let path = entry.path();
            if path.is_file() {
                let path_str = path.to_str().unwrap();
                if path_str.contains(&layer_id) & !path_str.ends_with(".sha256") {
                    let sha =
                        std::fs::read_to_string(format!("{}.compressed.sha256", path_str)).unwrap();
                    let content_size = std::fs::metadata(&path_str)
                        .expect("Unable to get metadata")
                        .len();
                    files.push((path_str.to_owned(), sha, content_size));
                }
            }
        }
        files.sort_by(|a, b| a.0.cmp(&b.0));

        files.iter().enumerate().for_each(|(i, (_, sha, size))| {
            let mut annotation = HashMap::new();
            annotation.insert(
                "org.skycontainers.layer_shard_index".to_owned(),
                format!("{}", i),
            );
            annotation.insert(
                "org.skycontainers.layer_shard_total".to_owned(),
                format!("{}", files.len()),
            );
            if i > 0 {
                annotation.insert(
                    "org.skycontainers.layer_shard_parent_sha".to_owned(),
                    format!("sha256:{}", files[0].1),
                );
            }
            let desc = oci_spec::image::DescriptorBuilder::default()
                .media_type("application/vnd.oci.image.layer.v1.tar+zstd")
                .annotations(annotation)
                .digest(format!("sha256:{}", sha))
                .size(*size as i64)
                .build()
                .unwrap();
            layer_descripters.push(desc);
        });

        if args.skip_upload {
            continue;
        }

        // m.println(format!(
        //     "Layer: {}, layer_id: {}, files: {:?}\n",
        //     layer, layer_id, files
        // ))
        // .unwrap();

        for (blob_path, sha256_hash, content_size) in files {
            let args = args.clone();
            let permit = semaphore.clone().acquire_owned().await.unwrap();
            let m = m.clone();
            let sty = sty.clone();

            joinset.spawn(async move {
                // Make the upload stream compatible with progress bar
                // let cursor = std::io::Cursor::new(buf);

                let mut is_success: Option<()> = None;
                let mut retry_count = 3;
                let pb = Arc::new(m.add(ProgressBar::new(content_size as u64)));
                pb.set_message(blob_path.clone()[blob_path.len() - 30..].to_owned());
                pb.set_style(sty);
                while is_success.is_none() && retry_count > 0 {
                    pb.set_position(0);

                    let cursor = tokio::fs::File::open(blob_path.clone())
                        .await
                        .expect("Unable to open file");
                    let buf_reader = ReaderStream::new(BufReader::new(cursor)).into_async_read();
                    let pbar_reader = AsyncReadPassThrough::new(buf_reader, pb.clone());
                    let reader_stream: Box<
                        dyn Stream<Item = Result<Bytes, Box<dyn std::error::Error + Send + Sync>>>
                            + Send,
                    > = Box::new(ReaderStream::new(pbar_reader.compat()).map_err(|e| e.into()));
                    let body = reqwest::Body::wrap_stream(hyper::Body::from(reader_stream));

                    let client = reqwest::Client::new();

                    let resp = client
                        .post(format!(
                            "http://{}/v2/{}/blobs/uploads/",
                            &args.registry_url, &args.image_name
                        ))
                        .send()
                        .await
                        .unwrap();
                    let upload_url = resp.headers().get("location");
                    if upload_url.is_none() {
                        println!("Upload failed: {:?}, retrying...", resp);
                        retry_count -= 1;
                        continue;
                    }
                    let upload_url = upload_url.unwrap();
                    let resp = client
                        .put(upload_url.to_str().unwrap())
                        .body(body)
                        .query(&[("digest", format!("sha256:{}", sha256_hash))])
                        .header("Content-Type", "application/octet-stream")
                        .header("Content-Length", content_size)
                        .send()
                        .await;

                    if resp.is_err() {
                        println!("Upload failed: {:?}, retrying...", resp);
                        retry_count -= 1;
                        continue;
                    }
                    let resp = resp.unwrap();

                    if resp.status() == 201 {
                        is_success = Some(());
                    } else {
                        println!(
                            "Upload failed: {}, {:?}, retrying...",
                            resp.status(),
                            resp.headers()
                        );
                        retry_count -= 1;
                    }
                }
                if is_success.is_none() {
                    panic!("Upload failed after 3 retries");
                }
                drop(permit);
            });
        }

        // let dir_size = fs_extra::dir::get_size(&layer).unwrap();
        // let write_pbar = Arc::new(m.add(ProgressBar::new(dir_size as u64)));
        // write_pbar.set_style(sty.clone());

        // let buf: Vec<u8> = Vec::new();
        // let raw_bytes_scanned_pbar = PassThroughProgressWriter::new(buf, write_pbar.clone());
        // let hasher = PassThroughHashWriter::new(raw_bytes_scanned_pbar);
        // let encoder = zstd::Encoder::new(hasher, 0).unwrap();
        // let mut builder = tar::Builder::new(encoder);
        // builder.follow_symlinks(false);
        // builder.append_dir_all("./", &layer).unwrap();
        // builder.finish().unwrap();
        // let encoder = builder.into_inner().unwrap();
        // let hasher = encoder.finish().unwrap();
        // let (sha256_hash, pbar_pass_through) = hasher.finish();
        // let buf = pbar_pass_through.finish();
        // let content_size = buf.len();

        // create a subprocess to tar the layer and stream to stdout
        // let mut cmd = std::process::Command::new("tar");
        // cmd.arg("-c")
        //     .arg("-C")
        //     .arg(&layer)
        //     .arg(".")
        //     .stdout(std::process::Stdio::piped());
        // let mut child = cmd.spawn().unwrap();
        // let stdout = child.stdout.take().unwrap();

        // let (sender, recv) = tokio::sync::mpsc::unbounded_channel::<Option<String>>();
        // split_into_tar(
        //     Box::new(stdout),
        //     200 * 1000 * 1000,
        //     "/tmp/sky-uploader-split/scratch.tar".to_string(),
        //     sender,
        // );

        // println!("Layer: {}, buff size: {}", layer, content_size);

        // let mut archive = tar::Archive::new(buf.as_slice());
        // for entry in archive.entries().unwrap() {
        //     // println!("Entry: {:?}", entry.unwrap().header());
        // }

        // println!("Upload response: {}, {:?}", resp.status(), resp.headers());
    }

    while let Some(_) = joinset.join_next().await {}

    // for desc in layer_descripters {
    //     println!("Layer: {:?}", desc);
    // }

    let mut config_builder = oci_spec::image::ConfigBuilder::default();
    let src_config = image_info.config.unwrap();
    if let Some(user) = src_config.user {
        config_builder = config_builder.user(user);
    }
    if let Some(env) = src_config.env {
        config_builder = config_builder.env(env);
    }
    if let Some(entrypoint) = src_config.entrypoint {
        config_builder = config_builder.entrypoint(entrypoint);
    }
    if let Some(cmd) = src_config.cmd {
        config_builder = config_builder.cmd(cmd);
    }
    // TODO: exposed_ports, volumes, working_dir, labels, stop_signal.
    let config = config_builder.build().unwrap();

    let root_fs_builder = oci_spec::image::RootFsBuilder::default().diff_ids(
        layer_descripters
            .iter()
            .map(|d| d.digest().clone()) // TODO: i think the diff ids should be the uncomressed sha256 but we are just using the compressed atm.
            .collect::<Vec<_>>(),
    );
    let root_fs = root_fs_builder.build().unwrap();

    let image_config_builder = oci_spec::image::ImageConfigurationBuilder::default()
        .architecture(oci_spec::image::Arch::Amd64)
        .config(config)
        .os(oci_spec::image::Os::Linux)
        .rootfs(root_fs);
    // TODO: history
    let image_config = image_config_builder.build().unwrap();

    // TODO: use image config to bring in diff ids.

    let resp = reqwest::Client::new()
        .post(format!(
            "http://{}/v2/{}/blobs/uploads/",
            &args.registry_url, &args.image_name
        ))
        .send()
        .await
        .unwrap();
    let upload_url = resp.headers().get("location").unwrap();
    let serialized_config = serde_json::to_string(&image_config).unwrap();
    let sha256_hash = data_encoding::HEXLOWER
        .encode(ring::digest::digest(&ring::digest::SHA256, serialized_config.as_bytes()).as_ref());
    let content_length = serialized_config.len();
    let resp = reqwest::Client::new()
        .put(upload_url.to_str().unwrap())
        .body(serialized_config)
        .query(&[("digest", format!("sha256:{}", sha256_hash))])
        .send()
        .await
        .unwrap();
    assert!(resp.status() == 201);

    // println!("Uploaded config: {:?}", image_config);

    let manifest = oci_spec::image::ImageManifestBuilder::default()
        .schema_version(oci_spec::image::SCHEMA_VERSION)
        .media_type("application/vnd.oci.image.manifest.v1+json")
        .config(
            oci_spec::image::DescriptorBuilder::default()
                .media_type("application/vnd.oci.image.config.v1+json")
                .digest(format!("sha256:{}", sha256_hash))
                .size(content_length as i64)
                .build()
                .unwrap(),
        )
        .layers(layer_descripters)
        .build()
        .unwrap();
    let serialized_manifest = serde_json::to_string(&manifest).unwrap();
    let sha256_hash = data_encoding::HEXLOWER.encode(
        ring::digest::digest(&ring::digest::SHA256, serialized_manifest.as_bytes()).as_ref(),
    );
    let content_length = serialized_manifest.len();

    for tag in vec![args.image_tag, sha256_hash.clone()[..6].to_string()] {
        let resp = reqwest::Client::new()
            .put(format!(
                "http://{}/v2/{}/manifests/{}",
                &args.registry_url, &args.image_name, &tag
            ))
            .body(serialized_manifest.clone())
            .header("Content-Type", "application/vnd.oci.image.manifest.v1+json")
            .header("Content-Length", content_length)
            .send()
            .await
            .unwrap();
        assert!(resp.status() == 201);
        println!("Uploaded manifest: {}, hash {}", tag, &sha256_hash);
    }
}

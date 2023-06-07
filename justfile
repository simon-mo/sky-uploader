list:
    @just --list

# run-args := "--source-image localhost:5001/image --registry-url localhost:5000 --image-name test-image"
run-args := "--source-image vicuna:latest --registry-url localhost:5000 --image-name vicuna --image-tag latest"

run:
    cargo build
    sudo env "PATH=$PATH" ../target/debug/sky-uploader {{run-args}}

run-release:
    cargo build --release
    sudo env "PATH=$PATH" ../target/release/sky-uploader {{run-args}}

run-registry:
    docker run --rm -p 5000:5000 --name registry registry:2

run-upload-config:
    cargo build
    sudo env "PATH=$PATH" ../target/debug/sky-uploader {{run-args}} --skip-upload
#!/usr/bin/env bash

# Get the absolute path of this script
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Find the root directory
ROOT_DIR="$(realpath "$SCRIPT_DIR/..")"

# Find the clip service path
PROTO_DIR="$ROOT_DIR/clip"

# Find the clip protobuf file location
PROTO_FILE="$PROTO_DIR/clip.proto"

# Find the server output directory
OUTPUT_DIR="$ROOT_DIR/server"

# Run protoc with dynamically computed paths
protoc -I="$ROOT_DIR" \
  --go_out=paths=source_relative:"$OUTPUT_DIR" \
  --go-grpc_out=paths=source_relative:"$OUTPUT_DIR" \
  --proto_path="$PROTO_DIR" \
  --go_opt=Mclip/clip.proto="github.com/foresturquhart/curator/server/clip" \
  --go-grpc_opt=Mclip/clip.proto="github.com/foresturquhart/curator/server/clip" \
  "$PROTO_FILE"
#!/usr/bin/env bash

OUT_DIR=$(realpath "$1")
PROTO_PATH=$(realpath '.')

mkdir -p "$OUT_DIR"

protoc \
    -I="$PROTO_PATH" \
    --python_out="$OUT_DIR" \
    --pyi_out="$OUT_DIR" \
    "$PROTO_PATH/perfetto_trace.proto" \


# Use this if you cloned the superproject repo from:
# https://android.googlesource.com/platform/superproject
# Make sure the repository is in the folder superproject together with this script

# PROTO_PATH=$(realpath './superproject/external/')
# protoc \
#     -I="$PROTO_PATH/protos/perfetto/trace" \
#     --python_out="$OUT_DIR" \
#     --pyi_out="$OUT_DIR" \
#     "$PROTO_PATH/protos/perfetto/trace/perfetto_trace.proto" \





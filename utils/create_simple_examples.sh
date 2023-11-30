#!/usr/bin/env bash

DEST_DIR="$1"

# 1 host, 1 IO operations
python -m create_goal simple --host-count 1 --writes 1 --reads 1 --no-mount --disk-size 1 "$DEST_DIR/host-1_writes-1_reads-1_wo-mount.goal"
python -m create_goal simple --host-count 1 --writes 0 --reads 1 --no-mount --disk-size 1 "$DEST_DIR/host-1_writes-0_reads-1_wo-mount.goal"
python -m create_goal simple --host-count 1 --writes 1 --reads 0 --no-mount --disk-size 1 "$DEST_DIR/host-1_writes-1_reads-0_wo-mount.goal"
python -m create_goal simple --host-count 1 --writes 1 --reads 1 --mount --disk-size 1    "$DEST_DIR/host-1_writes-1_reads-1_mount.goal"
python -m create_goal simple --host-count 1 --writes 0 --reads 1 --mount --disk-size 1    "$DEST_DIR/host-1_writes-0_reads-1_mount.goal"
python -m create_goal simple --host-count 1 --writes 1 --reads 0 --mount --disk-size 1    "$DEST_DIR/host-1_writes-1_reads-0_mount.goal"

# 1 host, 2 IO operations
python -m create_goal simple --host-count 1 --writes 2 --reads 2 --no-mount --disk-size 1 "$DEST_DIR/host-1_writes-2_reads-2_wo-mount.goal"
python -m create_goal simple --host-count 1 --writes 0 --reads 2 --no-mount --disk-size 1 "$DEST_DIR/host-1_writes-0_reads-2_wo-mount.goal"
python -m create_goal simple --host-count 1 --writes 2 --reads 0 --no-mount --disk-size 1 "$DEST_DIR/host-1_writes-2_reads-0_wo-mount.goal"
python -m create_goal simple --host-count 1 --writes 2 --reads 2 --mount --disk-size 1    "$DEST_DIR/host-1_writes-2_reads-2_mount.goal"
python -m create_goal simple --host-count 1 --writes 0 --reads 2 --mount --disk-size 1    "$DEST_DIR/host-1_writes-0_reads-2_mount.goal"
python -m create_goal simple --host-count 1 --writes 2 --reads 0 --mount --disk-size 1    "$DEST_DIR/host-1_writes-2_reads-0_mount.goal"

# 2 host, 1 IO operations
python -m create_goal simple --host-count 2 --writes 1 --reads 1 --no-mount --disk-size 1 "$DEST_DIR/host-2_writes-1_reads-1_wo-mount.goal"
python -m create_goal simple --host-count 2 --writes 0 --reads 1 --no-mount --disk-size 1 "$DEST_DIR/host-2_writes-0_reads-1_wo-mount.goal"
python -m create_goal simple --host-count 2 --writes 1 --reads 0 --no-mount --disk-size 1 "$DEST_DIR/host-2_writes-1_reads-0_wo-mount.goal"
python -m create_goal simple --host-count 2 --writes 1 --reads 1 --mount --disk-size 1    "$DEST_DIR/host-2_writes-1_reads-1_mount.goal"
python -m create_goal simple --host-count 2 --writes 0 --reads 1 --mount --disk-size 1    "$DEST_DIR/host-2_writes-0_reads-1_mount.goal"
python -m create_goal simple --host-count 2 --writes 1 --reads 0 --mount --disk-size 1    "$DEST_DIR/host-2_writes-1_reads-0_mount.goal"

# 2 host, 2 IO operations
python -m create_goal simple --host-count 2 --writes 2 --reads 2 --no-mount --disk-size 1 "$DEST_DIR/host-2_writes-2_reads-2_wo-mount.goal"
python -m create_goal simple --host-count 2 --writes 0 --reads 2 --no-mount --disk-size 1 "$DEST_DIR/host-2_writes-0_reads-2_wo-mount.goal"
python -m create_goal simple --host-count 2 --writes 2 --reads 0 --no-mount --disk-size 1 "$DEST_DIR/host-2_writes-2_reads-0_wo-mount.goal"
python -m create_goal simple --host-count 2 --writes 2 --reads 2 --mount --disk-size 1    "$DEST_DIR/host-2_writes-2_reads-2_mount.goal"
python -m create_goal simple --host-count 2 --writes 0 --reads 2 --mount --disk-size 1    "$DEST_DIR/host-2_writes-0_reads-2_mount.goal"
python -m create_goal simple --host-count 2 --writes 2 --reads 0 --mount --disk-size 1    "$DEST_DIR/host-2_writes-2_reads-0_mount.goal"



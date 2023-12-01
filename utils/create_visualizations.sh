#!/usr/bin/env bash

BINS="$1"
SRC_DIR="$2"

mkdir -p "$SRC_DIR/viz"
mkdir -p "$SRC_DIR/trace"

for f in "$SRC_DIR"/*.goal; do
    BNAME=$(basename "$f" .goal)
    TOPOLOGY_JSON="${SRC_DIR}/${BNAME}_topology.json"
    BPATH_VIZ="${SRC_DIR}/viz/${BNAME}.viz"
    BPATH_TRACE="${SRC_DIR}/trace/${BNAME}.trace"

    echo "$BNAME"; 
    "$BINS"/txt2bin -i "$f" -o temp.bin \
        && "$BINS"/LogGOPSim -f temp.bin -V "${BPATH_VIZ}" \
        && "$BINS"/visualize --rank-name-map "${TOPOLOGY_JSON}" "${BPATH_VIZ}" "${BPATH_TRACE}"; 
done; 
rm temp.bin



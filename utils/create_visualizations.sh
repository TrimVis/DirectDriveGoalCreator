#!/usr/bin/env bash

BINS="$1"
SRC_DIR="$2"

mkdir -p "$SRC_DIR/viz"
mkdir -p "$SRC_DIR/trace"

for f in "$SRC_DIR"/*.goal; do
    BNAME=$(basename "$f" .goal)
    TOPOLOGY_JSON="${SRC_DIR}/${BNAME}_topology.json"
    BPATH_VIZ="${SRC_DIR}/viz/${BNAME}.viz"
    # Create all variants (expert, advanced, simple)
    BPATH_TRACE_EXPERT="${SRC_DIR}/trace/expert/${BNAME}.trace"
    BPATH_TRACE_ADVANCED="${SRC_DIR}/trace/advanced/${BNAME}.trace"
    BPATH_TRACE_SIMPLE="${SRC_DIR}/trace/simple/${BNAME}.trace"

    # Run in loggopssim and create viz files, exit in case of errors
    "$BINS"/txt2bin -i "$f" -o temp.bin && "$BINS"/LogGOPSim -f temp.bin -V "${BPATH_VIZ}" || exit 1;

    # Use topology files if they exist for naming
    if [ -f "$TOPOLOGY_JSON" ]; then
        "$BINS"/visualize "${BPATH_VIZ}" "${BPATH_TRACE_EXPERT}" --expert --rank-name-map "${TOPOLOGY_JSON}";
        "$BINS"/visualize "${BPATH_VIZ}" "${BPATH_TRACE_ADVANCED}" --advanced --rank-name-map "${TOPOLOGY_JSON}";
        "$BINS"/visualize "${BPATH_VIZ}" "${BPATH_TRACE_SIMPLE}" --rank-name-map "${TOPOLOGY_JSON}";
    else
        "$BINS"/visualize "${BPATH_VIZ}" "${BPATH_TRACE_EXPERT}" --expert;
        "$BINS"/visualize "${BPATH_VIZ}" "${BPATH_TRACE_ADVANCED}" --advanced;
        "$BINS"/visualize "${BPATH_VIZ}" "${BPATH_TRACE_SIMPLE}";
    fi;
done; 
rm temp.bin




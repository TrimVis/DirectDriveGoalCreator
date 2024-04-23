#!/usr/bin/env bash

# add the script directory to the pythonpath
REPO_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
REPO_DIR=$(realpath -- "$REPO_DIR/..")

# Get the required bins 
LOGOPS="${REPO_DIR}/utils/LogGOPSim"
TXT2BIN="${REPO_DIR}/utils/txt2bin"

# Prepare the src dir and the dest dirs too
SRC_DIR="$1"
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
    rm temp.bin;

    if ! "$TXT2BIN" -i "$f" -o temp.bin; then
        echo "Error during txt2bin execution..."
        echo "Skipping!"
        continue
    fi

    if ! "$LOGOPS" -f temp.bin -V "${BPATH_VIZ}"; then
        echo "Error during LogGOPSim execution..."
        echo "Skipping!"
        continue
    fi

    echo "Generating visualizations for ${f}"

    # Use topology files if they exist for naming
    if [ -f "$TOPOLOGY_JSON" ]; then
        "${REPO_DIR}/visualize" \
            "${BPATH_VIZ}" "${BPATH_TRACE_EXPERT}" \
            --rank-name-map "${TOPOLOGY_JSON}" \
            --expert > /dev/null;
        "${REPO_DIR}/visualize" \
            "${BPATH_VIZ}" "${BPATH_TRACE_ADVANCED}" \
            --rank-name-map "${TOPOLOGY_JSON}" \
            --advanced > /dev/null;
        "${REPO_DIR}/visualize" \
            "${BPATH_VIZ}" "${BPATH_TRACE_SIMPLE}"\
            --rank-name-map "${TOPOLOGY_JSON}" > /dev/null;
    else
        "${REPO_DIR}/visualize" \
            "${BPATH_VIZ}" "${BPATH_TRACE_EXPERT}" \
            --expert > /dev/null;
        "${REPO_DIR}/visualize" \
            "${BPATH_VIZ}" "${BPATH_TRACE_ADVANCED}" \
            --advanced > /dev/null;
        "${REPO_DIR}/visualize" \
            "${BPATH_VIZ}" "${BPATH_TRACE_SIMPLE}" > /dev/null;
    fi;

    echo "Done!"
done; 




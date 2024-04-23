#!/usr/bin/env bash

# add the script directory to the pythonpath
REPO_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
REPO_DIR=$(realpath -- "$REPO_DIR/..")

DEST_DIR="$1"
mkdir -p "$DEST_DIR"

# Iterate over each range as specified
for host_count in 1 2; do
    for no_reads in 0 1; do
        for no_writes in 0 1; do
            for mount in "no-mount" "mount"; do
                base_name="${DEST_DIR}/host-${host_count}_writes-${no_writes}_reads-${no_reads}_${mount}"

                # Skip already generated files
                if [[ -f "${base_name}.goal" ]]; then
                    echo "File already exists! (${base_name}.goal). Skipping..."
                    continue
                fi

                echo "Generating host: ${host_count}; writes: ${no_writes}; reads: ${no_reads}; ${mount}"
                "${REPO_DIR}/trace2goal" simple \
                       --ccs-count 1 --bss-count 1 --host-count ${host_count} \
                       --writes ${no_writes} --reads ${no_reads} \
                       --${mount} --disk-size 1 \
                       --rank-names-dest "${base_name}_topology.json"\
                       "${base_name}.goal" > /dev/null
                echo "Done: ${base_name}.goal"
            done
        done
    done
done

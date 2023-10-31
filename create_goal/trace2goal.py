import csv
from loguru import logger
from tqdm import tqdm

from network import NetworkTopology, DirectDriveNetwork


def parse_trace(trace_path: str, out_path: str):
    # Extract Information about Topology and Disks
    logger.info("Extracting general trace information")

    slice_size = 1024*1024
    disk_size = 1024*1024*1024
    host_count = 1
    slb_count = 1
    gs_count = 1
    mds_count = 1
    ccs_count = 1
    bss_count = 1

    csv_line_no = 0
    with open(trace_path, 'r') as f:
        reader = csv.reader(f)
        for (asu, lba, size, opcode, *_) in tqdm(reader):
            host_count = max(host_count, int(asu) + 1)
            disk_size = max(int(lba) + int(size), disk_size)
            csv_line_no += 1

    # Create Network Topology
    topology = NetworkTopology(
        host_count=host_count,
        slb_count=slb_count,
        gs_count=gs_count,
        mds_count=mds_count,
        ccs_count=ccs_count,
        bss_count=bss_count,
    )

    # Create Network
    network = DirectDriveNetwork(
        topology=topology, slice_size=slice_size, disk_size=disk_size)

    # Add Interactions
    logger.info("Adding interactions")
    with open(trace_path, 'r') as f:
        reader = csv.reader(f)
        for (asu, lba, size, opcode, *_) in tqdm(reader, total=csv_line_no):
            network.add_interaction(op_code=opcode, asu=int(asu),
                                    address=int(lba), size=int(size))

    # Finalize
    network.to_goal(out_path)

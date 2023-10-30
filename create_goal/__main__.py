#!/usr/bin/env python3.11

import random
from loguru import logger
from tqdm import tqdm
from network import NetworkTopology, DirectDriveNetwork
from trace2goal import parse_trace

testing = True

if testing:
    host_count = 16
    disk_size = 4096*1024
    slice_size = 1024

    topology = NetworkTopology(
        host_count=host_count,
        slb_count=1,
        gs_count=1,
        mds_count=1,
        ccs_count=128,
        bss_count=1280,
    )
    network = DirectDriveNetwork(
        topology=topology, slice_size=slice_size, disk_size=disk_size)

    pbar = tqdm(total=(host_count*host_count*2+host_count))
    logger.debug("Adding Mount Interaction")
    for h in range(host_count):
        network.add_mount(h)
    pbar.update(host_count)
    logger.debug("Adding Read Interaction")
    for h in range(host_count):
        for _ in range(host_count):
            start = random.randint(0, disk_size//2)
            end = random.randint(start, disk_size)
            network.add_read(h, start, end)
        pbar.update(host_count)
    logger.debug("Adding Write Interaction")
    for h in range(host_count):
        for _ in range(host_count):
            start = random.randint(0, disk_size//2)
            end = random.randint(start, disk_size)
            network.add_write(h, start, end)
        pbar.update(host_count)

    goal_res = network.to_goal()
else:
    trace_path = "/home/trim/uni/HS23/SemesterProject/umass_data_trace/Financial1.spc"
    out_path = "./out.goal"
    parse_trace(trace_path, out_path)

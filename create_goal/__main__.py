#!/usr/bin/env python3.11

import csv
import sys
import random
import click
from loguru import logger
from tqdm import tqdm
from .network import NetworkTopology, DirectDriveNetwork


@click.group(name="create_goal")
@click.option('--debug/--no-debug', default=False, help='Show debug logs')
def cli(debug):
    # Significantly reduce what and how we log in case no debug flag is set
    if not debug:
        def my_format(record):
            mins = record['elapsed'].seconds // 60
            secs = record['elapsed'].seconds % 60
            return "[<green>{mins}m{secs}s</green>] {message}\n".format(**record, mins=mins, secs=secs)

        logger.remove()  # remove the old handler. Else, the old one will work along with the new one you've added below'
        logger.add(sys.stdout, format=my_format,
                   filter="__main__", level="INFO")
    pass


@cli.command(name="trace", help="Transform a uMass trace file to a goal file. The no of hosts and minimum disk size will be autodected and adapted if necessary")
@click.argument('trace_path', type=click.Path(exists=True, dir_okay=False, resolve_path=True))
@click.argument('out_path', type=click.Path(exists=False, writable=True, dir_okay=False, resolve_path=True))
@click.option('--slice-size', default=1024, help='Slice size in kB')
@click.option('--slb-count', default=1, help='No of Software Load Balancers in network')
@click.option('--gs-count', default=1, help='No of Gateway Switches in network')
@click.option('--mds-count', default=1, help='No of MetaData Services in network')
@click.option('--ccs-count', default=8, help='No of Change Coordinator Services in network')
@click.option('--bss-count', default=64, help='No of Block Storage Services in network')
def cli_pt(trace_path, out_path, slice_size, slb_count, gs_count, mds_count, ccs_count, bss_count):
    logger.info("Extracting host count and disk size from trace")

    disk_size = 1024*1024*1024
    slice_size *= 1024
    host_count = 1

    csv_line_no = 0
    with open(trace_path, 'r') as f:
        reader = csv.reader(f)
        for (asu, lba, size, opcode, *_) in tqdm(reader):
            host_count = max(host_count, int(asu) + 1)
            disk_size = max(int(lba) + int(size), disk_size)
            csv_line_no += 1

    # Create Network Topology
    logger.info(
        f"Creating network topology ({host_count} hosts; {ccs_count} CCS; {bss_count} BSS)")
    topology = NetworkTopology(
        host_count=host_count,
        slb_count=slb_count,
        gs_count=gs_count,
        mds_count=mds_count,
        ccs_count=ccs_count,
        bss_count=bss_count,
    )

    # Create Network
    logger.info(
        f"Creating network (Slice Size: {slice_size//1024}kB; Disk Size: {disk_size//1024}kB)")
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
    logger.info(f"Writing goal file to '{out_path}'")
    network.to_goal(out_path)


@cli.command(name="simple", help="Creates a goal file of a simple network and adds for each host random read and writes")
@click.option('-N', default=16, help='No. of random read and writes per host in network')
@click.option('--disk-size', default=4096, help='Disk size in kB')
@click.option('--slice-size', default=1, help='Slice size in kB')
@click.option('--host-count', default=16, help='No. of hosts in network')
@click.option('--slb-count', default=1, help='No of Software Load Balancers in network')
@click.option('--gs-count', default=1, help='No of Gateway Switches in network')
@click.option('--mds-count', default=1, help='No of MetaData Services in network')
@click.option('--ccs-count', default=128, help='No of Change Coordinator Services in network')
@click.option('--bss-count', default=1280, help='No of Block Storage Services in network')
@click.argument('out_file', type=click.Path(exists=False, writable=True, dir_okay=False, resolve_path=True))
def cli_cs(out_file, n, host_count, disk_size, slice_size, slb_count, gs_count, mds_count, ccs_count, bss_count):
    """ Creates a simple network and random reads and writes in it """
    disk_size *= 1024
    slice_size *= 1024

    topology = NetworkTopology(
        host_count=host_count,
        slb_count=slb_count,
        gs_count=gs_count,
        mds_count=mds_count,
        ccs_count=ccs_count,
        bss_count=bss_count,
    )
    network = DirectDriveNetwork(
        topology=topology, slice_size=slice_size, disk_size=disk_size)

    logger.info("Adding Mount Interactions")
    pbar = tqdm(total=(n*host_count*2+host_count))
    for h in range(host_count):
        network.add_mount(h)
    pbar.update(host_count)

    logger.info("Adding Read Interactions")
    for h in range(host_count):
        for _ in range(n):
            start = random.randint(0, disk_size//2)
            end = random.randint(start, disk_size)
            network.add_read(h, start, end)
        pbar.update(host_count)

    logger.info("Adding Write Interactions")
    for h in range(host_count):
        for _ in range(n):
            start = random.randint(0, disk_size//2)
            end = random.randint(start, disk_size)
            network.add_write(h, start, end)
        pbar.update(host_count)

    logger.info(f"Writing goal file to '{out_file}'")
    network.to_goal(out_file)


if __name__ == "__main__":
    cli()

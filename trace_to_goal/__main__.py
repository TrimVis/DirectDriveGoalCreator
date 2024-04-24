#!/usr/bin/env python3.11

import csv
import sys
import random
import click
from loguru import logger
from tqdm import tqdm
from .network import NetworkTopology, DirectDriveNetwork, VALID_TOPOLOGY_STRATEGIES


@click.group(name="trace2goal")
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


@cli.command(name="trace", help="Transform a uMass trace file to a goal file. The no of hosts and minimum disk size will be autodected and adapted if necessary")
@click.argument('trace_path', type=click.Path(exists=True, dir_okay=False, resolve_path=True))
@click.argument('out_path', type=click.Path(exists=False, writable=True, dir_okay=False, resolve_path=True))
@click.option('--slice-size', default=1024, help='Slice size in kB')
@click.option('--slb-count', default=1, help='No of Software Load Balancers in network')
@click.option('--gs-count', default=1, help='No of Gateway Switches in network')
@click.option('--mds-count', default=1, help='No of MetaData Services in network')
@click.option('--ccs-count', default=8, help='No of Change Coordinator Services in network')
@click.option('--bss-count', default=64, help='No of Block Storage Services in network')
@click.option('--next-slb-strategy', default='round-robin', help="Strategy to decide on next SLB")
@click.option('--topology-strategy', default='grouped-by-kind', help=f"Strategy to use to spread elements across network (One of: {VALID_TOPOLOGY_STRATEGIES})")
@click.option('--rank-names-dest', type=click.Path(exists=False, writable=True, dir_okay=False, resolve_path=True))
@click.option('--op-depens/--no-op-depens', default=True, help='Whether operations of the same host should require termination before the next operation can be executed')
@click.option('--dump-state/--no-dump-state', default=True, help='Will dump the state to disk and delete local references to reduce memory footprint significantly.')
@click.option('--max-no-instructions', type=int, default=None, help='Only read the first X instructions from the trace file.')
def cli_pt(trace_path, out_path, slice_size, slb_count, gs_count, mds_count, ccs_count, bss_count, next_slb_strategy, topology_strategy, rank_names_dest, op_depens, dump_state, max_no_instructions):
    disk_size = 1024*1024*1024
    slice_size *= 1024
    host_count = 1

    logger.info("Extracting host count and disk size from trace")
    csv_line_no = 0
    with open(trace_path, 'r') as f:
        reader = csv.reader(f)
        for (asu, lba, size, opcode, *_) in tqdm(reader):
            host_count = max(host_count, int(asu) + 1)
            disk_size = max(int(lba) + int(size), disk_size)

            csv_line_no += 1
            if max_no_instructions is not None and csv_line_no > max_no_instructions:
                break

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
        strategy=topology_strategy
    )
    if rank_names_dest:
        topology.to_file(rank_names_dest)

    # Create Network
    logger.info(
        f"Creating network (Slice Size: {slice_size//1024}kB; Disk Size: {disk_size//1024}kB)")
    network = DirectDriveNetwork(
        topology=topology, slice_size=slice_size, disk_size=disk_size,
        next_slb_strategy=next_slb_strategy, op_depens=op_depens,
        dump_state=dump_state
    )

    # Add Interactions
    logger.info("Adding interactions")
    with open(trace_path, 'r') as f:
        reader = csv.reader(f)
        for (i, (asu, lba, size, opcode, *_)) in tqdm(enumerate(reader), total=csv_line_no):
            network.add_interaction(op_code=opcode, host=int(asu),
                                    address=int(lba), size=int(size))

            if max_no_instructions is not None and i > max_no_instructions:
                break

    # Finalize
    logger.info(f"Writing goal file to '{out_path}'")
    network.to_goal(out_path)


@cli.command(name="simple", help="Creates a goal file of a simple network and adds for each host random read and writes")
@click.option('--writes', default=16, help='No. of random writes per host in network')
@click.option('--reads', default=16, help='No. of random read per host in network')
@click.option('--mount/--no-mount', default=True, help='Also simulate mount operation for each host')
@click.option('--disk-size', default=4096, help='Disk size in kB')
@click.option('--slice-size', default=1, help='Slice size in kB')
@click.option('--host-count', default=16, help='No. of hosts in network')
@click.option('--slb-count', default=1, help='No of Software Load Balancers in network')
@click.option('--gs-count', default=1, help='No of Gateway Switches in network')
@click.option('--mds-count', default=1, help='No of MetaData Services in network')
@click.option('--ccs-count', default=128, help='No of Change Coordinator Services in network')
@click.option('--bss-count', default=1280, help='No of Block Storage Services in network')
@click.option('--topology-strategy', default='grouped-by-kind', help=f"Strategy to use to spread elements across network (One of: {VALID_TOPOLOGY_STRATEGIES})")
@click.option('--rank-names-dest', type=click.Path(exists=False, writable=True, dir_okay=False, resolve_path=True))
@click.argument('out_file', type=click.Path(exists=False, writable=True, dir_okay=False, resolve_path=True))
def cli_cs(out_file, writes, reads, mount, host_count, disk_size, slice_size, slb_count, gs_count, mds_count, ccs_count, bss_count, topology_strategy, rank_names_dest):
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
        strategy=topology_strategy
    )
    if rank_names_dest:
        topology.to_file(rank_names_dest)

    network = DirectDriveNetwork(
        topology=topology, slice_size=slice_size, disk_size=disk_size,
        dump_state=True, op_depens=True
    )

    pbar = tqdm(total=(reads * host_count + writes * host_count))

    if not reads and not writes and mount:
        for h in range(host_count):
            network.add_mount(h)

    if reads:
        logger.info("Adding Read Interactions")
        for h in range(host_count):
            for _ in range(reads):
                start = random.randint(0, disk_size//2)
                len = random.randint(0, disk_size-start)
                network.add_interaction(
                    op_code='r', host=h, address=start, size=len, mount=mount)
            pbar.update(host_count)

    if writes:
        logger.info("Adding Write Interactions")
        for h in range(host_count):
            for _ in range(writes):
                start = random.randint(0, disk_size//2)
                len = random.randint(0, disk_size-start)
                network.add_interaction(
                    op_code='w', host=h, address=start, size=len, mount=mount)
            pbar.update(host_count)

    logger.info(f"Writing goal file to '{out_file}'")
    network.to_goal(out_file)


@cli.command(name="worst-case", help="Creates a goal file of a simple network and adds for each host highly congested read and writes")
@click.option('--writes', default=8, help='No. of random writes per host in network')
@click.option('--reads', default=8, help='No. of random read per host in network')
@click.option('--repeats', default=2, help='No. of repeats of the read/write cycle')
@click.option('--mount/--no-mount', default=True, help='Also simulate mount operation for each host')
@click.option('--disk-size', default=4096, help='Disk size in kB')
@click.option('--slice-size', default=64, help='Slice size in kB')
@click.option('--host-count', default=4, help='No. of hosts in network')
@click.option('--slb-count', default=1, help='No of Software Load Balancers in network')
@click.option('--gs-count', default=1, help='No of Gateway Switches in network')
@click.option('--mds-count', default=1, help='No of MetaData Services in network')
@click.option('--ccs-count', default=4, help='No of Change Coordinator Services in network')
@click.option('--bss-count', default=4, help='No of Block Storage Services in network')
@click.option('--topology-strategy', default='grouped-by-kind', help=f"Strategy to use to spread elements across network (One of: {VALID_TOPOLOGY_STRATEGIES})")
@click.option('--rank-names-dest', type=click.Path(exists=False, writable=True, dir_okay=False, resolve_path=True))
@click.option('--dump-state/--no-dump-state', default=True, help='Will dump the state to disk and delete local references to reduce memory footprint significantly.')
@click.argument('out_file', type=click.Path(exists=False, writable=True, dir_okay=False, resolve_path=True))
def cli_wc(out_file, writes, reads, mount, host_count, disk_size, slice_size, slb_count, gs_count, mds_count, ccs_count, bss_count, topology_strategy, rank_names_dest, repeats, dump_state):
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
        strategy=topology_strategy
    )
    if rank_names_dest:
        topology.to_file(rank_names_dest)

    network = DirectDriveNetwork(
        topology=topology, slice_size=slice_size, disk_size=disk_size,
        dump_state=dump_state, op_depens=True
    )

    pbar = tqdm(total=(repeats*reads*host_count + repeats*writes *
                host_count + (host_count if mount else 0)))
    if mount:
        logger.info("Adding Mount Interactions")
        for h in range(host_count):
            network.add_mount(h)
        pbar.update(host_count)

    for r in range(repeats):
        if reads:
            logger.info(f"Adding Read Interactions (Rep {r})")
            for _ in range(reads):
                start = random.randint(0, disk_size//2)
                end = random.randint(start, disk_size)
                for h in range(host_count):
                    network.add_read(h, start, end)
                pbar.update(host_count)

        if writes:
            logger.info(f"Adding Write Interactions (Rep {r})")
            for _ in range(writes):
                start = random.randint(0, disk_size//2)
                end = random.randint(start, disk_size)
                for h in range(host_count):
                    network.add_write(h, start, end)
                pbar.update(host_count)

    logger.info(f"Writing goal file to '{out_file}'")
    network.to_goal(out_file)


if __name__ == "__main__":
    cli()

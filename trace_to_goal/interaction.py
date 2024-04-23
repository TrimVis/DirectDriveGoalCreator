from typing import List, Tuple, Literal
from math import ceil

from .common import Addr, SliceId, SliceMap

# Request Config
LOOKUP_REQ_SIZE: int = 256 * 4
LOOKUP_RESP_SIZE: int = 1024 * 4
MOUNT_REQ_SIZE: int = 1024 * 4
MOUNT_RESP_SIZE: int = 1024 * 4


# Interaction time
def calc_io_time(data_size: int, kind: Literal['read'] | Literal['write']):
    # NOTE: estimate for sequential read and write of average ssd taken from
    # https://www.tomshardware.com/features/ssd-benchmarks-hierarchy
    # TODO pjordan: Find a more reputable value for this

    if kind == 'read':
        # 6000 MB/s => 1e9/(6000 * 1e6) ns/B = 1/6 ns/B
        per_byte_cost = 1/6
    else:
        # 1500 MB/s
        per_byte_cost = 1/1.5
    return ceil(data_size * per_byte_cost)


# Interactions
def inject_mount(network: 'DirectDriveNetwork', host_id: int):
    get_new_tag = network.get_next_tag
    get_builder = network.get_builder

    host_rank = network.topology.get_host(host_id)
    host_builder = get_builder(host_rank)

    slb_rank = network.topology.get_slb(network.get_next_slb())
    slb_builder = get_builder(slb_rank)

    gs_rank = network.topology.get_gs(network.get_next_gs())
    gs_builder = get_builder(gs_rank)

    mds_rank = network.topology.get_mds(network.get_next_mds())
    mds_builder = get_builder(mds_rank)

    # Step 1: Request map of slices
    #   Host -> SLB -> GS -> MDS
    req_tag = get_new_tag()
    lbl_host_req = host_builder.add_send(MOUNT_REQ_SIZE, slb_rank, req_tag)
    lbl_slb_req_i = slb_builder.add_recv(MOUNT_REQ_SIZE, host_rank, req_tag)
    lbl_slb_req_o = slb_builder.add_send(MOUNT_REQ_SIZE, gs_rank, req_tag)
    lbl_gs_req_i = gs_builder.add_recv(MOUNT_REQ_SIZE, slb_rank, req_tag)
    lbl_gs_req_o = gs_builder.add_send(MOUNT_REQ_SIZE, mds_rank, req_tag)
    lbl_mds_req = mds_builder.add_recv(MOUNT_REQ_SIZE, gs_rank, req_tag)

    # Step 2: Lookup map of slices
    lbl_mds_load = mds_builder.add_calc(calc_io_time(MOUNT_RESP_SIZE, 'read'))

    # Step 3: Reply with map of slices
    #   MDS -> GS -> SLB -> Host
    resp_tag = get_new_tag()
    lbl_mds_resp = mds_builder.add_send(MOUNT_RESP_SIZE, gs_rank, resp_tag)
    lbl_gs_resp_i = gs_builder.add_recv(MOUNT_RESP_SIZE, mds_rank, resp_tag)
    lbl_gs_resp_o = gs_builder.add_send(MOUNT_RESP_SIZE, slb_rank, resp_tag)
    lbl_slb_resp_i = slb_builder.add_recv(MOUNT_RESP_SIZE, gs_rank, resp_tag)
    lbl_slb_resp_o = slb_builder.add_send(MOUNT_RESP_SIZE, host_rank, resp_tag)
    lbl_host_resp = host_builder.add_recv(MOUNT_RESP_SIZE, slb_rank, resp_tag)

    # Dependencies
    host_builder.require_dependency(lbl_host_resp, lbl_host_req)
    gs_builder.require_dependency(lbl_gs_req_o, lbl_gs_req_i)
    gs_builder.require_dependency(lbl_gs_resp_o, lbl_gs_resp_i)
    slb_builder.require_dependency(lbl_slb_req_o, lbl_slb_req_i)
    slb_builder.require_dependency(lbl_slb_resp_o, lbl_slb_resp_i)
    mds_builder.require_dependency(lbl_mds_load, lbl_mds_req)
    mds_builder.require_dependency(lbl_mds_resp, lbl_mds_load)

    return [lbl_host_resp]


def resolve_to_slices_and_sizes(slice_map: SliceMap, data_start: int, data_end: int) -> List[Tuple[SliceId, int]]:
    # By doing this manually we can terminate early by only looking once at the data
    results = []
    for (sid, (start, end)) in enumerate(slice_map):
        if end < data_start:
            continue
        if data_end < start:
            break
        dist = min(end, data_end) - min(start, data_start)
        results.append((sid, dist))

    return results


def inject_read(network: 'DirectDriveNetwork', host_id: int, start: Addr, length: int, depends_on=[]):
    get_new_tag = network.get_next_tag
    get_builder = network.get_builder

    slice_ids = resolve_to_slices_and_sizes(
        network.slice_map, start, start+length)

    host_rank = network.topology.get_host(host_id)
    host_builder = get_builder(host_rank)
    bss_builders = {
        id: [
            get_builder(network.topology.get_bss(bss_id))
            for bss_id in network.bss_resp[network.slice_resp[id]]
        ]
        for (id, _) in slice_ids
    }
    ccs_builders = {
        id: get_builder(network.topology.get_ccs(network.slice_resp[id]))
        for (id, _) in slice_ids
    }

    result_lbls = []

    # for (id, _) in slice_ids:
    for (id, size) in slice_ids:
        # Part A: Request all SqNs (Assumption)
        ccs_builder = ccs_builders.get(id)
        assert ccs_builder, f"CCS builder for slice {id} missing"
        ccs_rank = ccs_builder.rank_id

        sqn_tag = get_new_tag()
        # Step 1: Host(VDC) -> CCS: Request SqN
        lbl_host_req_sqn = host_builder.add_send(
            LOOKUP_REQ_SIZE, ccs_rank, sqn_tag)
        lbl_ccs_req_sqn = ccs_builder.add_recv(
            LOOKUP_REQ_SIZE, host_rank, sqn_tag)

        # Step 2: Lookup Sqn
        ccs_builder.add_calc(calc_io_time(LOOKUP_RESP_SIZE, 'read'))

        # Step 3: CCS -> Host(VDC): Send Sqn
        lbl_ccs_resp_sqn = ccs_builder.add_send(
            LOOKUP_RESP_SIZE, host_rank, sqn_tag)
        lbl_host_resp_sqn = host_builder.add_recv(
            LOOKUP_RESP_SIZE, ccs_rank, sqn_tag)

        # Step 1-3: Dependencies
        host_builder.require_dependency(
            lbl_host_resp_sqn, lbl_host_req_sqn)
        ccs_builder.require_dependency(lbl_ccs_resp_sqn, lbl_ccs_req_sqn)

        # Part B: Read all slice data
        resp_bss_builders = bss_builders.get(id)
        assert resp_bss_builders, f"BSS builders for slice {id} missing"
        bss_builder = resp_bss_builders[network.get_next_bss(
            id) % len(resp_bss_builders)]
        bss_rank = bss_builder.rank_id

        recv_tag = get_new_tag()
        # Step 1: Host(VDC) -> BSS: Request slice data
        lbl_host_req_slice = host_builder.add_send(
            LOOKUP_REQ_SIZE, bss_rank, recv_tag)
        lbl_bss_req_slice = bss_builder.add_recv(
            LOOKUP_REQ_SIZE, host_rank, recv_tag)

        # Step 2: Lookup Slice data
        bss_builder.add_calc(calc_io_time(size, 'read'))

        # Step 3: BSS -> Host(VDC): Send slice data
        lbl_bss_resp_slice = bss_builder.add_send(
            size, host_rank, recv_tag)
        lbl_host_resp_slice = host_builder.add_recv(
            size, bss_rank, recv_tag)
        # We don't need this later on

        # Dependencies
        host_builder.require_dependency(lbl_host_req_slice, lbl_host_resp_sqn)
        bss_builder.require_dependency(lbl_bss_resp_slice, lbl_bss_req_slice)

        for d in depends_on:
            host_builder.require_dependency(lbl_host_req_sqn, d)

        result_lbls.append(lbl_host_resp_slice)

    return result_lbls


def inject_write(network: 'DirectDriveNetwork', host_id: int, start: Addr, length: int, depends_on=[]):
    get_new_tag = network.get_next_tag
    get_builder = network.get_builder

    slice_ids = resolve_to_slices_and_sizes(
        network.slice_map, start, start + length)

    host_rank = network.topology.get_host(host_id)
    host_builder = get_builder(host_rank)
    bss_builders = {
        id: [
            get_builder(network.topology.get_bss(bss_id))
            for bss_id in network.bss_resp[network.slice_resp[id]]
        ]
        for (id, _) in slice_ids
    }
    ccs_builders = {
        id: get_builder(network.topology.get_ccs(network.slice_resp[id]))
        for (id, _) in slice_ids
    }

    result_lbls = []
    for (id, size) in slice_ids:
        ccs_builder = ccs_builders.get(id)
        assert ccs_builder, f"CCS builder for slice {id} missing"
        ccs_rank = ccs_builder.rank_id

        data_tag = get_new_tag()
        # Step 1: Host(VDC) -> CCS: Send data
        lbl_host_req_sqn = host_builder.add_send(
            size, ccs_rank, data_tag)
        lbl_ccs_req_sqn = ccs_builder.add_recv(
            size, host_rank, data_tag)

        # Step 2: Store data on CCS
        lbl_ccs_store = ccs_builder.add_calc(calc_io_time(size, 'write'))
        ccs_builder.require_dependency(lbl_ccs_store, lbl_ccs_req_sqn)

        # Step 3: CCS -> all(BSS): Replicate data
        resp_bss_builders = bss_builders.get(id)
        assert resp_bss_builders, f"BSS builders for slice {id} missing"

        sqn_promise_lbls = []
        for bss_builder in resp_bss_builders:
            bss_rank = bss_builder.rank_id
            repl_tag = get_new_tag()
            # Step 3a: Send data from CCS to BSS
            lbl_ccs_replicate = ccs_builder.add_send(
                size, bss_rank, repl_tag)
            lbl_bss_replicate = bss_builder.add_recv(
                size, ccs_rank, repl_tag)

            # Step 3b: BSS writes data
            lbl_bss_store = bss_builder.add_calc(calc_io_time(size, 'write'))

            sqn_tag = get_new_tag()
            # Step 3c: BSS responds with SqN to CCS
            lbl_bss_sqn = bss_builder.add_send(
                LOOKUP_REQ_SIZE, ccs_rank, sqn_tag)
            lbl_ccs_sqn = ccs_builder.add_recv(
                LOOKUP_REQ_SIZE, bss_rank, sqn_tag)

            # Dependencies
            bss_builder.require_dependency(lbl_bss_sqn, lbl_bss_store)
            bss_builder.require_dependency(
                lbl_bss_store, lbl_bss_replicate)
            ccs_builder.require_dependency(
                lbl_ccs_replicate, lbl_ccs_store)
            ccs_builder.require_dependency(lbl_ccs_sqn, lbl_ccs_replicate)
            sqn_promise_lbls.append(lbl_ccs_sqn)

        # Step 4: CCS -> Host: Reply with sqn
        host_sqn_tag = get_new_tag()
        lbl_ccs_sqn_resp = ccs_builder.add_send(
            LOOKUP_RESP_SIZE, host_rank, host_sqn_tag)
        lbl_host_sqn_resp = host_builder.add_recv(
            LOOKUP_RESP_SIZE, ccs_rank, host_sqn_tag)

        result_lbls.append(lbl_host_sqn_resp)

        # TODO pjordan: Figure out how to support quorums beside N=M
        # Quorum check before sending back the promise
        for lbl_ccs_sqn in sqn_promise_lbls:
            ccs_builder.require_dependency(lbl_ccs_sqn_resp, lbl_ccs_sqn)

        host_builder.require_dependency(
            lbl_host_sqn_resp, lbl_host_req_sqn)

        for d in depends_on:
            host_builder.require_dependency(lbl_host_req_sqn, d)

    return result_lbls

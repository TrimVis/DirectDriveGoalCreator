
from typing import List, Optional

from common import Addr, SliceId, SliceMap, InteractionKind


# Interactions
class Interaction:
    kind: InteractionKind
    id: Optional[str]

    host_id: int
    # Used to define range which should be accessed
    data_start: Addr
    data_end: Addr

    def inject(self, network):  # : network.DirectDriveNetwork):
        del network
        raise NotImplementedError()


class IoInteraction(Interaction):
    # Used to define range which should be accessed
    data_start: Addr
    data_end: Addr

    def resolve_to_slices(self, slice_map: SliceMap):
        slices: List[SliceId] = []

        for (sid, (start, end)) in enumerate(slice_map):
            if (
                # Slice is completely within data region
                (self.data_start <= start and end < self.data_end) or
                # Beginning of slice is withing data region
                (self.data_start <= start and start < self.data_end) or
                # End of slice is withing data region
                (self.data_end > end and end > self.data_start)
            ):
                slices.append(sid)

        return slices


class ReadInteraction(IoInteraction):
    def __init__(self, start: Addr, length: int, host_id: int):
        self.kind = InteractionKind.READ
        self.host_id = host_id
        self.data_start = start
        self.data_end = start + length

    def inject(self, network):  # : network.DirectDriveNetwork):
        assert self.kind == InteractionKind.READ
        get_new_tag = network.get_next_tag
        get_builder = network.get_builder

        slice_ids = self.resolve_to_slices(network.slice_map)

        host_rank = network.topology.get_os(self.host_id)
        host_builder = get_builder(host_rank)
        bss_builders = {
            id: [
                get_builder(network.topology.get_bss(bss_id))
                for bss_id in network.bss_resp[network.slice_resp[id]]
            ]
            for id in slice_ids
        }
        ccs_builders = {
            id: get_builder(network.topology.get_ccs(network.slice_resp[id]))
            for id in slice_ids
        }

        # Part A: Request all SqNs (Assumption)
        for id in slice_ids:
            ccs_builder = ccs_builders.get(id)
            assert ccs_builder, f"CCS builder for slice {id} missing"
            ccs_rank = ccs_builder.rank_id

            sqn_tag = get_new_tag()
            # Step 1: Host(VDC) -> CCS: Request SqN
            lbl_host_req_sqn = host_builder.add_send(1, ccs_rank, sqn_tag)
            lbl_ccs_req_sqn = ccs_builder.add_recv(1, host_rank, sqn_tag)

            # Step 2: Lookup Sqn
            ccs_builder.add_calc(200)

            # Step 3: CCS -> Host(VDC): Send Sqn
            lbl_ccs_resp_sqn = ccs_builder.add_send(1, host_rank, sqn_tag)
            lbl_host_resp_sqn = host_builder.add_recv(1, ccs_rank, sqn_tag)

            # Step 1-3: Dependencies
            host_builder.require_dependency(
                lbl_host_resp_sqn, lbl_host_req_sqn)
            ccs_builder.require_dependency(lbl_ccs_resp_sqn, lbl_ccs_req_sqn)

        # Part B: Read all slice data
        for id in slice_ids:
            resp_bss_builders = bss_builders.get(id)
            assert resp_bss_builders, f"BSS builders for slice {id} missing"
            # TODO pjordan: Don't always choose 0
            bss_builder = resp_bss_builders[0]
            bss_rank = bss_builder.rank_id

            recv_tag = get_new_tag()
            # Step 1: Host(VDC) -> BSS: Request slice data
            lbl_host_req_slice = host_builder.add_send(1, bss_rank, recv_tag)
            lbl_bss_req_slice = bss_builder.add_recv(1, host_rank, recv_tag)

            # Step 2: Lookup Slice data
            bss_builder.add_calc(200)

            # Step 3: BSS -> Host(VDC): Send slice data
            lbl_bss_resp_slice = bss_builder.add_send(1, host_rank, recv_tag)
            lbl_host_resp_slice = host_builder.add_recv(1, bss_rank, recv_tag)

            # Step 1-3: Dependencies
            host_builder.require_dependency(
                lbl_host_resp_slice, lbl_host_req_slice)
            bss_builder.require_dependency(
                lbl_bss_resp_slice, lbl_bss_req_slice)


class WriteInteraction(IoInteraction):
    kind: InteractionKind = InteractionKind.WRITE

    def __init__(self, start: Addr, length: int, host_id: int):
        self.host_id = host_id
        self.data_start = start
        self.data_end = start + length

    def inject(self, network: 'DirectDriveNetwork'):
        assert self.kind == InteractionKind.WRITE
        get_new_tag = network.get_next_tag
        get_builder = network.get_builder

        slice_ids = self.resolve_to_slices(network.slice_map)

        host_rank = network.topology.get_os(self.host_id)
        host_builder = get_builder(host_rank)
        bss_builders = {
            id: [
                get_builder(network.topology.get_bss(bss_id))
                for bss_id in network.bss_resp[network.slice_resp[id]]
            ]
            for id in slice_ids
        }
        ccs_builders = {
            id: get_builder(network.topology.get_ccs(network.slice_resp[id]))
            for id in slice_ids
        }

        for id in slice_ids:
            ccs_builder = ccs_builders.get(id)
            assert ccs_builder, f"CCS builder for slice {id} missing"
            ccs_rank = ccs_builder.rank_id

            data_tag = get_new_tag()
            # Step 1: Host(VDC) -> CCS: Send data
            # TODO pjordan: Use actual data size
            lbl_host_req_sqn = host_builder.add_send(1024, ccs_rank, data_tag)
            lbl_ccs_req_sqn = ccs_builder.add_recv(1024, host_rank, data_tag)

            # Step 2: Store data on CCS
            lbl_ccs_store = ccs_builder.add_calc(200)
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
                    1024, bss_rank, repl_tag)
                lbl_bss_replicate = bss_builder.add_recv(
                    1024, ccs_rank, repl_tag)

                # Step 3b: BSS writes data
                lbl_bss_store = bss_builder.add_calc(200)

                sqn_tag = get_new_tag()
                # Step 3c: BSS responds with SqN to CCS
                lbl_bss_sqn = bss_builder.add_send(1, ccs_rank, sqn_tag)
                lbl_ccs_sqn = ccs_builder.add_recv(1, bss_rank, sqn_tag)

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
            lbl_ccs_sqn_resp = ccs_builder.add_send(1, host_rank, host_sqn_tag)
            lbl_host_sqn_resp = host_builder.add_recv(
                1, ccs_rank, host_sqn_tag)

            # Quorum check before sending back the promise
            for lbl_ccs_sqn in sqn_promise_lbls:
                ccs_builder.require_dependency(lbl_ccs_sqn_resp, lbl_ccs_sqn)

            host_builder.require_dependency(
                lbl_host_sqn_resp, lbl_host_req_sqn)

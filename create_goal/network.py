import math
from dataclasses import dataclass

from rank import RankBuilder
from interaction import Interaction, WriteInteraction, ReadInteraction
from typing import List, Optional
from common import Addr, Id, SliceMap, SliceResponsibility, BssResponsibility


@dataclass(frozen=True, kw_only=True)
class NetworkTopology:
    os_count: Id = 1
    slb_count: Id = 1
    gs_count: Id = 1
    mds_count: Id = 1
    ccs_count: Id = 1
    bss_count: Id = 1

    def is_valid(self) -> bool:
        for (name, value) in vars(self).items():
            if value < 1:
                print(f"Topology invalid: {name} >= 1 (is {value}")
                return False
        return True

    def _get(self, id: int, count: int, offset: int):
        assert id < count
        return offset + id

    def get_os(self, id: int):
        offset = 0
        return self._get(id, self.os_count, offset)

    def get_slb(self, id: int):
        offset = self.os_count
        return self._get(id, self.slb_count, offset)

    def get_gs(self, id: int):
        offset = self.os_count + self.slb_count
        return self._get(id, self.gs_count, offset)

    def get_mds(self, id: int):
        offset = self.os_count + self.slb_count + self.gs_count
        return self._get(id, self.mds_count, offset)

    def get_ccs(self, id: int):
        offset = self.os_count + self.slb_count +\
            self.gs_count + self.mds_count
        return self._get(id, self.ccs_count, offset)

    def get_bss(self, id: int):
        offset = self.os_count + self.slb_count +\
            self.gs_count + self.mds_count + self.ccs_count
        return self._get(id, self.bss_count, offset)

    def get_total_ranks(self):
        return self.os_count + self.slb_count +\
            self.gs_count + self.mds_count +\
            self.ccs_count + self.bss_count


class DirectDriveNetwork:
    topology: NetworkTopology
    slice_map: SliceMap
    slice_resp: SliceResponsibility
    bss_resp: BssResponsibility

    next_label_id: int = 0
    next_tag_id: int = 0

    builders: List[RankBuilder]
    # Although this list is ordered, due to non-blocking behaviour of
    # some interactions it does not mean they are executed in this order
    interactions: List[Interaction]

    def __init__(self, topology: NetworkTopology,
                 disk_size: int, slice_size: int):
        assert topology.is_valid(), "Network topology invalid: All entries should be >= 1"
        self.topology = topology

        # TODO pjordan: These args are a little weird
        # resp and slice_map creation should be handled in a different place
        # to allow various structures
        no_slices = math.ceil(disk_size / slice_size)
        self.slice_map = [
            (slice_size * id, slice_size * (id + 1))
            for id in range(no_slices)
        ]
        self.slice_resp = [
            id % topology.ccs_count
            for id in range(no_slices)
        ]
        bss_factor = math.ceil(topology.bss_count / topology.ccs_count)
        self.bss_resp = [
            [
                ccs_id * bss_factor + id for id in range(bss_factor)
            ]
            for ccs_id in range(topology.ccs_count)
        ]
        no_ranks = self.topology.get_total_ranks()
        self.builders = [
            RankBuilder(rid, self.get_next_label)
            for rid in range(no_ranks)
        ]
        self.interactions = []

        # TODO pjordan: Add this
        # Inject comments in builders for readability
        for b in self.builders:
            b.add_comment('')

    def add_interaction(self, *, op_code: str, asu: int,
                        address: int, size: int):
        if op_code == "R":
            self.add_read(asu, address, size)
        elif op_code == "W":
            self.add_write(asu, address, size)
        else:
            raise Exception("Unknown interaction type!")

    def add_read(self, host: int, address: Addr, size: int):
        interaction = ReadInteraction(address, size, host)
        self.interactions.append(interaction)

    def add_write(self, host: int, address: Addr, size: int):
        interaction = WriteInteraction(address, size, host)
        self.interactions.append(interaction)

    def inject_interactions(self):
        # Inject all interactions into builders
        for interaction in self.interactions:
            interaction.inject(self)

    def to_goal(self):
        # Create 'header' containing the num ranks
        no_ranks = self.topology.get_total_ranks()
        header = f'num_ranks {no_ranks}\n\n'
        # Concat all serialized rankbuilders
        body = '\n\n'.join([b.serialize() for b in self.builders])
        return header + body

    def get_builder(self, rank_id: int):
        return self.builders[rank_id]

    def get_next_label(self, prefix: Optional[str] = 'l') -> str:
        new_id = self.next_label_id
        self.next_label_id += 1
        return f'{prefix}{new_id}'

    def get_next_tag(self) -> int:
        new_id = self.next_label_id
        self.next_tag_id += 1
        return new_id

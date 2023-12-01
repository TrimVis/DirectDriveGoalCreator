import json
import math
import random
from dataclasses import dataclass
from loguru import logger
from tqdm import tqdm
from typing import List, Optional, Dict

from .rank import RankBuilder
from .interaction import inject_mount, inject_read, inject_write
from .common import Addr, Id, SliceMap, SliceResponsibility, BssResponsibility


@dataclass(frozen=True, kw_only=True)
class NetworkTopology:
    host_count: Id = 1
    slb_count: Id = 1
    gs_count: Id = 1
    mds_count: Id = 1
    ccs_count: Id = 1
    bss_count: Id = 1

    def __post_init__(self):
        # Update the total number of ranks
        logger.info("Created network topology:")
        logger.info("hosts: {}; slbs: {}; gs: {}; mds: {}; ccs: {}; bss: {}",
                    self.host_count, self.slb_count, self.gs_count, self.mds_count, self.ccs_count, self.bss_count)

    def is_valid(self) -> bool:
        for (name, value) in vars(self).items():
            if value < 1:
                logger.error(f"Topology invalid: {name} >= 1 (is {value}")
                return False
        return True

    def _get(self, id: int, count: int, offset: int):
        assert id < count
        return offset + id

    def get_host(self, id: int):
        offset = 0
        return self._get(id, self.host_count, offset)

    def get_slb(self, id: int):
        offset = self.host_count
        return self._get(id, self.slb_count, offset)

    def get_gs(self, id: int):
        offset = self.host_count + self.slb_count
        return self._get(id, self.gs_count, offset)

    def get_mds(self, id: int):
        offset = self.host_count + self.slb_count + self.gs_count
        return self._get(id, self.mds_count, offset)

    def get_ccs(self, id: int):
        offset = self.host_count + self.slb_count +\
            self.gs_count + self.mds_count
        return self._get(id, self.ccs_count, offset)

    def get_bss(self, id: int):
        offset = self.host_count + self.slb_count +\
            self.gs_count + self.mds_count + self.ccs_count
        return self._get(id, self.bss_count, offset)

    def get_total_ranks(self):
        return self.host_count + self.slb_count +\
            self.gs_count + self.mds_count +\
            self.ccs_count + self.bss_count

    def to_file(self, dest):
        value = {}

        for i in range(self.host_count):
            value[str(self.get_host(i))] = f"Host {i}"
        for i in range(self.slb_count):
            value[str(self.get_slb(i))] = f"SLB {i}"
        for i in range(self.gs_count):
            value[str(self.get_gs(i))] = f"GS {i}"
        for i in range(self.mds_count):
            value[str(self.get_mds(i))] = f"MDS {i}"
        for i in range(self.ccs_count):
            value[str(self.get_ccs(i))] = f"CCS {i}"
        for i in range(self.bss_count):
            value[str(self.get_bss(i))] = f"BSS {i}"

        json_value = json.dumps(value)
        with open(dest, "w+") as f:
            f.writelines(json_value)


VALID_NEXT_STRATEGIES = ['round-robin', 'random', 'first']


class DirectDriveNetwork:
    topology: NetworkTopology
    slice_map: SliceMap
    slice_resp: SliceResponsibility
    bss_resp: BssResponsibility

    next_counter: Dict[str, int] = {}

    next_ccs_strategy: str = "round-robin"
    next_bss_strategy: str = "round-robin"
    next_gs_strategy: str = "first"
    next_slb_strategy: str = "first"
    next_mds_strategy: str = "first"

    builders: List[RankBuilder]

    def __init__(self, topology: NetworkTopology,
                 disk_size: int, slice_size: int,
                 next_ccs_strategy: Optional[str] = None,
                 next_bss_strategy: Optional[str] = None,
                 next_gs_strategy: Optional[str] = None,
                 next_slb_strategy: Optional[str] = None,
                 next_mds_strategy: Optional[str] = None,
                 ):
        logger.info("Creating DirectDriveNetwork with:")
        logger.info("disk sizes: {}; slice_size: {}", disk_size, slice_size)
        assert topology.is_valid(), "Network topology invalid: All entries should be >= 1"
        self.topology = topology

        # TODO pjordan: These args are a little weird
        # resp and slice_map creation should be handled in a different place
        # to allow various structures
        logger.debug("Creating slice_map")
        no_slices = math.ceil(disk_size / slice_size)
        self.slice_map = [
            (slice_size * id, slice_size * (id + 1))
            for id in range(no_slices)
        ]
        logger.debug("Creating slice_resp")
        self.slice_resp = [
            id % topology.ccs_count
            for id in range(no_slices)
        ]
        logger.debug("Creating bss_resp")
        bss_factor = math.ceil(topology.bss_count / topology.ccs_count)
        self.bss_resp = [
            [
                ccs_id * bss_factor + id for id in range(bss_factor)
            ]
            for ccs_id in range(topology.ccs_count)
        ]
        logger.debug("Creating builders")
        no_ranks = self.topology.get_total_ranks()
        self.builders = [
            RankBuilder(rid, self.get_next_label)
            for rid in range(no_ranks)
        ]

        # Inject comments in builders for readability
        logger.debug("Adding rank comments")
        for i in range(self.topology.host_count):
            self.builders[self.topology.get_host(i)].add_comment(f'Host #{i}')
        for i in range(self.topology.slb_count):
            self.builders[self.topology.get_slb(i)].add_comment(f'SLB #{i}')
        for i in range(self.topology.gs_count):
            self.builders[self.topology.get_gs(i)].add_comment(f'GS #{i}')
        for i in range(self.topology.mds_count):
            self.builders[self.topology.get_mds(i)].add_comment(f'MDS #{i}')
        for i in range(self.topology.ccs_count):
            self.builders[self.topology.get_ccs(i)].add_comment(f'CCS #{i}')
        for i in range(self.topology.bss_count):
            self.builders[self.topology.get_bss(i)].add_comment(f'BSS #{i}')

        # Checking strategies
        if next_gs_strategy:
            assert next_gs_strategy in VALID_NEXT_STRATEGIES, "Next GS strategy is not supported"
            self.next_gs_strategy = next_gs_strategy
        if next_mds_strategy:
            assert next_mds_strategy in VALID_NEXT_STRATEGIES, "Next MDS strategy is not supported"
            self.next_mds_strategy = next_mds_strategy
        if next_ccs_strategy:
            assert next_ccs_strategy in VALID_NEXT_STRATEGIES, "Next CCS strategy is not supported"
            self.next_ccs_strategy = next_ccs_strategy
        if next_bss_strategy:
            assert next_bss_strategy in VALID_NEXT_STRATEGIES, "Next BSS strategy is not supported"
            self.next_bss_strategy = next_bss_strategy
        if next_slb_strategy:
            assert next_slb_strategy in VALID_NEXT_STRATEGIES, "Next SLB strategy is not supported"
            self.next_slb_strategy = next_slb_strategy

        logger.success("Finished DirectDriveNetwork initialization")

    def add_interaction(self, *, op_code: str, asu: int,
                        address: int, size: int):
        if op_code == "r":
            self.add_read(asu, address, size)
        elif op_code == "w":
            self.add_write(asu, address, size)
        else:
            raise Exception("Unknown interaction type!")

    def add_read(self, host: int, address: Addr, size: int):
        inject_read(self, host, address, size)

    def add_write(self, host: int, address: Addr, size: int):
        inject_write(self, host, address, size)

    def add_mount(self, host: int):
        inject_mount(self, host)

    def to_goal(self, dest_file: str = "./out.goal"):
        logger.info("Creating goal file at: {}", dest_file)
        with open(dest_file, 'w+') as f:
            # Create 'header' containing the num ranks
            no_ranks = self.topology.get_total_ranks()
            header = f'num_ranks {no_ranks}\n\n'
            f.write(header)
            for b in tqdm(self.builders):
                f.write(b.serialize())
                f.write('\n')

    def get_builder(self, rank_id: int):
        return self.builders[rank_id]

    def _get_next_counter(self, lbl: str, *, modulo: Optional[int] = None) -> int:
        next = self.next_counter.get(lbl, 0)

        if modulo:
            self.next_counter[lbl] = (next + 1) % modulo
        else:
            self.next_counter[lbl] = next + 1

        return next

    def _get_next_strategy_counter(self, lbl: str, strategy: str, *, modulo: int):
        if strategy == 'round-robin':
            return self._get_next_counter(lbl, modulo=modulo)
        elif strategy == 'random':
            return random.randint(0, modulo)
        elif strategy == 'first':
            return 0
        raise RuntimeError('Invalid strategy')

    def get_next_label(self, prefix: Optional[str] = 'l') -> str:
        return f"{prefix}{self._get_next_counter('label')}"

    def get_next_tag(self) -> int:
        return self._get_next_counter('tag')

    def get_next_bss(self, slice_id: Optional[int]) -> int:
        return self._get_next_strategy_counter(
            f'bss{slice_id}' if slice_id else 'bss',
            self.next_bss_strategy,
            modulo=self.topology.bss_count
        )

    def get_next_ccs(self) -> int:
        return self._get_next_strategy_counter('ccs', self.next_ccs_strategy, modulo=self.topology.ccs_count)

    def get_next_mds(self) -> int:
        return self._get_next_strategy_counter('mds', self.next_mds_strategy, modulo=self.topology.mds_count)

    def get_next_gs(self) -> int:
        return self._get_next_strategy_counter('gs', self.next_gs_strategy, modulo=self.topology.gs_count)

    def get_next_slb(self) -> int:
        return self._get_next_strategy_counter('slb', self.next_slb_strategy, modulo=self.topology.slb_count)

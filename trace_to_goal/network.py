import json
import math
import random
import os
from loguru import logger
from tqdm import tqdm
from typing import List, Optional, Dict, Literal
from pathlib import Path

from .rank import RankBuilder
from .interaction import inject_mount, inject_read, inject_write
from .common import Addr, Id, SliceMap, SliceResponsibility, \
    BssResponsibility, DEFAULT_DUMP_DIR

VALID_TOPOLOGY_STRATEGIES = ['grouped-by-kind', 'fat-tree']
TopologyStrategy = Literal['grouped-by-kind', 'fat-tree']


class NetworkTopology:
    host_count: Id = 1
    slb_count: Id = 1
    gs_count: Id = 1
    mds_count: Id = 1
    ccs_count: Id = 1
    bss_count: Id = 1
    strategy: TopologyStrategy = 'grouped-by-kind'
    mapping: Dict = {}

    def _init_grouped_by_kind_state(self):
        self.mapping = {}

        def spread_across_network(kind, count, offset):
            for i in range(count):
                key = f'{kind}{i}'
                pos = offset + i
                self.mapping[key] = pos

        spread_across_network('host', self.host_count, 0)
        spread_across_network('slb', self.slb_count, self.host_count)
        spread_across_network('gs', self.gs_count,
                              self.host_count + self.slb_count)
        spread_across_network('mds', self.mds_count,
                              self.host_count + self.slb_count + self.gs_count)
        spread_across_network('ccs', self.ccs_count, self.host_count +
                              self.slb_count + self.gs_count + self.mds_count)
        spread_across_network('bss', self.bss_count, self.host_count +
                              self.slb_count + self.gs_count + self.mds_count + self.ccs_count)

    def _init_fattree_state(self):
        self.mapping = {}
        # Spread all components evenly across the network
        no_total_ranks = self.get_total_ranks()

        def spread_across_network(kind, count):
            fac = no_total_ranks / (count + 1)
            for i in range(count):
                key = f'{kind}{i}'
                pos = round((i + 1) * fac)
                if pos in self.mapping.values():
                    pos_l = (pos - 1) % no_total_ranks
                    pos_r = (pos + 1) % no_total_ranks
                    while pos_l in self.mapping.values() and pos_r in self.mapping.values():
                        pos_l = (pos_l - 1) % no_total_ranks
                        pos_r = (pos_r + 1) % no_total_ranks

                    if pos_l not in self.mapping.values():
                        self.mapping[key] = pos_l
                    else:
                        self.mapping[key] = pos_r
                else:
                    self.mapping[key] = pos

        spread_across_network('host', self.host_count)
        spread_across_network('slb', self.slb_count)
        spread_across_network('gs', self.gs_count)
        spread_across_network('mds', self.mds_count)
        spread_across_network('ccs', self.ccs_count)
        spread_across_network('bss', self.bss_count)

    def __init__(self, *, host_count=None, slb_count=None, gs_count=None, mds_count=None, ccs_count=None, bss_count=None, strategy=None):
        if host_count is not None:
            self.host_count = host_count
        if slb_count is not None:
            self.slb_count = slb_count
        if gs_count is not None:
            self.gs_count = gs_count
        if mds_count is not None:
            self.mds_count = mds_count
        if ccs_count is not None:
            self.ccs_count = ccs_count
        if bss_count is not None:
            self.bss_count = bss_count
        if strategy is not None:
            self.strategy = strategy

        if self.strategy == 'fat-tree':
            self._init_fattree_state()
        elif self.strategy == 'grouped-by-kind':
            self._init_grouped_by_kind_state()
        else:
            assert True, "Your selected strategy is not valid"

        # Update the total number of ranks
        logger.info("Created network topology:")
        logger.info("hosts: {}; slbs: {}; gs: {}; mds: {}; ccs: {}; bss: {}",
                    self.host_count, self.slb_count, self.gs_count, self.mds_count, self.ccs_count, self.bss_count)

    def is_valid(self) -> bool:
        for (name, value) in vars(self).items():
            if name.endswith('_count') and value < 1:
                logger.error(f"Topology invalid: {name} >= 1 (is {value}")
                return False
        return True

    def _get(self, id: int, kind: str):
        return self.mapping.get(f'{kind}{id}')

    def get_host(self, id: int):
        return self._get(id, 'host')

    def get_slb(self, id: int):
        return self._get(id, 'slb')

    def get_gs(self, id: int):
        return self._get(id, 'gs')

    def get_mds(self, id: int):
        return self._get(id, 'mds')

    def get_ccs(self, id: int):
        return self._get(id, 'ccs')

    def get_bss(self, id: int):
        return self._get(id, 'bss')

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
NextStrategy = Literal['round-robin', 'random', 'first']


class DirectDriveNetwork:
    topology: NetworkTopology
    slice_map: SliceMap
    slice_resp: SliceResponsibility
    bss_resp: BssResponsibility

    next_counter: Dict[str, int] = {}

    next_ccs_strategy: NextStrategy = "round-robin"
    next_bss_strategy: NextStrategy = "round-robin"
    next_gs_strategy: NextStrategy = "first"
    next_slb_strategy: NextStrategy = "first"
    next_mds_strategy: NextStrategy = "first"

    builders: List[RankBuilder]

    op_depens: bool
    inplace: bool = False
    inplace_file: Optional[str] = None
    known_hosts: List[int] = []
    host_dependencies: Dict[int, List[str]] = {}

    def __init__(self, topology: NetworkTopology,
                 disk_size: int, slice_size: int,
                 next_ccs_strategy: Optional[NextStrategy] = None,
                 next_bss_strategy: Optional[NextStrategy] = None,
                 next_gs_strategy: Optional[NextStrategy] = None,
                 next_slb_strategy: Optional[NextStrategy] = None,
                 next_mds_strategy: Optional[NextStrategy] = None,
                 op_depens: bool = True,
                 dump_state: bool = False,
                 dump_folder: str = DEFAULT_DUMP_DIR
                 ):
        logger.info("Creating DirectDriveNetwork with:")
        logger.info("disk sizes: {}; slice_size: {}", disk_size, slice_size)
        assert topology.is_valid(), "Network topology invalid: All entries should be >= 1"
        self.topology = topology
        self.op_depens = op_depens
        self.dump_state = dump_state
        self.dump_folder = dump_folder
        assert not dump_state or dump_folder is not None, "None is not a valid value for the dump folder"

        if self.dump_state:
            logger.info("Creating dump folder to keep state:")
            # Create all the parent folders
            parent = Path(self.dump_folder).absolute()
            os.makedirs(parent, exist_ok=True)

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
            RankBuilder(rid, self.get_next_label,
                        dump_dir=self.dump_folder if self.dump_state else None)
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
        # Add mount on first interaction
        if asu not in self.known_hosts:
            self.host_dependencies[asu] = self.add_mount(asu)

        deps = self.host_dependencies[asu] if self.op_depens else []
        if op_code == "r":
            self.host_dependencies[asu] = self.add_read(
                asu, address, size, depends_on=deps)
        elif op_code == "w":
            self.host_dependencies[asu] = self.add_write(
                asu, address, size, depends_on=deps)
        else:
            raise Exception("Unknown interaction type!")

    def add_read(self, host: int, address: Addr, size: int, depends_on=[]):
        return inject_read(self, host, address, size, depends_on=depends_on)

    def add_write(self, host: int, address: Addr, size: int, depends_on=[]):
        return inject_write(self, host, address, size, depends_on=depends_on)

    def add_mount(self, host: int):
        return inject_mount(self, host)

    def to_goal(self, dest_file: str = "./out.goal"):
        logger.info("Creating goal file at: {}", dest_file)

        # Create all the parent folders
        parent = Path(dest_file).parent.absolute()
        os.makedirs(parent, exist_ok=True)

        with open(dest_file, 'w+') as f:
            # Create 'header' containing the num ranks
            no_ranks = self.topology.get_total_ranks()
            header = f'num_ranks {no_ranks}\n\n'
            f.write(header)
            for b in tqdm(self.builders):
                if self.dump_state:
                    b.serialize(append_file=f)
                else:
                    rank_res = b.serialize()
                    assert rank_res is not None, "unreachable"
                    f.write(rank_res)
                    del rank_res

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

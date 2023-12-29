import json

from typing import Dict  # , List, Tuple
from enum import Enum
from tqdm import tqdm

from .perfetto_wrapper import TProcess, TThread, TTrace, get_unique_uuid


def get_rank_label(rank_map, rank_id, descriptive=True):
    rank_label = rank_map.get(f"{rank_id}")
    if not descriptive:
        return rank_label or f"Rank {rank_id}"

    if rank_label:
        rank_label = f"Rank {rank_id}: " + rank_label
    else:
        rank_label = f"Rank {rank_id}"
    return rank_label


def id_generator():
    # Can't start at 0 as this is per default the "swapper"
    v = 1
    while True:
        yield v
        v += 1


class Operations(Enum):
    SEND = "osend"
    RECV = "orecv"
    TRANSMIT = "transmission"
    CALC = "loclop"
    NOISE = "noise"


class ChannelKind(Enum):
    CPU = "cpu"
    NUC = "nuc"


class Kind(Enum):
    SIMPLE = "simple"
    EXPERT = "expert"
    ADVANCED = "advanced"


class KindUtils:
    def __init__(self, kind):
        self.id_gen = id_generator()
        self.kind = kind

    def get_name(self, op):
        if op == Operations.RECV.value:
            return "Recv"
        elif op == Operations.SEND.value:
            return "Send"
        elif op == Operations.CALC.value:
            return "Calc"
        elif op == Operations.NOISE.value:
            return "Noise"
        elif op == Operations.TRANSMIT.value:
            return "Transmit"

    def get_from_thread_list(self, rank_id, c_kind, o_rank_id=None):
        if self.kind == Kind.SIMPLE:
            return self.threads[rank_id]
        elif self.kind == Kind.EXPERT:
            if c_kind == ChannelKind.CPU:
                return self.cpu_threads[rank_id]
            elif c_kind == ChannelKind.NUC:
                assert o_rank_id is not None, "Missing argument: o_rank_id"
                assert rank_id != o_rank_id, "Invalid communication found! From and to ranks are the same"
                if rank_id < o_rank_id:
                    return self.bidirectional_channels[rank_id][o_rank_id]
                else:
                    return self.bidirectional_channels[o_rank_id][rank_id]
        elif self.kind == Kind.ADVANCED:
            if c_kind == ChannelKind.CPU:
                return self.cpu_threads[rank_id]
            elif c_kind == ChannelKind.NUC:
                return self.nuc_threads[rank_id]

    def create_threads_list(self, rank_map, num_ranks):
        if self.kind == Kind.SIMPLE:
            self.threads = []

            for rank_id in range(num_ranks):
                thread_id = next(self.id_gen)
                rank_label = get_rank_label(rank_map, rank_id)
                self.threads.append(TThread(thread_id, rank_label))
        elif self.kind == Kind.ADVANCED:
            self.cpu_threads = []
            self.nuc_threads = []

            for rank_id in range(num_ranks):
                rank_label = get_rank_label(rank_map, rank_id)
                # get next available ids for threads
                cpu_id = next(self.id_gen)
                nuc_id = next(self.id_gen)
                self.cpu_threads.append(TThread(cpu_id, rank_label + " (CPU)"))
                self.nuc_threads.append(TThread(nuc_id, rank_label + " (NUC)"))
        elif self.kind == Kind.EXPERT:
            self.cpu_threads = []
            self.bidirectional_channels = {}

            for rank_id in range(num_ranks):
                rank_label = get_rank_label(rank_map, rank_id)
                # get next available id for cpu thread
                cpu_id = next(self.id_gen)
                self.cpu_threads.append(TThread(cpu_id, rank_label + " (CPU)"))
                # add bidirectional channels for all ranks larger than current rank
                self.bidirectional_channels[rank_id] = {
                    i: TThread(
                        next(self.id_gen),
                        rank_label + " <-> " +
                        get_rank_label(rank_map, i)
                        + " (NUC)"
                    )
                    for i in range(rank_id + 1, num_ranks)
                }

    def get_thread_list(self):
        if self.kind == Kind.SIMPLE:
            return self.threads
        elif self.kind == Kind.ADVANCED:
            return [*self.cpu_threads, *self.nuc_threads]
        elif self.kind == Kind.EXPERT:
            res = [*self.cpu_threads]
            for ch in self.bidirectional_channels.values():
                res.extend(ch.values())
            return res
        return []


class TraceBuilder:
    _kind: Kind
    _utils: KindUtils
    _rank_mappings: Dict[str, str]
    _process_name: str
    _viz_in_file: str

    def __init__(self, process_name="Network_Visualization"):
        self._process_name = process_name
        self._rank_mappings = {}

    def kind(self, kind: Kind):
        self._kind = kind
        self._utils = KindUtils(kind)
        return self

    def rank_name_map(self, rank_name_map_path):
        with open(rank_name_map_path, "r") as f:
            self._rank_mappings = json.loads(f.read())
        return self

    def viz_file(self, in_file):
        self._viz_in_file = in_file
        return self

    def build(self):
        lines = self._read_lines()
        numranks = int(lines[0][0:-2].split(" ")[1])

        # create a thread for each rank and work type and if necessary
        # also a thread for each communication channel (expert mode)
        self._utils.create_threads_list(self._rank_mappings, numranks)

        # Inject our operations into the corresponding threads
        self._inject_operations(lines)

        process = TProcess(0, self._process_name)

        # Add all our threads to our process
        threads = self._utils.get_thread_list()
        for i, t in enumerate(threads):
            process.add_thread(i, t)

        # Finally serialize the trace file
        trace = TTrace()
        trace.inject([process])
        # trace.serialize_to_file(out_file)

        return trace

    def _read_lines(self):
        with open(self._viz_in_file, "r") as f:
            return f.readlines()

    def _inject_operations(self, lines):
        # transmission queue
        transmissions = []

        # parse the remaining lines
        pbar = tqdm(total=(len(lines) - 1))
        for line in lines[1:]:
            params = line.split(" ")
            op = params[0]
            args = params[1:]

            if op == Operations.TRANSMIT.value:
                # Extract args
                src = int(args[0])
                dst = int(args[1])
                start = int(args[2])
                end = int(args[3])
                size = int(args[4])

                # Remember transmission for later, to make sure all send
                # and recv operations already exist
                transmissions += [(src, dst, start, end, size)]
            else:
                # Extract args
                rank = int(args[0])
                cpu = int(args[1])
                start = int(args[2])
                end = int(args[3])
                debug_vars = [
                    ("rank", str(rank)),
                    ("cpu", str(cpu)),
                ]

                # Add interaction
                name = self._utils.get_name(op)
                thread = self._utils.get_from_thread_list(
                    rank, ChannelKind.CPU)
                assert thread, "Something went wrong... Couldn't find corresponding thread"
                thread.add_event(name, estart=start, eend=end,
                                 op=op, debug=debug_vars)
                pbar.update(1)

        for (src, dst, start, end, size) in transmissions:
            self._inject_transmission(src, dst, start, end - 1, size)
            pbar.update(1)

    # NOTE pjordan: this is a little bit of a clusterfuck, we are breaking all
    # of the previously introduced abstractions to insert the flows properly
    # At least it works 😅
    # TODO pjordan: Refactor this
    def _inject_transmission(self, src, dst, start, end, size):
        flow_id = get_unique_uuid()
        transmit_name = self._utils.get_name(Operations.TRANSMIT.value)

        if self._kind != Kind.SIMPLE:
            nuc_thread = self._utils.get_from_thread_list(
                src, ChannelKind.NUC, o_rank_id=dst)
            assert nuc_thread
            assert start <= end
            nuc_thread.add_event(
                transmit_name,
                estart=start,
                # etime=end,
                eend=end,
                flow_ids=[flow_id],
                debug=[
                    ("size", str(size)),
                    ("estart", str(start)),
                    ("eend", str(end)),
                ],
            )

        src_thread = self._utils.get_from_thread_list(src, ChannelKind.CPU)
        assert src_thread
        if send_candidates := [
            (i, t)
            for (i, t) in enumerate(src_thread.event_params)
            if t[5] == Operations.SEND.value
            and int(t[2]) <= int(start)
            and not t[4]
        ]:
            (send_id, send_t) = max(
                send_candidates,
                key=lambda t: t[1][1],
            )
            src_thread.event_params[send_id] = (
                send_t[0],
                send_t[1],
                # Extend end time for send event to the transmit end time
                # in simple mode
                send_t[2],  # send_t[2] if self._kind != Kind.SIMPLE else end,
                send_t[3],
                [flow_id, *send_t[4]] if send_t[4] else [flow_id],
                send_t[5],
                send_t[6],
            )

        dst_thread = self._utils.get_from_thread_list(dst, ChannelKind.CPU)
        assert dst_thread
        if recv_candidates := [
            (i, t)
            for (i, t) in enumerate(dst_thread.event_params)
            if t[5] == Operations.RECV.value
            and end <= t[1]
            and not t[4]
        ]:
            (recv_id, recv_t) = min(
                recv_candidates,
                key=lambda t: t[1][1],
            )

            dst_thread.event_params[recv_id] = (
                recv_t[0],
                recv_t[1],
                recv_t[2],
                recv_t[3],
                [flow_id, *recv_t[4]] if recv_t[4] else [flow_id],
                recv_t[5],
                recv_t[6],
            )

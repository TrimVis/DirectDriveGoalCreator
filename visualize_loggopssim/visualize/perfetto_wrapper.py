from random import randint

from .perfetto_trace_pb2 import (
    Trace,
    TracePacket,
    TrackDescriptor,
    ThreadDescriptor,
    ProcessDescriptor,
    TrackEvent,
    DebugAnnotation,
)

MAX_UUID = 2**64 - 1
KNOWN_UUIDS = []

# This is a required field, not sure why though
TRUSTED_PACKET_SEQ_ID = randint(0, 2**16)


def get_unique_uuid():
    uuid = None
    while not uuid or uuid in KNOWN_UUIDS:
        uuid = randint(0, MAX_UUID)

    KNOWN_UUIDS.append(uuid)
    return uuid


class TThread:
    def __init__(self, tid, tname):
        self.tid = tid
        self.tname = tname
        self.event_params = []
        self.uuid = get_unique_uuid()

    def add_event(
        self, ename, estart=None, eend=None, etime=None, flow_ids=None, op=None, debug=None
    ):
        assert ename is not None, "Provide an event name"
        assert (
            estart is not None or etime is not None
        ), "Please provide a start or instant time for the event"
        assert not (
            (estart is not None or eend is not None) and etime is not None
        ), "The event has to be instant or a slice, not both"

        # This is used as an intermediary, so we can after the fact inject flows
        self.event_params += [(ename, estart, eend, etime, flow_ids, op, debug)]

    def inject_event(self, packet_list, ename, estart, eend, etime, flow_ids, debug):
        if estart is not None:
            packet_list += [
                TracePacket(
                    timestamp=estart,
                    track_event=TrackEvent(
                        type=TrackEvent.Type.TYPE_SLICE_BEGIN,
                        track_uuid=self.uuid,
                        flow_ids=flow_ids,
                        name=ename,
                        debug_annotations=[DebugAnnotation(name=k, string_value=v) for (k, v) in debug] if debug else None,
                    ),
                    trusted_packet_sequence_id=TRUSTED_PACKET_SEQ_ID,
                )
            ]
        if eend is not None:
            packet_list += [
                TracePacket(
                    timestamp=eend,
                    track_event=TrackEvent(
                        type=TrackEvent.Type.TYPE_SLICE_END,
                        track_uuid=self.uuid,
                    ),
                    trusted_packet_sequence_id=TRUSTED_PACKET_SEQ_ID,
                )
            ]

        if etime is not None:
            packet_list += [
                TracePacket(
                    timestamp=etime,
                    track_event=TrackEvent(
                        type=TrackEvent.Type.TYPE_INSTANT,
                        track_uuid=self.uuid,
                        name=ename,
                        debug_annotations=[DebugAnnotation(name=k, string_value=v) for (k, v) in debug] if debug else None,
                    ),
                    trusted_packet_sequence_id=TRUSTED_PACKET_SEQ_ID,
                )
            ]

    def inject(self, packet_list, parent):
        # Inject thread initializer
        thread_init_packet = TracePacket(
            track_descriptor=TrackDescriptor(
                uuid=self.uuid,
                parent_uuid=parent.uuid,
                thread=ThreadDescriptor(
                    pid=parent.pid,
                    tid=self.tid,
                    thread_name=self.tname,
                ),
            ),
            trusted_packet_sequence_id=TRUSTED_PACKET_SEQ_ID,
        )
        packet_list += [thread_init_packet]

        # Inject all events
        for ename, estart, eend, etime, flow_ids, _, debug in self.event_params:
            self.inject_event(packet_list, ename, estart, eend, etime, flow_ids, debug)


class TProcess:
    def __init__(self, pid, pname):
        self.pid = pid
        self.pname = pname
        self.threads = {}
        self.uuid = get_unique_uuid()

    def add_thread(self, id, thread):
        self.threads[id] = thread

    def get_thread(self, id):
        return self.threads.get(id, None)

    def inject(self, packet_list):
        # Inject process init packet
        process_init_packet = TracePacket(
            track_descriptor=TrackDescriptor(
                uuid=self.uuid,
                process=ProcessDescriptor(
                    pid=self.pid,
                    process_name=self.pname,
                ),
            ),
            trusted_packet_sequence_id=TRUSTED_PACKET_SEQ_ID,
        )
        packet_list += [process_init_packet]

        # Inject thread packets
        for t in self.threads.values():
            t.inject(packet_list, self)


class TTrace:
    def __init__(self):
        self.packet_list = []

    def inject(self, process_list):
        for p in process_list:
            p.inject(self.packet_list)

    def serialize(self):
        trace = Trace(packet=self.packet_list)
        return trace.SerializeToString()

    def serialize_to_file(self, file_path):
        with open(file_path, "wb+") as f:
            f.write(self.serialize())

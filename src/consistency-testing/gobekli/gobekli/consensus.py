from collections import namedtuple


class Violation(Exception):
    def __init__(self, message):
        self.message = message
        super().__init__(self.message)


AcceptedWrite = namedtuple('AcceptedWrite',
                           ['idx', 'write_id', 'version', 'value'])
Write = namedtuple('Write', ['prev_write_id', 'write_id', 'version', 'value'])


def idstr(write):
    return write.write_id + ":" + str(write.version)


# https://en.wikipedia.org/wiki/Linearizability
#
# Linearizability is a property of a concurrent / distributed system. We say
# that an access to data structure is linearizable if even when operations
# overlap, each operation appears to take place instantaneously.
#
# This means that we can think of a linearizable data structure as if it was
# implemented as a remote single threaded server.
#
# Formally it means that if we were given a log of data access across all
# clients consisting of all opearionts including start (ts), end (te) and the
# results of the operations then for each operation we could choose a moment tx
# (ts <= tx <= te) such that if we sorted all the operations by tx and executed
# them in order then we would observed the same results.
#
# To check linearizability of a register (a remote variable) we should sort the
# write / read events by their start and end time and for each event execute
# a corresponding method (from a single thread).
#
# The algorith is inspired by the "Testing shared memories" by Phillip Gibbons
# and Ephraim Korach
# http://citeseerx.ist.psu.edu/viewdoc/summary?doi=10.1.1.107.3013
#
# We use write_id to do read-mapping and use compare and set on write_id to
# resolve write-order. Since we require that operations are already sorted the
# complexity falls form O(N ln N) class to O(N) class where N is the length of
# a history. It looks like a shifting of the responsibility but if all
# operations are initiated from the same node they already happen in time so we
# don't need to sort and can check consistency on the fly.
class LinearizabilityRegisterChecker:
    def __init__(self):
        self.head = None
        self.pending_writes = dict()
        self.applied = dict()
        self.history_by_idx = dict()
        self.history_by_write_id = dict()
        self.reads = dict()

    # set an initial value of a register
    # value consists of an actual value, write_id and version
    #
    #   write_id - is a unique id of an attempt to change the register
    #              if the attempt succeeds id is stored as part of the
    #              value; it's important that each operation has its own
    #              unique id
    #
    #   version  - is an monotonic part of a value, version of the current value
    #              must be greater that the previous version. Unlike write_id
    #              versions don't need to be unique and multiple concurrent
    #              attempts to change the value may use the same version. So a
    #              client may read the current value's version, increment it and
    #              issue an update.
    #
    #              version is only needed to garbage collect timed out
    #              operations in order not to store them indefinitely
    #
    def init(self, write_id, version, value):
        self.head = AcceptedWrite(0, write_id, version, value)
        self.history_by_idx[self.head.idx] = self.head
        self.history_by_write_id[self.head.write_id] = self.head

    def write_started(self, prev_write_id, write_id, version, value):
        self.applied[write_id] = False
        self.pending_writes[write_id] = Write(prev_write_id, write_id, version,
                                              value)

    def write_ended(self, write_id):
        assert write_id in self.applied

        if self.applied[write_id]:
            del self.applied[write_id]
            return

        assert write_id in self.pending_writes

        self.observe(write_id)
        del self.applied[write_id]

    def observe(self, write_id):
        write = self.pending_writes[write_id]
        chain = []
        while True:
            if write.prev_write_id == self.head.write_id:
                chain.append(write)
                if self.head.version >= write.version:
                    raise Violation(
                        " -> ".join(map(idstr, chain)) + " -> " +
                        write.prev_write_id +
                        " doesn't lead to the latest observed state: " +
                        idstr(self.head))
                for w in reversed(chain):
                    self.applied[w.write_id] = True
                    del self.pending_writes[w.write_id]
                    self.head = AcceptedWrite(self.head.idx + 1, w.write_id,
                                              w.version, w.value)
                    self.history_by_idx[self.head.idx] = self.head
                    self.history_by_write_id[self.head.write_id] = self.head
                break
            elif write.prev_write_id in self.pending_writes:
                chain.append(write)
                if self.pending_writes[
                        write.prev_write_id].version >= write.version:
                    raise Violation(
                        " -> ".join(map(idstr, chain)) + " -> " +
                        write.prev_write_id +
                        " doesn't lead to the pending state: " +
                        idstr(self.pending_writes[write.prev_write_id]))
                write = self.pending_writes[write.prev_write_id]
            else:
                chain.append(write)
                raise Violation(
                    " -> ".join(map(idstr, chain)) + " -> " +
                    write.prev_write_id +
                    " doesn't lead to the latest observed state: " +
                    idstr(self.head))

    def write_canceled(self, write_id):
        assert write_id in self.applied

        if write_id in self.pending_writes:
            del self.pending_writes[write_id]
            del self.applied[write_id]
        else:
            if self.applied[write_id]:
                raise Violation("Can't cancel an already applied write: " +
                                write_id)
            del self.applied[write_id]

    def write_timeouted(self, write_id):
        assert write_id in self.applied
        del self.applied[write_id]

    def read_started(self, pid):
        assert pid not in self.reads
        self.reads[pid] = self.head.idx

    def read_ended(self, pid, write_id, value):
        assert pid in self.reads

        if write_id in self.pending_writes:
            self.observe(write_id)

        if write_id in self.history_by_write_id:
            assert self.reads[pid] in self.history_by_idx
            read_write = self.history_by_write_id[write_id]
            known_write = self.history_by_idx[self.reads[pid]]
            if read_write.version < known_write.version:
                raise Violation(
                    f"Stale read {idstr(read_write)} while {idstr(known_write)} was already known when the read started"
                )
            if read_write.value != value:
                raise Violation(
                    f"Read value {value} doesn't match written value {read_write.value}"
                )
            del self.reads[pid]
        else:
            raise Violation(f"Stale or phantom read {write_id}")

    def read_canceled(self, pid):
        del self.reads[pid]

# Copyright 2024 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from enum import Enum


class State(Enum):
    STARTED = 0
    CONSTRUCTING = 1
    CONSTRUCTED = 2
    TX = 3
    COMMIT = 4
    OK = 5
    ERROR = 6
    VIOLATION = 7
    EVENT = 8
    ABORT = 9
    LOG = 10
    SEND = 11
    READ = 12
    SEEN = 13


cmds = {
    "started": State.STARTED,
    "constructing": State.CONSTRUCTING,
    "constructed": State.CONSTRUCTED,
    "tx": State.TX,
    "cmt": State.COMMIT,
    "brt": State.ABORT,
    "ok": State.OK,
    "err": State.ERROR,
    "event": State.EVENT,
    "violation": State.VIOLATION,
    "log": State.LOG,
    "send": State.SEND,
    "read": State.READ,
    "seen": State.SEEN
}

threads = {
    "producing": {
        State.STARTED: [State.CONSTRUCTING],
        State.CONSTRUCTING: [State.CONSTRUCTED, State.ERROR],
        State.CONSTRUCTED: [State.SEND, State.CONSTRUCTING],
        State.SEND: [State.ABORT, State.COMMIT, State.ERROR],
        State.ABORT: [State.OK, State.ERROR],
        State.COMMIT: [State.OK, State.ERROR],
        State.OK: [State.SEND, State.CONSTRUCTING],
        State.ERROR: [State.CONSTRUCTING]
    },
    "streaming": {
        State.STARTED: [State.CONSTRUCTING],
        State.CONSTRUCTING: [State.CONSTRUCTED, State.ERROR],
        State.CONSTRUCTED: [State.READ, State.CONSTRUCTING],
        State.READ: [State.CONSTRUCTING, State.TX],
        State.TX: [State.ABORT, State.COMMIT, State.ERROR],
        State.ABORT: [State.OK, State.ERROR],
        State.COMMIT: [State.OK, State.ERROR],
        State.OK: [State.READ, State.CONSTRUCTING],
        State.ERROR: [State.CONSTRUCTING, State.READ]
    },
    "consuming": {
        State.STARTED: [State.CONSTRUCTING],
        State.CONSTRUCTING: [State.CONSTRUCTED, State.ERROR],
        State.CONSTRUCTED: [State.CONSTRUCTING, State.SEEN],
        State.SEEN: [State.SEEN, State.CONSTRUCTING]
    }
}

phantoms = [State.EVENT, State.VIOLATION, State.LOG]

/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "lsm/db/snapshot.h"

#include "lsm/core/internal/keys.h"

namespace lsm::db {

using internal::operator""_seqno;

snapshot::~snapshot() {
    _prev->_next = _next;
    _next->_prev = _prev;
}

std::unique_ptr<snapshot>
snapshot_list::create(internal::sequence_number seqno) {
    auto latest_seqno = newest_seqno().value_or(0_seqno);
    vassert(
      seqno >= latest_seqno,
      "snapshots may not be taken on older data: {} >= {}",
      seqno,
      latest_seqno);
    auto snap = std::unique_ptr<snapshot>(new snapshot(seqno));
    // Insert into our circularly linked list.
    snap->_next = _head._next;
    snap->_prev = &_head;
    _head._next->_prev = snap.get();
    _head._next = snap.get();
    return snap;
}

snapshot_list::snapshot_list()
  : _head(internal::sequence_number::min()) {
    // We make this a circularly linked list to simplify the logic (instead of
    // nullptr as a special sentinel value)
    _head._next = &_head;
    _head._prev = &_head;
}

} // namespace lsm::db

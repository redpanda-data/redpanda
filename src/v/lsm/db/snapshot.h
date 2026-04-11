// Copyright (c) 2014 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found at https://github.com/google/leveldb/blob/main/LICENSE. See
// https://github.com/google/leveldb/blob/main/AUTHORS for names of
// contributors.
//
// Modifications copyright 2025 Redpanda Data, Inc.

#pragma once

#include "lsm/core/internal/keys.h"

#include <memory>

namespace lsm::db {

class snapshot_list;

// An explicit snapshot of the database.
//
// Currently the database always takes a snapshot for individual reads, but this
// allows for multiple reads to work on the same snapshot.
class snapshot {
    explicit snapshot(internal::sequence_number seqno)
      : _seqno(seqno) {}

public:
    snapshot(const snapshot&) = delete;
    snapshot(snapshot&&) = delete;
    snapshot& operator=(const snapshot&) = delete;
    snapshot& operator=(snapshot&&) = delete;
    ~snapshot();

    // The sequence number that this snapshot was taken at.
    internal::sequence_number seqno() const { return _seqno; }

private:
    friend class snapshot_list;

    snapshot* _prev = nullptr;
    snapshot* _next = nullptr;
    internal::sequence_number _seqno;
};

// snapshot_list is a holder for the current list of held snapshots
class snapshot_list {
public:
    snapshot_list();
    snapshot_list(const snapshot_list&) = delete;
    snapshot_list(snapshot_list&&) = delete;
    snapshot_list& operator=(const snapshot_list&) = delete;
    snapshot_list& operator=(snapshot_list&&) = delete;
    ~snapshot_list() {
        vassert(
          empty(),
          "all snapshots must be released before the snapshot list is "
          "destructed");
    }

    // Create a new explicit snapshot at the given seqno.
    //
    // The snapshot will be released when destructed.
    std::unique_ptr<snapshot> create(internal::sequence_number seqno);

    // Return the newest sequence number of the snapshot held by readers.
    std::optional<internal::sequence_number> newest_seqno() const {
        if (empty()) {
            return std::nullopt;
        }
        return _head._next->seqno();
    }

    // Return the oldest sequence number of the snapshot held by readers.
    std::optional<internal::sequence_number> oldest_seqno() const {
        if (empty()) {
            return std::nullopt;
        }
        return _head._prev->seqno();
    }

    // Returns true if there no explicit snapshots held by readers.
    bool empty() const { return _head._next == &_head; }

private:
    friend class snapshot;

    snapshot _head;
};

} // namespace lsm::db

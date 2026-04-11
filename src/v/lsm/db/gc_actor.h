// Copyright (c) 2014 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found at https://github.com/google/leveldb/blob/main/LICENSE. See
// https://github.com/google/leveldb/blob/main/AUTHORS for names of
// contributors.
//
// Modifications copyright 2025 Redpanda Data, Inc.

#pragma once

#include "container/chunked_hash_map.h"
#include "lsm/core/internal/files.h"
#include "lsm/core/internal/options.h"
#include "lsm/db/table_cache.h"
#include "lsm/io/persistence.h"
#include "ssx/actor.h"

namespace lsm::db {

// A request to the GC actor to remove old unused files.
struct gc_message {
    // All the of the live files in the database at this time.
    chunked_hash_set<internal::file_handle> live_files;
    // The highest file ID that is safe to consider for GC.
    // Files with IDs > safe_highest_file_id are in-flight and must be skipped.
    // This is set to (min_in_flight_file_id - 1) by the manifest actor.
    internal::file_id safe_highest_file_id;
};

// An actor to remove old files from the database that are no longer being used.
//
// This actor only has a mailbox size of 1 and drops old requests when new ones
// come in because new requests will be inclusive of old ones.
class gc_actor
  : public ssx::actor<gc_message, 1, ssx::overflow_policy::drop_oldest> {
public:
    gc_actor(
      io::data_persistence* persistence,
      ss::lw_shared_ptr<internal::options> opts,
      table_cache* table_cache)
      : _persistence(persistence)
      , _opts(std::move(opts))
      , _table_cache(table_cache) {}

    size_t pending_delete_count() const { return _pending_deletes.size(); }

protected:
    ss::future<> process(gc_message msg) override;

    void on_error(std::exception_ptr ex) noexcept override;

private:
    io::data_persistence* _persistence;
    ss::lw_shared_ptr<internal::options> _opts;
    table_cache* _table_cache;
    chunked_hash_map<internal::file_handle, ss::lowres_clock::time_point>
      _pending_deletes;
};

} // namespace lsm::db

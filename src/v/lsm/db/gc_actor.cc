// Copyright (c) 2014 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found at https://github.com/google/leveldb/blob/main/LICENSE. See
// https://github.com/google/leveldb/blob/main/AUTHORS for names of
// contributors.
//
// Modifications copyright 2025 Redpanda Data, Inc.

#include "lsm/db/gc_actor.h"

#include "base/vlog.h"
#include "lsm/core/internal/logger.h"

namespace lsm::db {

ss::future<> gc_actor::process(gc_message msg) {
    vlog(log.trace, "gc_actor_process_start");

    auto now = ss::lowres_clock::now();

    // Lookup new files to remove
    auto gen = _persistence->list_files();
    while (auto file_handle_opt = co_await gen()) {
        if (_as.abort_requested()) {
            co_return;
        }
        auto& file_handle = file_handle_opt->get();
        if (msg.live_files.contains(file_handle)) {
            // This file is currently apart of the database, don't remove
            // it.
            continue;
        }
        if (file_handle.epoch > _opts->database_epoch) {
            // This file was written by a future epoch.
            continue;
        }
        if (
          file_handle.epoch == _opts->database_epoch
          && file_handle.id > msg.safe_highest_file_id) {
            // This file ID is at or above the minimum in-flight ID.
            // It could be currently being written, so skip deletion.
            continue;
        }
        auto it = _pending_deletes.find(file_handle);
        if (it == _pending_deletes.end()) {
            // This is our first time seeing a pending delete, store the
            // time at which we want to delete it.
            it = _pending_deletes.insert_or_assign(
              it,
              file_handle,
              now + absl::ToChronoNanoseconds(_opts->file_deletion_delay));
        }
    }

    // Remove any pending files
    int deleted = 0;
    auto it = _pending_deletes.begin();
    while (it != _pending_deletes.end()) {
        if (now < it->second) {
            ++it;
            continue;
        }
        co_await _table_cache->evict(it->first);
        co_await _persistence->remove_file(it->first);
        it = _pending_deletes.erase(it);
        ++deleted;
    }

    vlog(log.trace, "gc_actor_process_end deleted={}", deleted);
}

void gc_actor::on_error(std::exception_ptr ex) noexcept {
    vlog(log.warn, "gc_actor_process_end error=\"{}\"", ex);
}

} // namespace lsm::db

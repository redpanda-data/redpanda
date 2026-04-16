/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_topics/level_one/metastore/lsm/garbage_collector.h"

#include "base/vlog.h"
#include "cloud_topics/level_one/common/abstract_io.h"
#include "cloud_topics/level_one/common/object_id.h"
#include "cloud_topics/level_one/domain/domain_manager_probe.h"
#include "cloud_topics/level_one/metastore/lsm/keys.h"
#include "cloud_topics/level_one/metastore/lsm/replicated_db.h"
#include "cloud_topics/level_one/metastore/lsm/state_reader.h"
#include "cloud_topics/level_one/metastore/lsm/state_update.h"
#include "cloud_topics/logger.h"

namespace cloud_topics::l1 {

namespace {
template<typename... T>
db_garbage_collector::error wrap_read_err(
  state_reader::error e, fmt::format_string<T...> msg, T&&... args) {
    switch (e.e) {
    case state_reader::errc::corruption:
    case state_reader::errc::io_error:
        return std::move(e).wrap(
          db_garbage_collector::errc::io_error,
          std::move(msg),
          std::forward<T>(args)...);
    case state_reader::errc::shutting_down:
        return std::move(e).wrap(
          db_garbage_collector::errc::db_needs_reopen,
          std::move(msg),
          std::forward<T>(args)...);
    }
}

template<typename... T>
db_garbage_collector::error
wrap_update_err(db_update_error e, fmt::format_string<T...> msg, T&&... args) {
    switch (e.e) {
    case db_update_errc::invalid_input:
    case db_update_errc::invalid_update:
    case db_update_errc::corruption:
    case db_update_errc::io_error:
        return std::move(e).wrap(
          db_garbage_collector::errc::io_error,
          std::move(msg),
          std::forward<T>(args)...);
    case db_update_errc::shutting_down:
        return std::move(e).wrap(
          db_garbage_collector::errc::db_needs_reopen,
          std::move(msg),
          std::forward<T>(args)...);
    }
}

template<typename... T>
db_garbage_collector::error wrap_db_err(
  replicated_database::error e, fmt::format_string<T...> msg, T&&... args) {
    switch (e.e) {
    case replicated_database::errc::io_error:
    case replicated_database::errc::update_rejected:
    case replicated_database::errc::replication_error:
        return std::move(e).wrap(
          db_garbage_collector::errc::io_error,
          std::move(msg),
          std::forward<T>(args)...);
    case replicated_database::errc::not_leader:
    case replicated_database::errc::shutting_down:
        return std::move(e).wrap(
          db_garbage_collector::errc::db_needs_reopen,
          std::move(msg),
          std::forward<T>(args)...);
    }
}
} // namespace

db_garbage_collector::db_garbage_collector(io* io, domain_manager_probe* probe)
  : io_(io)
  , probe_(probe) {}

ss::future<std::expected<std::optional<object_id>, db_garbage_collector::error>>
db_garbage_collector::remove_unreferenced_batch(
  replicated_database* db,
  ss::abort_source* as,
  size_t batch_size,
  std::optional<object_id> start_point,
  model::timestamp prereg_expiry_cutoff,
  model::timestamp deletion_delay_cutoff,
  chunked_vector<object_id>& to_expire_out) {
    auto persisted_seqno = db->db().max_persisted_seqno();
    if (!persisted_seqno.has_value()) {
        // Nothing has been persisted yet, cannot remove anything.
        co_return std::nullopt;
    }
    auto iter_fut = co_await ss::coroutine::as_future(
      db->db().create_iterator());
    if (iter_fut.failed()) {
        auto ex = iter_fut.get_exception();
        auto errc = ssx::is_shutdown_exception(ex) ? errc::db_needs_reopen
                                                   : errc::io_error;
        co_return std::unexpected(
          error(errc, "Error creating iterator: {}", ex));
    }

    auto object_range_res = make_object_range(
      std::move(iter_fut.get()), start_point);
    if (!object_range_res.has_value()) {
        co_return std::unexpected(wrap_read_err(
          std::move(object_range_res.error()), "Error getting object range"));
    }
    auto object_gen = object_range_res.value().get_rows();
    chunked_vector<object_id> to_remove;
    size_t batch_expire_count = 0;
    std::optional<object_id> next_batch_start{std::nullopt};
    while (auto obj_ref_opt = co_await object_gen()) {
        if (as->abort_requested()) {
            co_return std::unexpected(error(
              errc::db_needs_reopen,
              "Aborted requested: {}",
              as->abort_requested_exception_ptr()));
        }
        auto obj_res = obj_ref_opt.value().get();
        if (!obj_res.has_value()) {
            co_return std::unexpected(wrap_read_err(
              std::move(obj_res.error()), "Error iterating through objects"));
        }
        auto& obj_row = obj_res.value();
        if (obj_row.seqno > *persisted_seqno) {
            // The latest value for this row isn't persisted yet. Don't
            // consider it for GC, to ensure read replica and restored
            // metastores can still see the objects.
            continue;
        }

        auto& obj_entry = obj_row.val.object;
        auto obj_key = object_row_key::decode(obj_row.key);
        auto oid = obj_key->oid;

        if (obj_entry.is_preregistration) {
            if (obj_entry.last_updated < prereg_expiry_cutoff) {
                vlog(
                  cd_log.debug,
                  "Expiring stale preregistered L1 object: {}",
                  oid);
                to_expire_out.emplace_back(oid);
                ++batch_expire_count;
                if (to_remove.size() + batch_expire_count == batch_size) {
                    if (auto next = next_uuid(oid()); next.has_value()) {
                        next_batch_start = object_id(*next);
                    }
                    break;
                }
            }
            continue;
        }

        if (obj_entry.removed_data_size > obj_entry.total_data_size) {
            vlog(
              cd_log.error,
              "Object {} row {} accounted as removed data {} > total data {}",
              oid,
              obj_row.key,
              obj_entry.removed_data_size,
              obj_entry.total_data_size);
            // Something is off about our accounting for this object; skip it,
            // but don't block the rest of GC.
            continue;
        }
        if (
          obj_entry.removed_data_size == obj_entry.total_data_size
          && obj_entry.last_updated <= deletion_delay_cutoff) {
            vlog(cd_log.debug, "Deleting L1 object: {}", oid);
            to_remove.emplace_back(oid);
            if (to_remove.size() + batch_expire_count == batch_size) {
                // Set an explicit next starting object if we had more than one
                // batch of objects so we can make incremental progress.
                // NOTE: nullopt if UUID_MAX (extremely unlikely overflow)
                if (auto next = next_uuid(oid()); next.has_value()) {
                    next_batch_start = object_id(*next);
                }
                break;
            }
        }
    }
    if (to_remove.empty() && batch_expire_count == 0) {
        vlog(cd_log.debug, "No objects eligible for garbage collection");
        co_return std::nullopt;
    }

    if (to_remove.empty()) {
        co_return next_batch_start;
    }

    auto num_to_remove = to_remove.size();
    auto del_res = co_await io_->delete_objects(to_remove.copy(), as);
    if (!del_res.has_value()) {
        co_return std::unexpected(
          error(errc::io_error, "Error deleting objects: {}", del_res.error()));
    }
    probe_->gc_objects_deleted(num_to_remove);
    remove_objects_db_update update{std::move(to_remove)};
    chunked_vector<write_batch_row> rows;
    auto build_res = co_await update.build_rows(rows);
    if (!build_res.has_value()) {
        co_return std::unexpected(wrap_update_err(
          std::move(build_res.error()),
          "Error building rows for object removal"));
    }
    if (!rows.empty()) {
        auto write_res = co_await db->write(std::move(rows));
        if (!write_res.has_value()) {
            co_return std::unexpected(wrap_db_err(
              std::move(write_res.error()),
              "Error replicating rows for object removal"));
        }
    }
    probe_->gc_object_deletions_replicated(num_to_remove);
    co_return next_batch_start;
}

ss::future<
  std::expected<chunked_vector<object_id>, db_garbage_collector::error>>
db_garbage_collector::remove_unreferenced_objects(
  replicated_database* db,
  ss::abort_source* as,
  size_t batch_size,
  model::timestamp prereg_expiry_cutoff,
  model::timestamp deletion_delay_cutoff) {
    chunked_vector<object_id> to_expire;
    std::optional<object_id> next_start;
    while (!as->abort_requested()) {
        auto batch_res = co_await remove_unreferenced_batch(
          db,
          as,
          batch_size,
          next_start,
          prereg_expiry_cutoff,
          deletion_delay_cutoff,
          to_expire);
        if (!batch_res.has_value()) {
            co_return std::unexpected(batch_res.error());
        }
        next_start = batch_res.value();
        if (!next_start.has_value()) {
            // Nothing left to remove.
            co_return to_expire;
        }
    }
    co_return std::unexpected(
      error(errc::db_needs_reopen, "Abort requested during GC"));
}

} // namespace cloud_topics::l1

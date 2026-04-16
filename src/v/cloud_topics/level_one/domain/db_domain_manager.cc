/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#include "cloud_topics/level_one/domain/db_domain_manager.h"

#include "cloud_topics/level_one/common/object_id.h"
#include "cloud_topics/level_one/metastore/domain_uuid.h"
#include "cloud_topics/level_one/metastore/lsm/garbage_collector.h"
#include "cloud_topics/level_one/metastore/lsm/keys.h"
#include "cloud_topics/level_one/metastore/lsm/state_reader.h"
#include "cloud_topics/level_one/metastore/lsm/state_update.h"
#include "cloud_topics/level_one/metastore/rpc_types.h"
#include "cloud_topics/level_one/metastore/state_update.h"
#include "cloud_topics/logger.h"
#include "lsm/io/cloud_persistence.h"
#include "lsm/proto/manifest.proto.h"
#include "ssx/sleep_abortable.h"

#include <seastar/core/sleep.hh>

namespace cloud_topics::l1 {
namespace {

rpc::errc
log_and_convert(const state_reader::error& e, std::string_view prefix) {
    using enum state_reader::errc;
    rpc::errc ret{};
    ss::log_level lvl{};
    switch (e.e) {
    case io_error:
        ret = rpc::errc::timed_out;
        lvl = ss::log_level::warn;
        break;
    case corruption:
        ret = rpc::errc::timed_out;
        lvl = ss::log_level::error;
        break;
    case shutting_down:
        ret = rpc::errc::not_leader;
        lvl = ss::log_level::debug;
        break;
    }
    vlogl(cd_log, lvl, "{}{}", prefix, e);
    return ret;
}

rpc::errc log_and_convert(const db_update_error& e, std::string_view prefix) {
    using enum db_update_errc;
    rpc::errc ret{};
    ss::log_level lvl{};
    switch (e.e) {
    case io_error:
        ret = rpc::errc::timed_out;
        lvl = ss::log_level::warn;
        break;
    case corruption:
        ret = rpc::errc::timed_out;
        lvl = ss::log_level::error;
        break;
    case shutting_down:
        ret = rpc::errc::not_leader;
        lvl = ss::log_level::debug;
        break;
    case invalid_input:
        // TODO: can there be a better error code for this?
        ret = rpc::errc::concurrent_requests;
        lvl = ss::log_level::error;
        break;
    case invalid_update:
        ret = rpc::errc::concurrent_requests;
        lvl = ss::log_level::debug;
        break;
    }
    vlogl(cd_log, lvl, "{}{}", prefix, e);
    return ret;
}

rpc::errc
log_and_convert(const replicated_database::error& e, std::string_view prefix) {
    rpc::errc ret{};
    ss::log_level lvl{};
    switch (e.e) {
    case replicated_database::errc::shutting_down:
        ret = rpc::errc::not_leader;
        lvl = ss::log_level::debug;
        break;
    case replicated_database::errc::io_error:
        ret = rpc::errc::timed_out;
        lvl = ss::log_level::warn;
        break;
    case replicated_database::errc::update_rejected:
        ret = rpc::errc::concurrent_requests;
        lvl = ss::log_level::warn;
        break;
    case replicated_database::errc::replication_error:
    case replicated_database::errc::not_leader:
        ret = rpc::errc::not_leader;
        lvl = ss::log_level::warn;
        break;
    }
    vlogl(cd_log, lvl, "{}{}", prefix, e);
    return ret;
}

// Scans up to `max_count` extents fully below `target_offset` (i.e.
// whose last_offset < target_offset). Returns target_offset if all
// such extents fit within `max_count`, otherwise returns the offset just
// past the last scanned extent.
ss::future<std::expected<kafka::offset, state_reader::error>>
scan_extents_below(
  state_reader& reader,
  const model::topic_id_partition& tp,
  kafka::offset target_offset,
  size_t max_count) {
    size_t count = 0;

    auto max_to_scan = kafka::prev_offset(target_offset);
    auto extents_res = co_await reader.get_inclusive_extents(
      tp, std::nullopt, max_to_scan);
    if (!extents_res.has_value()) {
        co_return std::unexpected(std::move(extents_res.error()));
    }

    if (!extents_res.value().has_value()) {
        // No extents in range, already at target.
        co_return target_offset;
    }

    kafka::offset last_offset{};
    auto gen = (*extents_res)->get_rows();
    while (auto row_opt = co_await gen()) {
        const auto& row = row_opt->get();
        if (!row.has_value()) {
            co_return std::unexpected(row.error());
        }
        if (row->val.last_offset >= target_offset) {
            // This extent and beyond includes the exclusive target, do not
            // include it.
            break;
        }
        ++count;
        last_offset = row->val.last_offset;
        if (count >= max_count) {
            // There are more extents than max_count below target_offset;
            // return what we scanned up through.
            co_return kafka::next_offset(last_offset);
        }
    }
    // There are under max_count extents below target_offset.
    co_return target_offset;
}

// Counts the total number of extents across all partitions for a topic,
// returning early once the count exceeds `max`.
ss::future<std::expected<size_t, state_reader::error>> count_topic_extents(
  state_reader& reader, const model::topic_id& tid, size_t max) {
    auto partitions_res = co_await reader.get_partitions_for_topic(tid);
    if (!partitions_res.has_value()) {
        co_return std::unexpected(std::move(partitions_res.error()));
    }
    size_t count = 0;
    for (const auto& pid : partitions_res.value()) {
        model::topic_id_partition tidp(tid, pid);
        auto extents_res = co_await reader.get_inclusive_extents(
          tidp, std::nullopt, std::nullopt);
        if (!extents_res.has_value()) {
            co_return std::unexpected(std::move(extents_res.error()));
        }
        if (!extents_res->has_value()) {
            continue;
        }
        auto gen = extents_res->value().get_rows();
        while (auto row_opt = co_await gen()) {
            const auto& row = row_opt->get();
            if (!row.has_value()) {
                co_return std::unexpected(row.error());
            }
            if (++count > max) {
                co_return max;
            }
        }
    }
    co_return count;
}

// Extracts topic IDs and topic_id_partitions from new_objects.
void collect_topics_and_partitions(
  const chunked_vector<new_object>& new_objects,
  absl::btree_set<model::topic_id>& topics,
  absl::btree_set<model::topic_id_partition>& partitions) {
    for (const auto& obj : new_objects) {
        for (const auto& [tid, pmap] : obj.extent_metas) {
            topics.insert(tid);
            for (const auto& [pid, _] : pmap) {
                partitions.emplace(tid, pid);
            }
        }
    }
}

// Extracts object IDs from new_objects.
void collect_object_ids(
  const chunked_vector<new_object>& new_objects,
  absl::btree_set<object_id>& oids) {
    for (const auto& obj : new_objects) {
        oids.insert(obj.oid);
    }
}

} // namespace

// entity_locks methods

ss::future<std::expected<void, rpc::errc>>
db_domain_manager::entity_locks::acquire_objects(
  absl::btree_set<object_id> additional) {
    vassert(
      object_locks.empty(),
      "Object locks already held; collect all object IDs before acquiring");
    try {
        auto new_units = co_await object_lock_map->acquire(additional);
        for (auto& u : new_units) {
            object_locks.push_back(std::move(u));
        }
        co_return std::expected<void, rpc::errc>{};
    } catch (...) {
        auto eptr = std::current_exception();
        auto lvl = ssx::is_shutdown_exception(eptr) ? ss::log_level::debug
                                                    : ss::log_level::warn;
        vlogl(
          cd_log, lvl, "Exception acquiring additional object locks: {}", eptr);
        co_return std::unexpected(rpc::errc::not_leader);
    }
}

db_domain_manager::db_domain_manager(
  model::term_id expected_term,
  ss::shared_ptr<stm> stm,
  std::filesystem::path staging_dir,
  cloud_io::remote* remote,
  cloud_storage_clients::bucket_name bucket,
  io* object_io,
  ss::scheduling_group sg,
  domain_manager_probe* probe)
  : expected_term_(expected_term)
  , staging_dir_(std::move(staging_dir))
  , remote_(remote)
  , bucket_(std::move(bucket))
  , object_io_(object_io)
  , sg_(sg)
  , stm_(std::move(stm))
  , gc_interval_(
      config::shard_local_cfg()
        .cloud_topics_long_term_garbage_collection_interval)
  , probe_(probe) {
    gc_interval_.watch([this]() { sem_.signal(); });
}

void db_domain_manager::start() {
    ssx::spawn_with_gate(gate_, [this] {
        return ss::with_scheduling_group(sg_, [this] { return gc_loop(); });
    });
}

ss::future<> db_domain_manager::stop_and_wait() {
    vlog(cd_log.debug, "DB domain manager stopping...");
    as_.request_abort();
    sem_.broken();
    partition_locks_.broken();
    object_locks_.broken();
    co_await gate_.close();
    auto wlock_res = co_await exclusive_db_lock();
    if (wlock_res.has_value()) {
        if (db_) {
            auto close_res = co_await db_->close();
            if (!close_res.has_value()) {
                vlog(
                  cd_log.warn, "Error closing database: {}", close_res.error());
            }
        }
    }
    vlog(cd_log.debug, "DB domain manager stopped...");
}

std::optional<ss::gate::holder> db_domain_manager::maybe_gate() {
    ss::gate::holder h;
    if (as_.abort_requested() || gate_.is_closed()) {
        return std::nullopt;
    }
    return gate_.hold();
}

ss::future<rpc::add_objects_reply>
db_domain_manager::add_objects(rpc::add_objects_request req) {
    // Collect all entities from request.
    absl::btree_set<model::topic_id> topics;
    absl::btree_set<model::topic_id_partition> partitions;
    absl::btree_set<object_id> oids;
    collect_topics_and_partitions(req.new_objects, topics, partitions);
    collect_object_ids(req.new_objects, oids);

    auto locks_res = co_await gate_and_open_writes({
      .topic_read_locks = std::move(topics),
      .partition_locks = std::move(partitions),
      .object_locks = std::move(oids),
    });
    if (!locks_res.has_value()) {
        co_return rpc::add_objects_reply{
          .ec = locks_res.error(),
        };
    }

    chunked_hash_map<model::topic_id_partition, kafka::offset> corrections;
    auto update = add_objects_db_update{
      .new_objects = std::move(req.new_objects),
      .new_terms = std::move(req.new_terms),
    };
    auto reader = state_reader(db_->db().create_snapshot());
    chunked_vector<write_batch_row> rows;
    auto build_res = co_await update.build_rows(reader, rows, &corrections);
    if (!build_res.has_value()) {
        co_return rpc::add_objects_reply{
          .ec = log_and_convert(
            build_res.error(), "Rejecting request to add objects: "),
        };
    }

    auto apply_res = co_await write_rows(locks_res.value(), std::move(rows));
    if (!apply_res.has_value()) {
        co_return rpc::add_objects_reply{
          .ec = apply_res.error(),
        };
    }

    co_return rpc::add_objects_reply{
      .ec = rpc::errc::ok,
      .corrected_next_offsets = std::move(corrections),
    };
}

ss::future<rpc::replace_objects_reply>
db_domain_manager::replace_objects(rpc::replace_objects_request req) {
    // Collect topics and partitions upfront.
    absl::btree_set<model::topic_id> topics;
    absl::btree_set<model::topic_id_partition> partitions;
    collect_topics_and_partitions(req.new_objects, topics, partitions);
    for (const auto& [tp, update] : req.compaction_updates) {
        topics.insert(tp.topic_id);
        partitions.insert(tp);
    }

    // Acquire topic and partition locks only — no object locks yet.
    auto locks_res = co_await gate_and_open_writes({
      .topic_read_locks = std::move(topics),
      .partition_locks = std::move(partitions),
    });
    if (!locks_res.has_value()) {
        co_return rpc::replace_objects_reply{
          .ec = locks_res.error(),
        };
    }

    chunked_hash_map<
      model::topic_id,
      chunked_hash_map<model::partition_id, compaction_state_update>>
      req_compaction_updates;
    for (auto& [tp, update] : req.compaction_updates) {
        const auto& t = tp.topic_id;
        const auto& p = tp.partition;
        req_compaction_updates[t][p] = std::move(update);
    }
    auto update = replace_objects_db_update{
      .new_objects = std::move(req.new_objects),
      .compaction_updates = std::move(req_compaction_updates),
    };

    // Discover old objects being replaced, merge with new object IDs,
    // and acquire all object locks in one sorted batch.
    {
        absl::btree_set<object_id> all_oids;
        collect_object_ids(update.new_objects, all_oids);

        auto discovery_reader = state_reader(db_->db().create_snapshot());
        auto discovered_res = co_await update.discover_replaced_object_ids(
          discovery_reader);
        if (!discovered_res.has_value()) {
            co_return rpc::replace_objects_reply{
              .ec = log_and_convert(
                discovered_res.error(), "Error discovering replaced objects: "),
            };
        }
        all_oids.merge(discovered_res.value());
        auto obj_locks_res = co_await locks_res.value().acquire_objects(
          std::move(all_oids));
        if (!obj_locks_res.has_value()) {
            co_return rpc::replace_objects_reply{
              .ec = obj_locks_res.error(),
            };
        }
    }

    // Final snapshot under full lock protection.
    auto reader = state_reader(db_->db().create_snapshot());
    chunked_vector<write_batch_row> rows;
    auto build_res = co_await update.build_rows(reader, rows);
    if (!build_res.has_value()) {
        co_return rpc::replace_objects_reply{
          .ec = log_and_convert(
            build_res.error(), "Rejecting request to replace objects: "),
        };
    }
    auto apply_res = co_await write_rows(locks_res.value(), std::move(rows));
    if (!apply_res.has_value()) {
        co_return rpc::replace_objects_reply{
          .ec = apply_res.error(),
        };
    }
    co_return rpc::replace_objects_reply{
      .ec = rpc::errc::ok,
    };
}

ss::future<rpc::get_first_offset_ge_reply>
db_domain_manager::get_first_offset_ge(rpc::get_first_offset_ge_request req) {
    auto gl_res = co_await gate_and_open_reads();
    if (!gl_res.has_value()) {
        co_return rpc::get_first_offset_ge_reply{.ec = gl_res.error()};
    }
    auto reader = state_reader(db_->db().create_snapshot());
    auto extent_res = co_await reader.get_extent_ge(req.tp, req.o);
    if (!extent_res.has_value()) {
        co_return rpc::get_first_offset_ge_reply{
          .ec = log_and_convert(extent_res.error(), "Error getting extent: "),
        };
    }
    if (!extent_res->has_value()) {
        co_return rpc::get_first_offset_ge_reply{
          .ec = rpc::errc::out_of_range,
        };
    }
    const auto& extent = extent_res.value().value();
    auto object_res = co_await reader.get_object(extent.oid);
    if (!object_res.has_value()) {
        co_return rpc::get_first_offset_ge_reply{
          .ec = log_and_convert(object_res.error(), "Error getting object: "),
        };
    }
    if (!object_res->has_value()) {
        co_return rpc::get_first_offset_ge_reply{
          .ec = rpc::errc::out_of_range,
        };
    }
    const auto& object = object_res.value().value();
    if (object.is_preregistration) {
        co_return rpc::get_first_offset_ge_reply{
          .ec = log_and_convert(
            state_reader::error(
              state_reader::errc::corruption,
              "Extent refers to a preregistered object"),
            "Error getting object"),
        };
    }
    co_return rpc::get_first_offset_ge_reply{
      .ec = rpc::errc::ok,
      .object = rpc::object_metadata{
        .oid = extent.oid,
        .footer_pos = object.footer_pos,
        .object_size = object.object_size,
        .first_offset = extent.base_offset,
        .last_offset = extent.last_offset,
      }};
}

ss::future<rpc::get_first_timestamp_ge_reply>
db_domain_manager::get_first_timestamp_ge(
  rpc::get_first_timestamp_ge_request req) {
    auto gl_res = co_await gate_and_open_reads();
    if (!gl_res.has_value()) {
        co_return rpc::get_first_timestamp_ge_reply{.ec = gl_res.error()};
    }
    auto reader = state_reader(db_->db().create_snapshot());
    auto extents_res = co_await reader.get_inclusive_extents(
      req.tp, req.o, std::nullopt);
    if (!extents_res.has_value()) {
        co_return rpc::get_first_timestamp_ge_reply{
          .ec = log_and_convert(
            extents_res.error(),
            fmt::format(
              "Error getting extents for {} timestamp: {}, min_offset: {}: ",
              req.tp,
              req.ts,
              req.o)),
        };
    }
    if (!extents_res.value().has_value()) {
        co_return rpc::get_first_timestamp_ge_reply{
          .ec = rpc::errc::out_of_range,
        };
    }

    // Find the first extent with max_timestamp >= ts.
    auto gen = extents_res.value().value().get_rows();
    while (auto row_opt = co_await gen()) {
        const auto& row = row_opt->get();
        if (!row.has_value()) {
            co_return rpc::get_first_timestamp_ge_reply{
              .ec = log_and_convert(
                row.error(), "Error iterating through extents: "),
            };
        }
        const auto& extent = row.value();
        if (extent.val.max_timestamp >= req.ts) {
            // Found a matching extent. Get the object info.
            auto object_res = co_await reader.get_object(extent.val.oid);
            if (!object_res.has_value()) {
                co_return rpc::get_first_timestamp_ge_reply{
                  .ec = log_and_convert(
                    object_res.error(),
                    fmt::format(
                      "Error getting object {} for {} timestamp: {}, "
                      "min_offset: {}: ",
                      extent.val.oid,
                      req.tp,
                      req.ts,
                      req.o)),
                };
            }
            if (!object_res.value().has_value()) {
                co_return rpc::get_first_timestamp_ge_reply{
                  .ec = rpc::errc::out_of_range,
                };
            }
            auto key = extent_row_key::decode(extent.key);
            const auto& object = object_res.value().value();
            if (object.is_preregistration) {
                co_return rpc::get_first_timestamp_ge_reply{
                  .ec = log_and_convert(
                    state_reader::error(
                      state_reader::errc::corruption,
                      "Extent refers to a preregistered object"),
                    "Error getting object"),
                };
            }
            co_return rpc::get_first_timestamp_ge_reply{
              .ec = rpc::errc::ok,
              .object = rpc::object_metadata{
                .oid = extent.val.oid,
                .footer_pos = object.footer_pos,
                .object_size = object.object_size,
                .first_offset = key->base_offset,
                .last_offset = extent.val.last_offset,
              },
            };
        }
    }

    co_return rpc::get_first_timestamp_ge_reply{
      .ec = rpc::errc::out_of_range,
    };
}

ss::future<rpc::get_first_offset_for_bytes_reply>
db_domain_manager::get_first_offset_for_bytes(
  rpc::get_first_offset_for_bytes_request req) {
    auto gl_res = co_await gate_and_open_reads();
    if (!gl_res.has_value()) {
        co_return rpc::get_first_offset_for_bytes_reply{.ec = gl_res.error()};
    }
    auto reader = state_reader(db_->db().create_snapshot());

    // Get metadata to find next_offset for size==0 case
    auto metadata_res = co_await reader.get_metadata(req.tp);
    if (!metadata_res.has_value()) {
        co_return rpc::get_first_offset_for_bytes_reply{
          .ec = log_and_convert(
            metadata_res.error(), "Error getting metadata: "),
        };
    }
    if (!metadata_res.value().has_value()) {
        vlog(cd_log.debug, "Partition {} not tracked", req.tp);
        co_return rpc::get_first_offset_for_bytes_reply{
          .ec = rpc::errc::missing_ntp,
        };
    }
    const auto& metadata = metadata_res.value().value();
    kafka::offset offset = metadata.next_offset;
    if (req.size == 0) {
        co_return rpc::get_first_offset_for_bytes_reply{
          .offset = offset,
          .ec = rpc::errc::ok,
        };
    }

    auto extents_res = co_await reader.get_inclusive_extents_backward(
      req.tp, std::nullopt, std::nullopt);
    if (!extents_res.has_value()) {
        co_return rpc::get_first_offset_for_bytes_reply{
          .ec = log_and_convert(
            metadata_res.error(), "Error getting backwards iterator: "),
        };
    }
    if (!extents_res.value().has_value()) {
        co_return rpc::get_first_offset_for_bytes_reply{
          .ec = rpc::errc::out_of_range,
        };
    }

    uint64_t remaining = req.size;
    auto gen = extents_res.value().value().get_rows();
    while (auto row_opt = co_await gen()) {
        const auto& row = row_opt->get();
        if (!row.has_value()) {
            co_return rpc::get_first_offset_for_bytes_reply{
              .ec = log_and_convert(
                metadata_res.error(), "Error iterating through extents: "),
            };
        }
        const auto& extent = row.value();
        auto key = extent_row_key::decode(extent.key);
        offset = key->base_offset;
        remaining -= std::min(remaining, extent.val.len);
        if (remaining == 0) {
            co_return rpc::get_first_offset_for_bytes_reply{
              .offset = offset,
              .ec = rpc::errc::ok,
            };
        }
    }

    co_return rpc::get_first_offset_for_bytes_reply{
      .ec = rpc::errc::out_of_range,
    };
}

ss::future<rpc::get_offsets_reply>
db_domain_manager::get_offsets(rpc::get_offsets_request req) {
    auto gl_res = co_await gate_and_open_reads();
    if (!gl_res.has_value()) {
        co_return rpc::get_offsets_reply{.ec = gl_res.error()};
    }
    auto reader = state_reader(db_->db().create_snapshot());
    auto metadata_res = co_await reader.get_metadata(req.tp);
    if (!metadata_res.has_value()) {
        co_return rpc::get_offsets_reply{
          .ec = log_and_convert(metadata_res.error(), "Error getting metadata"),
        };
    }
    if (!metadata_res->has_value()) {
        co_return rpc::get_offsets_reply{
          .ec = rpc::errc::missing_ntp,
        };
    }
    const auto& metadata = **metadata_res;
    co_return rpc::get_offsets_reply{
      .ec = rpc::errc::ok,
      .start_offset = metadata.start_offset,
      .next_offset = metadata.next_offset,
    };
}

ss::future<rpc::get_size_reply>
db_domain_manager::get_size(rpc::get_size_request req) {
    auto gl_res = co_await gate_and_open_reads();
    if (!gl_res.has_value()) {
        co_return rpc::get_size_reply{.ec = gl_res.error()};
    }
    auto reader = state_reader(db_->db().create_snapshot());
    auto metadata_res = co_await reader.get_metadata(req.tp);
    if (!metadata_res.has_value()) {
        co_return rpc::get_size_reply{
          .ec = log_and_convert(metadata_res.error(), "Error getting metadata"),
        };
    }
    if (!metadata_res->has_value()) {
        co_return rpc::get_size_reply{
          .ec = rpc::errc::missing_ntp,
        };
    }
    const auto& metadata = **metadata_res;
    co_return rpc::get_size_reply{
      .ec = rpc::errc::ok,
      .size = metadata.size,
      .num_extents = metadata.num_extents,
    };
}

ss::future<rpc::get_compaction_info_reply>
db_domain_manager::do_get_compaction_info(
  const gate_read_lock&,
  state_reader& reader,
  rpc::get_compaction_info_request req) {
    // Get metadata for start_offset and next_offset.
    auto metadata_res = co_await reader.get_metadata(req.tp);
    if (!metadata_res.has_value()) {
        co_return rpc::get_compaction_info_reply{
          .ec = log_and_convert(
            metadata_res.error(), "Error getting metadata: "),
        };
    }
    if (!metadata_res.value().has_value()) {
        co_return rpc::get_compaction_info_reply{
          .ec = rpc::errc::missing_ntp,
        };
    }
    const auto& metadata = **metadata_res;
    const auto start_offset = metadata.start_offset;
    const auto next_offset = metadata.next_offset;

    // Check for empty log.
    if (start_offset >= next_offset) {
        co_return rpc::get_compaction_info_reply{
          .ec = rpc::errc::ok,
          .dirty_ranges = {},
          .removable_tombstone_ranges = {},
          .dirty_ratio = 0.0,
          .earliest_dirty_ts = std::nullopt,
          .compaction_epoch = metadata.compaction_epoch,
          .start_offset = start_offset,
        };
    }

    // Get compaction state if any.
    auto compaction_res = co_await reader.get_compaction_metadata(req.tp);
    if (!compaction_res.has_value()) {
        co_return rpc::get_compaction_info_reply{
          .ec = log_and_convert(
            metadata_res.error(), "Error getting compaction metadata: "),
        };
    }

    offset_interval_set dirty_ranges;
    offset_interval_set removable_tombstone_ranges;
    std::optional<model::timestamp> earliest_dirty_ts;

    const auto log_last_offset = kafka::prev_offset(next_offset);

    if (!compaction_res.value().has_value()) {
        // Nothing has been compacted yet, the whole log is dirty.
        dirty_ranges.insert(start_offset, log_last_offset);
    } else {
        const auto& cmp_state = **compaction_res;

        // Compute dirty_ranges by inverting cleaned_ranges.
        auto offsets_stream = cmp_state.cleaned_ranges.make_stream();
        auto dirty_base_candidate = start_offset;
        while (offsets_stream.has_next()) {
            auto cleaned_range = offsets_stream.next();
            if (cleaned_range.base_offset > dirty_base_candidate) {
                dirty_ranges.insert(
                  dirty_base_candidate,
                  kafka::prev_offset(cleaned_range.base_offset));
            }
            dirty_base_candidate = kafka::next_offset(
              cleaned_range.last_offset);
        }
        if (dirty_base_candidate <= log_last_offset) {
            dirty_ranges.insert(dirty_base_candidate, log_last_offset);
        }

        // Collect removable_tombstone_ranges.
        for (const auto& r : cmp_state.cleaned_ranges_with_tombstones) {
            if (
              r.cleaned_with_tombstones_at
              <= req.tombstone_removal_upper_bound_ts) {
                removable_tombstone_ranges.insert(r.base_offset, r.last_offset);
            }
        }
    }

    // Iterate extents to compute dirty_ratio and earliest_dirty_ts.
    size_t total_size = 0;
    size_t dirty_size = 0;
    const auto& cleaned_ranges = compaction_res->has_value()
                                   ? (*compaction_res)->cleaned_ranges
                                   : offset_interval_set{};

    auto extents_res = co_await reader.get_inclusive_extents(
      req.tp, std::nullopt, std::nullopt);
    if (!extents_res.has_value()) {
        co_return rpc::get_compaction_info_reply{
          .ec = log_and_convert(extents_res.error(), "Error getting extents: "),
        };
    }
    if (extents_res.value().has_value()) {
        auto gen = (*extents_res)->get_rows();
        while (auto row_opt = co_await gen()) {
            const auto& row = row_opt->get();
            if (!row.has_value()) {
                co_return rpc::get_compaction_info_reply{
                  .ec = log_and_convert(
                    row.error(), "Error iterating through extents: "),
                };
            }
            const auto& extent = *row;
            auto key = extent_row_key::decode(extent.key);
            auto base = key->base_offset;
            if (base < start_offset) {
                // The extent is partially truncated.
                base = start_offset;
            }
            auto last = extent.val.last_offset;

            total_size += extent.val.len;
            if (!cleaned_ranges.covers(base, last)) {
                dirty_size += extent.val.len;
                // Track earliest dirty timestamp. We cannot assume timestamps
                // are monotonic with offsets, so we must find the minimum
                // across all dirty extents.
                if (
                  !earliest_dirty_ts.has_value()
                  || extent.val.max_timestamp < *earliest_dirty_ts) {
                    earliest_dirty_ts = extent.val.max_timestamp;
                }
            }
        }
    }

    double dirty_ratio = total_size == 0 ? 0.0
                                         : static_cast<double>(dirty_size)
                                             / static_cast<double>(total_size);

    co_return rpc::get_compaction_info_reply{
      .ec = rpc::errc::ok,
      .dirty_ranges = std::move(dirty_ranges),
      .removable_tombstone_ranges = std::move(removable_tombstone_ranges),
      .dirty_ratio = dirty_ratio,
      .earliest_dirty_ts = earliest_dirty_ts,
      .compaction_epoch = metadata.compaction_epoch,
      .start_offset = start_offset,
    };
}

ss::future<rpc::get_compaction_info_reply>
db_domain_manager::get_compaction_info(rpc::get_compaction_info_request req) {
    auto gl_res = co_await gate_and_open_reads();
    if (!gl_res.has_value()) {
        co_return rpc::get_compaction_info_reply{.ec = gl_res.error()};
    }
    auto reader = state_reader(db_->db().create_snapshot());
    co_return co_await do_get_compaction_info(gl_res.value(), reader, req);
}

ss::future<rpc::get_term_for_offset_reply>
db_domain_manager::get_term_for_offset(rpc::get_term_for_offset_request req) {
    auto gl_res = co_await gate_and_open_reads();
    if (!gl_res.has_value()) {
        co_return rpc::get_term_for_offset_reply{.ec = gl_res.error()};
    }
    auto reader = state_reader(db_->db().create_snapshot());
    auto metadata_res = co_await reader.get_metadata(req.tp);
    if (!metadata_res.has_value()) {
        co_return rpc::get_term_for_offset_reply{
          .ec = log_and_convert(
            metadata_res.error(), "Error getting metadata: "),
        };
    }
    if (!metadata_res.value().has_value()) {
        co_return rpc::get_term_for_offset_reply{
          .ec = rpc::errc::missing_ntp,
        };
    }
    const auto& metadata = metadata_res.value().value();
    if (req.offset > metadata.next_offset) {
        co_return rpc::get_term_for_offset_reply{
          .ec = rpc::errc::out_of_range,
        };
    }
    auto term_res = co_await reader.get_term_le(req.tp, req.offset);
    if (!term_res.has_value()) {
        co_return rpc::get_term_for_offset_reply{
          .ec = log_and_convert(term_res.error(), "Error getting term: "),
        };
    }
    if (!term_res.value().has_value()) {
        co_return rpc::get_term_for_offset_reply{
          .ec = rpc::errc::out_of_range,
        };
    }
    co_return rpc::get_term_for_offset_reply{
      .ec = rpc::errc::ok,
      .term = term_res.value().value().term_id,
    };
}

ss::future<rpc::get_end_offset_for_term_reply>
db_domain_manager::get_end_offset_for_term(
  rpc::get_end_offset_for_term_request req) {
    auto gl_res = co_await gate_and_open_reads();
    if (!gl_res.has_value()) {
        co_return rpc::get_end_offset_for_term_reply{.ec = gl_res.error()};
    }
    auto reader = state_reader(db_->db().create_snapshot());
    auto metadata_res = co_await reader.get_metadata(req.tp);
    if (!metadata_res.has_value()) {
        co_return rpc::get_end_offset_for_term_reply{
          .ec = log_and_convert(
            metadata_res.error(), "Error getting metadata: "),
        };
    }
    if (!metadata_res.value().has_value()) {
        co_return rpc::get_end_offset_for_term_reply{
          .ec = rpc::errc::missing_ntp,
        };
    }
    auto end_res = co_await reader.get_term_end(req.tp, req.term);
    if (!end_res.has_value()) {
        co_return rpc::get_end_offset_for_term_reply{
          .ec = log_and_convert(metadata_res.error(), "Error getting term: "),
        };
    }
    if (!end_res.value().has_value()) {
        co_return rpc::get_end_offset_for_term_reply{
          .ec = rpc::errc::out_of_range,
        };
    }
    co_return rpc::get_end_offset_for_term_reply{
      .ec = rpc::errc::ok,
      .end_offset = end_res.value().value(),
    };
}

ss::future<rpc::set_start_offset_reply> db_domain_manager::do_set_start_offset(
  const entity_locks& locks, rpc::set_start_offset_request req) {
    static constexpr size_t max_extents_per_batch = 1000;

    const auto target_offset = req.start_offset;
    kafka::offset current_start_offset{};

    // Get current start offset and validate.
    {
        auto reader = state_reader(db_->db().create_snapshot());
        auto meta_res = co_await reader.get_metadata(req.tp);
        if (!meta_res.has_value()) {
            co_return rpc::set_start_offset_reply{
              .ec = log_and_convert(
                meta_res.error(), "Error reading metadata for partition: "),
            };
        }
        if (!meta_res->has_value()) {
            co_return rpc::set_start_offset_reply{
              .ec = rpc::errc::missing_ntp,
            };
        }
        const auto& meta = meta_res->value();
        if (req.start_offset > meta.next_offset) {
            vlog(
              cd_log.debug,
              "Rejecting request to set {} start offset to {}, current next "
              "offset {}",
              req.tp,
              req.start_offset,
              meta.next_offset);
            co_return rpc::set_start_offset_reply{
              .ec = rpc::errc::concurrent_requests,
            };
        }
        current_start_offset = meta.start_offset;
    }

    if (current_start_offset >= target_offset) {
        co_return rpc::set_start_offset_reply{
          .ec = rpc::errc::ok,
        };
    }

    // Process one batch. Object locks must already be held by the caller.
    auto reader = state_reader(db_->db().create_snapshot());

    // Scan through extents to find an intermediate offset bounded by
    // max_extents_per_batch.
    auto new_start_res = co_await scan_extents_below(
      reader, req.tp, target_offset, max_extents_per_batch);
    if (!new_start_res.has_value()) {
        co_return rpc::set_start_offset_reply{
          .ec = log_and_convert(
            new_start_res.error(), "Error scanning extents: "),
        };
    }

    auto update = set_start_offset_db_update{
      .tp = req.tp,
      .new_start_offset = new_start_res.value(),
    };

    chunked_vector<write_batch_row> rows;
    bool is_no_op = false;
    auto build_res = co_await update.build_rows(reader, rows, &is_no_op);
    if (!build_res.has_value()) {
        co_return rpc::set_start_offset_reply{
          .ec = log_and_convert(
            build_res.error(), "Rejecting request to set start offset: "),
        };
    }
    if (is_no_op) {
        co_return rpc::set_start_offset_reply{
          .ec = rpc::errc::ok,
          .has_more = new_start_res.value() < target_offset,
        };
    }
    auto apply_res = co_await write_rows(locks, std::move(rows));
    if (!apply_res.has_value()) {
        co_return rpc::set_start_offset_reply{
          .ec = apply_res.error(),
        };
    }

    co_return rpc::set_start_offset_reply{
      .ec = rpc::errc::ok,
      .has_more = new_start_res.value() < target_offset,
    };
}

ss::future<rpc::set_start_offset_reply>
db_domain_manager::set_start_offset(rpc::set_start_offset_request req) {
    auto locks_res = co_await gate_and_open_writes({
      .topic_read_locks = {req.tp.topic_id},
      .partition_locks = {req.tp},
    });
    if (!locks_res.has_value()) {
        co_return rpc::set_start_offset_reply{
          .ec = locks_res.error(),
        };
    }

    // Discover all object IDs below the target offset and acquire their
    // locks before the batched write loop.
    {
        auto update = set_start_offset_db_update{
          .tp = req.tp,
          .new_start_offset = req.start_offset,
        };
        auto discovery_reader = state_reader(db_->db().create_snapshot());
        auto discovered_res = co_await update.discover_truncated_object_ids(
          discovery_reader);
        if (!discovered_res.has_value()) {
            co_return rpc::set_start_offset_reply{
              .ec = log_and_convert(
                discovered_res.error(),
                "Error discovering truncated objects: "),
            };
        }
        if (!discovered_res.value().empty()) {
            auto obj_locks_res = co_await locks_res.value().acquire_objects(
              std::move(discovered_res.value()));
            if (!obj_locks_res.has_value()) {
                co_return rpc::set_start_offset_reply{
                  .ec = obj_locks_res.error(),
                };
            }
        }
    }

    co_return co_await do_set_start_offset(locks_res.value(), req);
}

ss::future<
  std::expected<db_domain_manager::set_partitions_empty_result, rpc::errc>>
db_domain_manager::set_partitions_empty(
  entity_locks& locks, const model::topic_id& tid) {
    auto reader = state_reader(db_->db().create_snapshot());
    auto partitions_res = co_await reader.get_partitions_for_topic(tid);
    if (!partitions_res.has_value()) {
        co_return std::unexpected(log_and_convert(
          partitions_res.error(), "Error getting partitions for topic: "));
    }
    if (partitions_res.value().empty()) {
        co_return set_partitions_empty_result{.has_more = false};
    }

    // Discover all object IDs across all partitions' extents and acquire
    // them in one sorted batch, so do_set_start_offset doesn't need to
    // grow object locks per partition.
    {
        auto update = remove_topics_db_update{
          .topics = chunked_vector<model::topic_id>::single(tid),
        };
        auto discovered_res = co_await update.discover_object_ids(reader);
        if (!discovered_res.has_value()) {
            co_return std::unexpected(log_and_convert(
              discovered_res.error(),
              "Error discovering objects for topic emptying: "));
        }
        if (!discovered_res.value().empty()) {
            auto obj_locks_res = co_await locks.acquire_objects(
              std::move(discovered_res.value()));
            if (!obj_locks_res.has_value()) {
                co_return std::unexpected(obj_locks_res.error());
            }
        }
    }

    // Process one batch of work for the first non-empty partition, then
    // return so the caller can bound work per RPC.
    const auto num_partitions = partitions_res.value().size();
    for (size_t i = 0; i < num_partitions; ++i) {
        const auto& pid = partitions_res.value()[i];
        model::topic_id_partition tidp(tid, pid);

        auto meta_reader = state_reader(db_->db().create_snapshot());
        auto meta_res = co_await meta_reader.get_metadata(tidp);
        if (!meta_res.has_value()) {
            co_return std::unexpected(
              log_and_convert(meta_res.error(), "Error getting metadata: "));
        }
        if (!meta_res.value().has_value()) {
            continue;
        }
        const auto& metadata = (*meta_res).value();
        if (metadata.start_offset < metadata.next_offset) {
            auto set_offset_reply = co_await do_set_start_offset(
              locks,
              rpc::set_start_offset_request{
                .tp = tidp,
                .start_offset = metadata.next_offset,
              });
            if (set_offset_reply.ec != rpc::errc::ok) {
                co_return std::unexpected(set_offset_reply.ec);
            }
            co_return set_partitions_empty_result{
              .has_more = i < (num_partitions - 1)};
        }
    }

    co_return set_partitions_empty_result{.has_more = false};
}

ss::future<std::expected<void, rpc::errc>>
db_domain_manager::discover_objects_and_remove_topics(
  entity_locks& locks, chunked_vector<model::topic_id> topics) {
    auto discovery_update = remove_topics_db_update{
      .topics = topics.copy(),
    };
    auto discovery_reader = state_reader(db_->db().create_snapshot());
    auto discovered_res = co_await discovery_update.discover_object_ids(
      discovery_reader);
    if (!discovered_res.has_value()) {
        co_return std::unexpected(log_and_convert(
          discovered_res.error(),
          "Error discovering objects for topic removal: "));
    }
    if (!discovered_res.value().empty()) {
        auto obj_locks_res = co_await locks.acquire_objects(
          std::move(discovered_res.value()));
        if (!obj_locks_res.has_value()) {
            co_return std::unexpected(obj_locks_res.error());
        }
    }
    co_return co_await do_remove_topics(locks, std::move(topics));
}

ss::future<std::expected<void, rpc::errc>> db_domain_manager::do_remove_topics(
  const entity_locks& locks, chunked_vector<model::topic_id> topics) {
    auto update = remove_topics_db_update{
      .topics = std::move(topics),
    };
    auto reader = state_reader(db_->db().create_snapshot());
    chunked_vector<write_batch_row> rows;
    auto build_res = co_await update.build_rows(reader, rows);
    if (!build_res.has_value()) {
        co_return std::unexpected(log_and_convert(
          build_res.error(), "Rejecting request to remove topics: "));
    }
    if (rows.empty()) {
        co_return std::expected<void, rpc::errc>{};
    }
    co_return co_await write_rows(locks, std::move(rows));
}

ss::future<rpc::remove_topics_reply>
db_domain_manager::remove_topics(rpc::remove_topics_request req) {
    static constexpr size_t max_extents_per_batch = 1000;

    // TODO: instead of locking all topics upfront, acquire locks per-batch:
    // each big-topic or small-topic batch gets its own gate_and_open_writes
    // with topic write locks + discovered partition/object locks.
    absl::btree_set<model::topic_id> topic_write_set(
      req.topics.begin(), req.topics.end());
    auto locks_res = co_await gate_and_open_writes({
      .topic_write_locks = std::move(topic_write_set),
    });
    if (!locks_res.has_value()) {
        co_return rpc::remove_topics_reply{
          .ec = locks_res.error(),
          .not_removed = {},
        };
    }

    auto reader = state_reader(db_->db().create_snapshot());
    size_t extents_so_far = 0;
    chunked_vector<model::topic_id> to_delete_so_far;

    auto copy_req_topics_from = [&req](size_t from_idx) {
        chunked_vector<model::topic_id> not_removed;
        std::copy(
          req.topics.begin() + static_cast<long>(from_idx),
          req.topics.end(),
          std::back_inserter(not_removed));
        return not_removed;
    };
    for (size_t i = 0; i < req.topics.size(); ++i) {
        const auto& tid = req.topics.at(i);
        auto topic_extents_res = co_await count_topic_extents(
          reader, tid, max_extents_per_batch);
        if (!topic_extents_res.has_value()) {
            co_return rpc::remove_topics_reply{
              .ec = log_and_convert(
                topic_extents_res.error(),
                "Error counting extents for topic removal: "),
              .not_removed = {},
            };
        }
        auto topic_extents = topic_extents_res.value();
        if (
          to_delete_so_far.empty() && topic_extents >= max_extents_per_batch) {
            // This topic is big and it's the first one (we don't have any
            // other topics accumulated). Do one batch of partition emptying
            // and return, expecting callers to retry.
            auto empty_res = co_await set_partitions_empty(
              locks_res.value(), tid);
            if (!empty_res.has_value()) {
                co_return rpc::remove_topics_reply{
                  .ec = empty_res.error(),
                  .not_removed = {},
                };
            }
            if (empty_res.value().has_more) {
                // More emptying to do — return this topic (and the rest)
                // as not_removed so the caller retries.
                co_return rpc::remove_topics_reply{
                  .ec = rpc::errc::ok,
                  .not_removed = copy_req_topics_from(i),
                };
            }
            // Partitions are fully emptied. Remove the remaining metadata.
            auto rm_res = co_await do_remove_topics(
              locks_res.value(), chunked_vector<model::topic_id>::single(tid));
            if (!rm_res.has_value()) {
                co_return rpc::remove_topics_reply{
                  .ec = rm_res.error(),
                  .not_removed = {},
                };
            }
            co_return rpc::remove_topics_reply{
              .ec = rpc::errc::ok,
              .not_removed = copy_req_topics_from(i + 1),
            };
        }

        if (extents_so_far + topic_extents > max_extents_per_batch) {
            // This topic will put us over our extent limit. Just remove the
            // topics we've accumulated so far and not this one, expecting that
            // callers will retry.
            auto rm_res = co_await discover_objects_and_remove_topics(
              locks_res.value(), std::move(to_delete_so_far));
            if (!rm_res.has_value()) {
                co_return rpc::remove_topics_reply{
                  .ec = rm_res.error(),
                  .not_removed = {},
                };
            }
            co_return rpc::remove_topics_reply{
              .ec = rpc::errc::ok,
              .not_removed = copy_req_topics_from(i),
            };
        }

        to_delete_so_far.push_back(tid);
        extents_so_far += topic_extents;
    }

    // We've made it through all our topics without hitting the extent limit.
    // It should be safe to just delete them all.
    if (!to_delete_so_far.empty()) {
        auto rm_res = co_await discover_objects_and_remove_topics(
          locks_res.value(), std::move(to_delete_so_far));
        if (!rm_res.has_value()) {
            co_return rpc::remove_topics_reply{
              .ec = rm_res.error(),
              .not_removed = {},
            };
        }
    }
    co_return rpc::remove_topics_reply{
      .ec = rpc::errc::ok,
      .not_removed = {},
    };
}

ss::future<rpc::get_compaction_infos_reply>
db_domain_manager::get_compaction_infos(rpc::get_compaction_infos_request req) {
    auto gl_res = co_await gate_and_open_reads();
    if (!gl_res.has_value()) {
        co_return rpc::get_compaction_infos_reply{
          .ec = gl_res.error(),
        };
    }

    auto reader = state_reader(db_->db().create_snapshot());
    chunked_hash_map<model::topic_id_partition, rpc::get_compaction_info_reply>
      compaction_infos;
    for (auto& log_req : req.logs) {
        auto log_info = co_await do_get_compaction_info(
          gl_res.value(), reader, log_req);
        compaction_infos.insert_or_assign(log_req.tp, std::move(log_info));
    }

    co_return rpc::get_compaction_infos_reply{
      .responses = std::move(compaction_infos)};
}

ss::future<rpc::get_extent_metadata_reply>
db_domain_manager::get_extent_metadata(rpc::get_extent_metadata_request req) {
    auto gl_res = co_await gate_and_open_reads();
    if (!gl_res.has_value()) {
        co_return rpc::get_extent_metadata_reply{.ec = gl_res.error()};
    }
    auto reader = state_reader(db_->db().create_snapshot());

    // Get extents either forwards or backwards based on request order.
    auto extents_res = [&]() {
        switch (req.o) {
        case rpc::get_extent_metadata_request::order::forwards:
            return reader.get_inclusive_extents(
              req.tp, req.min_offset, req.max_offset);
        case rpc::get_extent_metadata_request::order::backwards:
            return reader.get_inclusive_extents_backward(
              req.tp, req.min_offset, req.max_offset);
        }
    }();
    auto extents_result = co_await std::move(extents_res);
    if (!extents_result.has_value()) {
        co_return rpc::get_extent_metadata_reply{
          .ec = log_and_convert(
            extents_result.error(), "Error getting extent range: "),
        };
    }
    if (!extents_result->has_value()) {
        co_return rpc::get_extent_metadata_reply{
          .ec = rpc::errc::ok,
          .extents = {},
          .end_of_stream = true,
        };
    }

    chunked_vector<rpc::extent_metadata> extents;
    bool end_of_stream = true;
    auto gen = (*extents_result)->get_rows();
    while (auto row_opt = co_await gen()) {
        const auto& row = row_opt->get();
        if (!row.has_value()) {
            co_return rpc::get_extent_metadata_reply{
              .ec = log_and_convert(
                row.error(), "Error iterating through extents: "),
            };
        }
        const auto& extent = row.value();
        auto key = extent_row_key::decode(extent.key);
        rpc::extent_metadata em{
          .base_offset = key->base_offset,
          .last_offset = extent.val.last_offset,
          .max_timestamp = extent.val.max_timestamp,
        };
        if (req.include_object_metadata) {
            auto object_res = co_await reader.get_object(extent.val.oid);
            if (!object_res.has_value()) {
                co_return rpc::get_extent_metadata_reply{
                  .ec = log_and_convert(
                    object_res.error(),
                    fmt::format(
                      "Error getting object {} in extent ({}~{}): ",
                      extent.val.oid,
                      key->base_offset,
                      extent.val.last_offset)),
                };
            }
            if (!object_res->has_value()) {
                co_return rpc::get_extent_metadata_reply{
                  .ec = rpc::errc::out_of_range,
                };
            }
            const auto& object = object_res.value().value();
            em.object_info = rpc::extent_object_info{
              .oid = extent.val.oid,
              .footer_pos = object.footer_pos,
              .object_size = object.object_size,
            };
        }
        extents.push_back(std::move(em));
        if (extents.size() >= req.max_num_extents) {
            end_of_stream = false;
            break;
        }
    }

    co_return rpc::get_extent_metadata_reply{
      .ec = rpc::errc::ok,
      .extents = std::move(extents),
      .end_of_stream = end_of_stream,
    };
}

ss::future<rpc::preregister_objects_reply>
db_domain_manager::preregister_objects(rpc::preregister_objects_request req) {
    preregister_objects_db_update update;
    update.registered_at = model::timestamp::now();
    update.object_ids.reserve(req.count);
    for (uint32_t i = 0; i < req.count; ++i) {
        update.object_ids.push_back(create_object_id());
    }

    absl::btree_set<object_id> oids(
      update.object_ids.begin(), update.object_ids.end());
    auto locks_res = co_await gate_and_open_writes({
      .object_locks = std::move(oids),
    });
    if (!locks_res.has_value()) {
        co_return rpc::preregister_objects_reply{
          .ec = locks_res.error(),
        };
    }

    auto reader = state_reader(db_->db().create_snapshot());
    chunked_vector<write_batch_row> rows;
    auto build_res = co_await update.build_rows(reader, rows);
    if (!build_res.has_value()) {
        co_return rpc::preregister_objects_reply{
          .ec = log_and_convert(
            build_res.error(), "Rejecting request to preregister objects: "),
        };
    }

    auto apply_res = co_await write_rows(locks_res.value(), std::move(rows));
    if (!apply_res.has_value()) {
        co_return rpc::preregister_objects_reply{
          .ec = apply_res.error(),
        };
    }

    probe_->objects_preregistered(req.count);
    co_return rpc::preregister_objects_reply{
      .ec = rpc::errc::ok,
      .object_ids = std::move(update.object_ids),
    };
}

ss::future<std::expected<ss::rwlock::holder, rpc::errc>>
db_domain_manager::exclusive_db_lock() {
    auto fut = co_await ss::coroutine::as_future(
      db_instance_lock_.hold_write_lock());
    if (fut.failed()) {
        auto ex = fut.get_exception();
        vlog(cd_log.debug, "Exception while getting database lock: {}", ex);
        co_return std::unexpected(rpc::errc::not_leader);
    }
    co_return std::move(fut.get());
}

ss::future<std::expected<db_domain_manager::gate_read_lock, rpc::errc>>
db_domain_manager::gate_and_open_reads() {
    auto gate_res = maybe_gate();
    if (!gate_res.has_value()) {
        // Shutting down.
        co_return std::unexpected(rpc::errc::not_leader);
    }
    auto init_db_res = co_await maybe_open_db();
    if (!init_db_res.has_value()) {
        co_return std::unexpected(init_db_res.error());
    }
    auto fut = co_await ss::coroutine::as_future(
      db_instance_lock_.hold_read_lock());
    if (fut.failed()) {
        // Shutting down.
        auto ex = fut.get_exception();
        vlog(cd_log.debug, "Exception while getting database lock: {}", ex);
        co_return std::unexpected(rpc::errc::not_leader);
    }
    if (!db_ || db_->needs_reopen()) {
        co_return std::unexpected(rpc::errc::not_leader);
    }
    co_return gate_read_lock{
      .gate = std::move(*gate_res),
      .db_lock = std::move(fut.get()),
    };
}

ss::future<std::expected<db_domain_manager::entity_locks, rpc::errc>>
db_domain_manager::gate_and_open_writes(write_lock_spec spec) {
    auto gl_res = co_await gate_and_open_reads();
    if (!gl_res.has_value()) {
        co_return std::unexpected(gl_res.error());
    }

    // Ordered acquisition: topic->partition->object.
    try {
        chunked_vector<entity_rwlock_map<model::topic_id>::tracked_units>
          topic_units;

        if (!spec.topic_read_locks.empty()) {
            auto read_units = co_await topic_locks_.acquire_read(
              spec.topic_read_locks);
            for (auto& u : read_units) {
                topic_units.push_back(std::move(u));
            }
        }
        if (!spec.topic_write_locks.empty()) {
            auto write_units = co_await topic_locks_.acquire_write(
              spec.topic_write_locks);
            for (auto& u : write_units) {
                topic_units.push_back(std::move(u));
            }
        }

        chunked_vector<
          entity_lock_map<model::topic_id_partition>::tracked_units>
          partition_units;
        if (!spec.partition_locks.empty()) {
            partition_units = co_await partition_locks_.acquire(
              spec.partition_locks);
        }

        chunked_vector<entity_lock_map<object_id>::tracked_units> object_units;
        if (!spec.object_locks.empty()) {
            object_units = co_await object_locks_.acquire(spec.object_locks);
        }

        co_return entity_locks{
          .read_lock = std::move(*gl_res),
          .topic_locks = std::move(topic_units),
          .partition_locks = std::move(partition_units),
          .object_locks = std::move(object_units),
          .partition_lock_map = &partition_locks_,
          .object_lock_map = &object_locks_,
        };
    } catch (...) {
        auto eptr = std::current_exception();
        auto lvl = ssx::is_shutdown_exception(eptr) ? ss::log_level::debug
                                                    : ss::log_level::warn;
        vlogl(cd_log, lvl, "Exception while acquiring entity locks: {}", eptr);
        co_return std::unexpected(rpc::errc::not_leader);
    }
}

ss::future<std::expected<void, rpc::errc>> db_domain_manager::write_rows(
  const entity_locks&, chunked_vector<write_batch_row> rows) {
    co_return co_await write_rows_no_lock(std::move(rows));
}

ss::future<std::expected<void, rpc::errc>>
db_domain_manager::write_rows_no_lock(chunked_vector<write_batch_row> rows) {
    // TODO: it's probably worth pushing some retries into replicated_database
    // while locks are still held, rather than stepping down immediately.
    auto apply_res = co_await db_->write(std::move(rows));
    if (apply_res.has_value()) {
        co_return std::expected<void, rpc::errc>{};
    }
    bool needs_step_down{false};
    switch (apply_res.error().e) {
        using enum replicated_database::errc;
    case replication_error:
    case io_error:
        needs_step_down = true;
        break;
    case update_rejected:
    case shutting_down:
    case not_leader:
        break;
    }
    if (needs_step_down) {
        auto step_down_fut = co_await ss::coroutine::as_future(
          stm_->raft()->step_down_in_term(
            expected_term_, "Failed to write to database"));
        if (step_down_fut.failed()) {
            // Only throws at shutdown.
            auto ex = step_down_fut.get_exception();
            vlog(cd_log.debug, "Exception while stepping down: {}", ex);
            co_return std::unexpected(rpc::errc::not_leader);
        }
    }
    co_return std::unexpected(
      log_and_convert(apply_res.error(), "Failed to write to database: "));
}

ss::future<std::expected<void, rpc::errc>> db_domain_manager::maybe_open_db() {
    if (db_ && !db_->needs_reopen()) {
        co_return std::expected<void, rpc::errc>{};
    }
    auto cur_term = stm_->raft()->term();
    if (cur_term != expected_term_) {
        vlog(
          cd_log.debug,
          "Not opening database, no longer term {}: {}",
          expected_term_,
          cur_term);
        co_return std::unexpected(rpc::errc::not_leader);
    }

    auto wlock_res = co_await exclusive_db_lock();
    if (!wlock_res.has_value()) {
        co_return std::unexpected(wlock_res.error());
    }
    if (db_) {
        if (!db_->needs_reopen()) {
            co_return std::expected<void, rpc::errc>{};
        }
        // TODO: background this with a gate hold?
        auto close_res = co_await db_->close();
        if (!close_res.has_value()) {
            co_return std::unexpected(
              log_and_convert(close_res.error(), "Failed to close database: "));
        }
        db_.reset();
        // Fallthrough to reopen.
    }
    vlog(
      cd_log.debug, "Opening database with expected term {}", expected_term_);
    auto db_res = co_await replicated_database::open(
      expected_term_, stm_.get(), staging_dir_, remote_, bucket_, as_, sg_);
    if (!db_res.has_value()) {
        co_return std::unexpected(
          log_and_convert(db_res.error(), "Failed to open database: "));
    }
    db_ = std::move(*db_res);
    co_return std::expected<void, rpc::errc>{};
}

ss::future<> db_domain_manager::gc_loop() {
    auto gate = maybe_gate();
    if (!gate.has_value()) {
        co_return;
    }

    auto ntp = stm_->raft()->log()->config().ntp();
    db_garbage_collector gc(object_io_, probe_);
    while (!as_.abort_requested()) {
        // NOTE: even though the garbage collector will remove objects and
        // actually write to the database, we don't need to take entity locks.
        // This is because there is no risk of logical row operations
        // colliding with object removal: the garbage collector will only ever
        // mutate unreferenced objects, and no other updates will update these
        // objects.
        auto gl_res = co_await gate_and_open_reads();
        if (!gl_res.has_value()) {
            break;
        }
        // TODO: make batch size configurable.
        vlog(cd_log.debug, "Running garbage collection now...");
        auto now = model::timestamp::now();
        auto ttl
          = config::shard_local_cfg().cloud_topics_preregistered_object_ttl();
        auto prereg_expiry_cutoff = model::timestamp{
          now() - static_cast<int64_t>(ttl.count())};
        auto deletion_delay = config::shard_local_cfg()
                                .cloud_topics_long_term_file_deletion_delay();
        auto deletion_delay_cutoff = model::timestamp{
          now() - static_cast<int64_t>(deletion_delay.count())};
        auto gc_res = co_await gc.remove_unreferenced_objects(
          db_.get(), &as_, 1000, prereg_expiry_cutoff, deletion_delay_cutoff);
        if (!gc_res.has_value()) {
            using enum db_garbage_collector::errc;
            switch (gc_res.error().e) {
            case db_needs_reopen:
                vlog(
                  cd_log.debug,
                  "Database needs reopen after GC: {}",
                  gc_res.error());
                break;
            case io_error:
                vlog(
                  cd_log.warn,
                  "IO error during garbage collection: {}",
                  gc_res.error());
                break;
            }
        }
        // Drop the database lock before expiring stale preregistered objects
        // and before sleeping, so we don't hold it unnecessarily.
        gl_res = {};
        if (gc_res.has_value() && !gc_res.value().empty()) {
            co_await expire_preregistered_objects(std::move(gc_res.value()));
        }

        auto sleep_interval = gc_interval_();
        vlog(
          cd_log.debug,
          "Re-running garbage collection in {}...",
          sleep_interval);
        try {
            co_await sem_.wait(
              sleep_interval, std::max(sem_.current(), size_t(1)));
        } catch (const ss::semaphore_timed_out&) {
            // Fall through
        } catch (...) {
            auto eptr = std::current_exception();
            auto log_lvl = ssx::is_shutdown_exception(eptr)
                             ? ss::log_level::debug
                             : ss::log_level::warn;
            vlogl(
              cd_log,
              log_lvl,
              "Garbage collection loop hit exception while sleeping: {}",
              eptr);
        }
    }
    vlog(cd_log.debug, "Garbage collection loop stopped...");
}

ss::future<rpc::restore_domain_reply>
db_domain_manager::restore_domain(rpc::restore_domain_request req) {
    vlog(cd_log.info, "Restoring domain UUID {}", req.new_uuid);
    auto gate_res = maybe_gate();
    if (!gate_res.has_value()) {
        co_return rpc::restore_domain_reply{
          .ec = rpc::errc::not_leader,
        };
    }
    auto init_db_res = co_await maybe_open_db();
    if (!init_db_res.has_value()) {
        co_return rpc::restore_domain_reply{
          .ec = init_db_res.error(),
        };
    }
    // No-op, we're already restored!
    if (db_ && db_->get_domain_uuid() == req.new_uuid) {
        co_return rpc::restore_domain_reply{
          .ec = rpc::errc::ok,
        };
    }

    // Block other users of the database while we're in the process of
    // resetting it.
    auto lock_res = co_await exclusive_db_lock();
    if (!lock_res.has_value()) {
        co_return rpc::restore_domain_reply{
          .ec = rpc::errc::not_leader,
        };
    }
    if (!db_ || db_->needs_reopen()) {
        co_return rpc::restore_domain_reply{
          .ec = rpc::errc::not_leader,
        };
    }
    // Check again for the no-op case a restore finished while we were waiting
    // for the lock.
    if (db_->get_domain_uuid() == req.new_uuid) {
        co_return rpc::restore_domain_reply{
          .ec = rpc::errc::ok,
        };
    }

    cloud_storage_clients::object_key domain_prefix{
      domain_cloud_prefix(req.new_uuid)};
    auto meta_persist = co_await lsm::io::open_cloud_metadata_persistence(
      remote_, bucket_, domain_prefix);

    // When reading the manifest this will find the latest manifest at or below
    // the given epoch. So to find the latest, supply the max epoch.
    auto manifest_fut = co_await ss::coroutine::as_future(
      meta_persist->read_manifest(lsm::internal::database_epoch::max()));
    std::optional<lsm::proto::manifest> manifest;
    if (manifest_fut.failed()) {
        auto ex = manifest_fut.get_exception();
        vlog(
          cd_log.error,
          "Failed to read domain manifest under path {}: {}",
          domain_prefix,
          ex);
        co_return rpc::restore_domain_reply{
          .ec = rpc::errc::concurrent_requests,
        };
    }
    auto manifest_result = manifest_fut.get();
    if (manifest_result.has_value()) {
        auto manifest_serde_fut = co_await ss::coroutine::as_future(
          lsm::proto::manifest::from_proto(std::move(manifest_result.value())));
        if (manifest_serde_fut.failed()) {
            auto ex = manifest_serde_fut.get_exception();
            vlog(
              cd_log.error,
              "Failed to deserialize domain manifest under path {}: {}",
              domain_prefix,
              ex);
            co_return rpc::restore_domain_reply{
              .ec = rpc::errc::concurrent_requests,
            };
        }
        manifest = std::move(manifest_serde_fut.get());
    }
    auto reset_res = co_await db_->reset(req.new_uuid, std::move(manifest));
    if (!reset_res.has_value()) {
        co_return rpc::restore_domain_reply{
          .ec = log_and_convert(reset_res.error(), "Failed to reset database"),
        };
    }
    co_await db_->close();
    db_.reset();
    vlog(
      cd_log.debug,
      "Re-opening database with expected term {}",
      expected_term_);
    auto db_res = co_await replicated_database::open(
      expected_term_, stm_.get(), staging_dir_, remote_, bucket_, as_, sg_);
    if (!db_res.has_value()) {
        co_return rpc::restore_domain_reply{
          .ec = log_and_convert(db_res.error(), "Failed to reopen database: "),
        };
    }
    db_ = std::move(*db_res);
    if (db_->get_domain_uuid() != req.new_uuid) {
        co_return rpc::restore_domain_reply{
          .ec = rpc::errc::concurrent_requests,
        };
    }
    co_return rpc::restore_domain_reply{
      .ec = rpc::errc::ok,
    };
}

ss::future<rpc::flush_domain_reply>
db_domain_manager::flush_domain(rpc::flush_domain_request req) {
    auto gl_res = co_await gate_and_open_reads();
    if (!gl_res.has_value()) {
        co_return rpc::flush_domain_reply{.ec = gl_res.error()};
    }
    auto flush_res = co_await db_->flush(30s);
    if (!flush_res.has_value()) {
        co_return rpc::flush_domain_reply{
          .ec = log_and_convert(flush_res.error(), "Failed to flush domain: "),
        };
    }
    co_return rpc::flush_domain_reply{
      .ec = rpc::errc::ok,
      .uuid = db_->get_domain_uuid(),
    };
}

ss::future<std::expected<database_stats, rpc::errc>>
db_domain_manager::get_database_stats() {
    auto gl_res = co_await gate_and_open_reads();
    if (!gl_res.has_value()) {
        co_return std::unexpected(gl_res.error());
    }

    auto stats = db_->db().get_data_stats();

    // Convert LSM stats to domain manager format
    database_stats result;
    result.active_memtable_bytes = stats.active_memtable_bytes;
    result.immutable_memtable_bytes = stats.immutable_memtable_bytes;
    result.total_size_bytes = stats.total_size_bytes;

    for (const auto& lsm_level : stats.levels) {
        lsm_level_info level;
        level.level_number = lsm_level.level_number;

        for (const auto& lsm_file : lsm_level.files) {
            lsm_file_info file;
            file.epoch = lsm_file.epoch;
            file.id = lsm_file.id;
            file.size_bytes = lsm_file.size_bytes;
            file.smallest_key_info = lsm_file.smallest_key_info;
            file.largest_key_info = lsm_file.largest_key_info;
            level.files.push_back(std::move(file));
        }

        result.levels.push_back(std::move(level));
    }

    co_return result;
}

ss::future<>
db_domain_manager::expire_preregistered_objects(chunked_vector<object_id> ids) {
    absl::btree_set<object_id> oids(ids.begin(), ids.end());
    auto locks_res = co_await gate_and_open_writes({
      .object_locks = std::move(oids),
    });
    if (!locks_res.has_value()) {
        vlog(
          cd_log.debug,
          "Not expiring preregistered objects, failed to acquire locks: {}",
          locks_res.error());
        co_return;
    }
    auto num_ids = ids.size();
    expire_preregistered_objects_db_update update{.object_ids = std::move(ids)};
    auto reader = state_reader(db_->db().create_snapshot());
    chunked_vector<write_batch_row> rows;
    auto build_res = co_await update.build_rows(reader, rows);
    if (!build_res.has_value()) {
        log_and_convert(
          build_res.error(),
          "Error building rows for preregistered object expiry: ");
        co_return;
    }
    if (rows.empty()) {
        co_return;
    }
    auto write_res = co_await write_rows(locks_res.value(), std::move(rows));
    if (!write_res.has_value()) {
        vlog(
          cd_log.warn,
          "Error writing preregistered object expiry rows: {}",
          write_res.error());
        co_return;
    }
    probe_->gc_objects_expired(num_ids);
}

ss::future<std::expected<void, rpc::errc>>
db_domain_manager::write_debug_rows(chunked_vector<write_batch_row> rows) {
    auto locks_res = co_await gate_and_open_writes({});
    if (!locks_res.has_value()) {
        co_return std::unexpected(locks_res.error());
    }
    co_return co_await write_rows(locks_res.value(), std::move(rows));
}

ss::future<std::expected<domain_manager::read_debug_rows_result, rpc::errc>>
db_domain_manager::read_debug_rows(
  std::optional<ss::sstring> seek_key,
  std::optional<ss::sstring> last_key,
  uint32_t max_rows) {
    auto gl_res = co_await gate_and_open_reads();
    if (!gl_res.has_value()) {
        co_return std::unexpected(gl_res.error());
    }
    try {
        auto iter = co_await db_->db().create_iterator();
        if (seek_key.has_value()) {
            co_await iter.seek(*seek_key);
        } else {
            co_await iter.seek_to_first();
        }

        chunked_vector<write_batch_row> rows;
        uint32_t count = 0;
        while (iter.valid() && count <= max_rows) {
            auto key = ss::sstring(iter.key());
            if (last_key.has_value() && key > *last_key) {
                break;
            }
            if (count == max_rows) {
                co_return read_debug_rows_result{
                  .rows = std::move(rows),
                  .next_key = std::move(key),
                };
            }
            rows.push_back(
              write_batch_row{
                .key = std::move(key),
                .value = iter.value(),
              });
            ++count;
            co_await iter.next();
        }
        co_return read_debug_rows_result{
          .rows = std::move(rows),
          .next_key = std::nullopt,
        };
    } catch (...) {
        auto ex = std::current_exception();
        if (ssx::is_shutdown_exception(ex)) {
            co_return std::unexpected(rpc::errc::not_leader);
        }
        vlog(cd_log.warn, "read_debug_rows exception: {}", ex);
        co_return std::unexpected(rpc::errc::timed_out);
    }
}

ss::future<
  std::expected<partition_validation_result, partition_validator::error>>
db_domain_manager::validate_partition(validate_partition_options opts) {
    auto gl_res = co_await gate_and_open_reads();
    if (!gl_res.has_value()) {
        co_return std::unexpected(
          partition_validator::error(
            partition_validator::errc::io_error, "failed to open database"));
    }
    if (opts.remote == nullptr) {
        opts.remote = remote_;
    }
    if (opts.bucket == nullptr) {
        opts.bucket = &bucket_;
    }
    if (opts.as == nullptr) {
        opts.as = &as_;
    }

    auto reader = state_reader(db_->db().create_snapshot());
    partition_validator validator(reader);
    co_return co_await validator.validate(opts);
}

} // namespace cloud_topics::l1

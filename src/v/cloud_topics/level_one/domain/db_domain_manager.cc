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
#include "cloud_topics/level_one/metastore/lsm/keys.h"
#include "cloud_topics/level_one/metastore/lsm/state_reader.h"
#include "cloud_topics/level_one/metastore/lsm/state_update.h"
#include "cloud_topics/level_one/metastore/rpc_types.h"
#include "cloud_topics/logger.h"
#include "container/chunked_hash_map.h"

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
    case replicated_database::errc::replication_error:
    case replicated_database::errc::not_leader:
        ret = rpc::errc::not_leader;
        lvl = ss::log_level::warn;
        break;
    }
    vlogl(cd_log, lvl, "{}{}", prefix, e);
    return ret;
}

} // namespace

db_domain_manager::db_domain_manager(
  model::term_id expected_term,
  ss::shared_ptr<stm> stm,
  std::filesystem::path staging_dir,
  cloud_io::remote* remote,
  cloud_storage_clients::bucket_name bucket)
  : expected_term_(expected_term)
  , staging_dir_(std::move(staging_dir))
  , remote_(remote)
  , bucket_(std::move(bucket))
  , stm_(std::move(stm)) {}

void db_domain_manager::start() {}

ss::future<> db_domain_manager::stop_and_wait() {
    vlog(cd_log.debug, "DB domain manager stopping...");
    as_.request_abort();
    auto gate_fut = gate_.close();
    writer_lock_.broken();
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

    co_await std::move(gate_fut);
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
    chunked_hash_set<object_id> added_oids;
    for (const auto& obj : req.new_objects) {
        added_oids.emplace(obj.oid);
    }

    chunked_hash_map<model::topic_id_partition, kafka::offset> corrections;
    auto update = add_objects_db_update{
      .new_objects = std::move(req.new_objects),
      .new_terms = std::move(req.new_terms),
    };
    auto gl_res = co_await gate_and_open_writes();
    if (!gl_res.has_value()) {
        co_return rpc::add_objects_reply{
          .ec = gl_res.error(),
        };
    }
    // Validate and build write batch rows
    auto reader = state_reader(db_->db().create_snapshot());
    chunked_vector<write_batch_row> rows;
    auto build_res = co_await update.build_rows(reader, rows, &corrections);
    if (!build_res.has_value()) {
        co_return rpc::add_objects_reply{
          .ec = log_and_convert(
            build_res.error(), "Rejecting request to add objects: "),
        };
    }

    auto apply_res = co_await write_rows(gl_res.value(), std::move(rows));
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
    chunked_hash_set<object_id> added_oids;
    for (const auto& obj : req.new_objects) {
        added_oids.emplace(obj.oid);
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
    auto gl_res = co_await gate_and_open_writes();
    if (!gl_res.has_value()) {
        co_return rpc::replace_objects_reply{
          .ec = gl_res.error(),
        };
    }
    auto reader = state_reader(db_->db().create_snapshot());
    chunked_vector<write_batch_row> rows;
    auto build_res = co_await update.build_rows(reader, rows);
    if (!build_res.has_value()) {
        co_return rpc::replace_objects_reply{
          .ec = log_and_convert(
            build_res.error(), "Rejecting request to replace objects: "),
        };
    }
    auto apply_res = co_await write_rows(gl_res.value(), std::move(rows));
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

ss::future<rpc::set_start_offset_reply>
db_domain_manager::set_start_offset(rpc::set_start_offset_request req) {
    auto gl_res = co_await gate_and_open_writes();
    if (!gl_res.has_value()) {
        co_return rpc::set_start_offset_reply{
          .ec = gl_res.error(),
        };
    }

    auto update = set_start_offset_db_update{
      .tp = req.tp,
      .new_start_offset = req.start_offset,
    };

    auto reader = state_reader(db_->db().create_snapshot());
    chunked_vector<write_batch_row> rows;
    auto build_res = co_await update.build_rows(reader, rows);
    if (!build_res.has_value()) {
        co_return rpc::set_start_offset_reply{
          .ec = log_and_convert(
            build_res.error(), "Rejecting request to set start offset: "),
        };
    }

    if (rows.empty()) {
        // No-op case: new_start_offset <= current start_offset.
        co_return rpc::set_start_offset_reply{
          .ec = rpc::errc::ok,
        };
    }

    auto apply_res = co_await write_rows(gl_res.value(), std::move(rows));
    if (!apply_res.has_value()) {
        co_return rpc::set_start_offset_reply{
          .ec = apply_res.error(),
        };
    }

    co_return rpc::set_start_offset_reply{
      .ec = rpc::errc::ok,
    };
}

ss::future<rpc::remove_topics_reply>
db_domain_manager::remove_topics(rpc::remove_topics_request req) {
    auto gl_res = co_await gate_and_open_writes();
    if (!gl_res.has_value()) {
        co_return rpc::remove_topics_reply{
          .ec = gl_res.error(),
          .not_removed = std::move(req.topics),
        };
    }

    auto update = remove_topics_db_update{
      .topics = std::move(req.topics),
    };

    auto reader = state_reader(db_->db().create_snapshot());
    chunked_vector<write_batch_row> rows;
    auto build_res = co_await update.build_rows(reader, rows);
    if (!build_res.has_value()) {
        co_return rpc::remove_topics_reply{
          .ec = log_and_convert(
            build_res.error(), "Rejecting request to remove topics: "),
          .not_removed = std::move(update.topics),
        };
    }

    if (rows.empty()) {
        // No-op case: no topics to remove or topics don't exist.
        co_return rpc::remove_topics_reply{
          .ec = rpc::errc::ok,
          .not_removed = {},
        };
    }

    auto apply_res = co_await write_rows(gl_res.value(), std::move(rows));
    if (!apply_res.has_value()) {
        co_return rpc::remove_topics_reply{
          .ec = apply_res.error(),
          .not_removed = std::move(update.topics),
        };
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
    if (!extents_result.has_value() || !extents_result->has_value()) {
        co_return rpc::get_extent_metadata_reply{
          .ec = rpc::errc::out_of_range,
        };
    }

    chunked_vector<rpc::extent_metadata> extents;
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
        extents.push_back(
          rpc::extent_metadata{
            .base_offset = key->base_offset,
            .last_offset = extent.val.last_offset,
            .max_timestamp = extent.val.max_timestamp,
          });
        if (extents.size() >= req.max_num_extents) {
            break;
        }
    }

    co_return rpc::get_extent_metadata_reply{
      .ec = rpc::errc::ok,
      .extents = std::move(extents),
    };
}

ss::future<std::expected<ss::rwlock::holder, rpc::errc>>
db_domain_manager::exclusive_db_lock() {
    auto fut = co_await ss::coroutine::as_future(
      db_instance_lock_.hold_write_lock());
    if (fut.failed()) {
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
        co_return std::unexpected(rpc::errc::not_leader);
    }
    co_return gate_read_lock{
      .gate = std::move(*gate_res),
      .db_lock = std::move(fut.get()),
    };
}

ss::future<std::expected<db_domain_manager::gate_writer_locks, rpc::errc>>
db_domain_manager::gate_and_open_writes() {
    auto gl_res = co_await gate_and_open_reads();
    if (!gl_res.has_value()) {
        co_return std::unexpected(gl_res.error());
    }
    auto fut = co_await ss::coroutine::as_future(writer_lock_.get_units());
    if (fut.failed()) {
        // Shutting down.
        co_return std::unexpected(rpc::errc::not_leader);
    }
    auto& gl = *gl_res;
    co_return gate_writer_locks{
      .gate_read_lock = std::move(gl),
      .writer_lock = std::move(fut.get()),
    };
}

ss::future<std::expected<void, rpc::errc>> db_domain_manager::write_rows(
  const gate_writer_locks&, chunked_vector<write_batch_row> rows) {
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
      expected_term_, stm_.get(), staging_dir_, remote_, bucket_, as_);
    if (!db_res.has_value()) {
        co_return std::unexpected(
          log_and_convert(db_res.error(), "Failed to open database: "));
    }
    db_ = std::move(*db_res);
    co_return std::expected<void, rpc::errc>{};
}

} // namespace cloud_topics::l1

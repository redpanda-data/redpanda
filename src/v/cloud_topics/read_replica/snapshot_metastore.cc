/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_topics/read_replica/snapshot_metastore.h"

#include "cloud_topics/level_one/metastore/lsm/keys.h"
#include "cloud_topics/logger.h"

#include <seastar/core/coroutine.hh>

namespace cloud_topics::read_replica {

namespace {

l1::metastore::errc
log_and_convert(const l1::state_reader::error& e, std::string_view prefix) {
    using enum l1::state_reader::errc;
    ss::log_level lvl{};
    switch (e.e) {
    case io_error:
        lvl = ss::log_level::warn;
        break;
    case corruption:
        lvl = ss::log_level::error;
        break;
    case shutting_down:
        lvl = ss::log_level::debug;
        break;
    }
    vlogl(cd_log, lvl, "{}{}", prefix, e);
    return l1::metastore::errc::transport_error;
}

} // namespace

snapshot_metastore::snapshot_metastore(
  ss::gate::holder gh, lsm::snapshot snapshot)
  : gate_holder_(std::move(gh))
  , reader_(std::move(snapshot)) {}

ss::future<std::expected<l1::metastore::object_response, l1::metastore::errc>>
snapshot_metastore::get_first_ge(
  const model::topic_id_partition& tidp, kafka::offset offset) {
    auto extent_result = co_await reader_.get_extent_ge(tidp, offset);
    if (!extent_result.has_value()) {
        co_return std::unexpected(
          log_and_convert(extent_result.error(), "Error getting extent: "));
    }
    if (!extent_result.value().has_value()) {
        co_return std::unexpected(errc::out_of_range);
    }

    const auto& extent = *extent_result.value();
    auto obj_result = co_await reader_.get_object(extent.oid);
    if (!obj_result.has_value()) {
        co_return std::unexpected(
          log_and_convert(obj_result.error(), "Error getting object: "));
    }
    if (!obj_result.value().has_value()) {
        co_return std::unexpected(errc::out_of_range);
    }
    const auto& obj = *obj_result.value();
    co_return object_response{
      .oid = extent.oid,
      .footer_pos = obj.footer_pos,
      .object_size = obj.object_size,
      .first_offset = extent.base_offset,
      .last_offset = extent.last_offset,
    };
}

ss::future<std::expected<l1::metastore::object_response, l1::metastore::errc>>
snapshot_metastore::get_first_ge(
  const model::topic_id_partition& tidp,
  kafka::offset offset,
  model::timestamp ts) {
    auto extents_result = co_await reader_.get_inclusive_extents(
      tidp, offset, std::nullopt);

    if (!extents_result.has_value()) {
        co_return std::unexpected(
          log_and_convert(extents_result.error(), "Error getting extents: "));
    }
    if (!extents_result.value().has_value()) {
        co_return std::unexpected(errc::out_of_range);
    }

    auto gen = extents_result.value()->get_rows();
    while (auto row_ref = co_await gen()) {
        const auto& row_result = row_ref->get();
        if (!row_result.has_value()) {
            co_return std::unexpected(
              log_and_convert(row_result.error(), "Error iterating extents: "));
        }
        const auto& row = row_result.value();
        const auto& extent = row.val;
        if (extent.max_timestamp >= ts) {
            auto obj_result = co_await reader_.get_object(extent.oid);
            if (!obj_result.has_value()) {
                co_return std::unexpected(log_and_convert(
                  obj_result.error(), "Error getting object: "));
            }
            if (!obj_result.value().has_value()) {
                co_return std::unexpected(errc::out_of_range);
            }
            const auto& obj = *obj_result.value();

            auto key = l1::extent_row_key::decode(row.key);
            if (!key.has_value()) {
                vlog(cd_log.error, "Failed to decode extent: {}", row.key);
                co_return std::unexpected(errc::transport_error);
            }

            co_return object_response{
              .oid = extent.oid,
              .footer_pos = obj.footer_pos,
              .object_size = obj.object_size,
              .first_offset = key->base_offset,
              .last_offset = extent.last_offset,
            };
        }
    }

    co_return std::unexpected(errc::out_of_range);
}

ss::future<std::expected<kafka::offset, l1::metastore::errc>>
snapshot_metastore::get_end_offset_for_term(
  const model::topic_id_partition& tidp, model::term_id term) {
    auto result = co_await reader_.get_term_end(tidp, term);
    if (!result.has_value()) {
        co_return std::unexpected(
          log_and_convert(result.error(), "Error getting term end: "));
    }
    if (!result.value().has_value()) {
        co_return std::unexpected(errc::out_of_range);
    }
    co_return *result.value();
}

ss::future<std::expected<model::term_id, l1::metastore::errc>>
snapshot_metastore::get_term_for_offset(
  const model::topic_id_partition& tidp, kafka::offset offset) {
    auto result = co_await reader_.get_term_le(tidp, offset);
    if (!result.has_value()) {
        co_return std::unexpected(
          log_and_convert(result.error(), "Error getting term: "));
    }
    if (!result.value().has_value()) {
        co_return std::unexpected(errc::out_of_range);
    }
    co_return result.value()->term_id;
}

ss::future<std::expected<l1::metastore::offsets_response, l1::metastore::errc>>
snapshot_metastore::get_offsets(const model::topic_id_partition& tidp) {
    auto result = co_await reader_.get_metadata(tidp);
    if (!result.has_value()) {
        co_return std::unexpected(
          log_and_convert(result.error(), "Error getting metadata: "));
    }
    if (!result.value().has_value()) {
        co_return std::unexpected(errc::missing_ntp);
    }
    const auto& metadata = *result.value();
    co_return offsets_response{
      .start_offset = metadata.start_offset,
      .next_offset = metadata.next_offset,
    };
}

ss::future<std::expected<l1::metastore::size_response, l1::metastore::errc>>
snapshot_metastore::get_size(const model::topic_id_partition& tidp) {
    auto result = co_await reader_.get_metadata(tidp);
    if (!result.has_value()) {
        co_return std::unexpected(
          log_and_convert(result.error(), "Error getting metadata: "));
    }
    if (!result.value().has_value()) {
        co_return std::unexpected(errc::missing_ntp);
    }
    co_return size_response{.size = result.value()->size};
}

ss::future<std::expected<
  std::unique_ptr<l1::metastore::object_metadata_builder>,
  l1::metastore::errc>>
snapshot_metastore::object_builder() {
    co_return std::unexpected(errc::invalid_request);
}

ss::future<std::expected<l1::metastore::add_response, l1::metastore::errc>>
snapshot_metastore::add_objects(
  const object_metadata_builder&, const term_offset_map_t&) {
    co_return std::unexpected(errc::invalid_request);
}

ss::future<std::expected<void, l1::metastore::errc>>
snapshot_metastore::replace_objects(const object_metadata_builder&) {
    co_return std::unexpected(errc::invalid_request);
}

ss::future<std::expected<void, l1::metastore::errc>>
snapshot_metastore::set_start_offset(
  const model::topic_id_partition&, kafka::offset) {
    co_return std::unexpected(errc::invalid_request);
}

ss::future<
  std::expected<l1::metastore::topic_removal_response, l1::metastore::errc>>
snapshot_metastore::remove_topics(const chunked_vector<model::topic_id>&) {
    co_return std::unexpected(errc::invalid_request);
}

ss::future<std::expected<kafka::offset, l1::metastore::errc>>
snapshot_metastore::get_first_offset_for_bytes(
  const model::topic_id_partition&, uint64_t) {
    co_return std::unexpected(errc::invalid_request);
}

ss::future<std::expected<void, l1::metastore::errc>>
snapshot_metastore::compact_objects(
  const object_metadata_builder&, const compaction_map_t&) {
    co_return std::unexpected(errc::invalid_request);
}

ss::future<
  std::expected<l1::metastore::compaction_info_response, l1::metastore::errc>>
snapshot_metastore::get_compaction_info(const compaction_info_spec&) {
    co_return std::unexpected(errc::invalid_request);
}

ss::future<
  std::expected<l1::metastore::compaction_info_map, l1::metastore::errc>>
snapshot_metastore::get_compaction_infos(
  const chunked_vector<compaction_info_spec>&) {
    co_return std::unexpected(errc::invalid_request);
}

ss::future<
  std::expected<l1::metastore::extent_metadata_response, l1::metastore::errc>>
snapshot_metastore::get_extent_metadata_forwards(
  const model::topic_id_partition& tidp,
  kafka::offset min_offset,
  kafka::offset max_offset,
  size_t max_num_extents,
  include_object_metadata include_obj_meta) {
    auto extents_result = co_await reader_.get_inclusive_extents(
      tidp, min_offset, max_offset);
    if (!extents_result.has_value()) {
        co_return std::unexpected(log_and_convert(
          extents_result.error(), "Error getting extents forwards: "));
    }
    if (!extents_result.value().has_value()) {
        co_return std::unexpected(errc::out_of_range);
    }

    extent_metadata_vec extents;
    bool end_of_stream = true;
    auto gen = extents_result.value()->get_rows();
    while (auto row_ref = co_await gen()) {
        const auto& row_result = row_ref->get();
        if (!row_result.has_value()) {
            co_return std::unexpected(log_and_convert(
              row_result.error(), "Error iterating extents forwards: "));
        }
        auto key = l1::extent_row_key::decode(row_result.value().key);
        if (!key.has_value()) {
            vlog(
              cd_log.error,
              "Failed to decode extent: {}",
              row_result.value().key);
            co_return std::unexpected(errc::transport_error);
        }
        const auto& val = row_result.value().val;
        extent_metadata em{
          .base_offset = key->base_offset,
          .last_offset = val.last_offset,
          .max_timestamp = val.max_timestamp,
        };
        if (include_obj_meta) {
            auto obj_result = co_await reader_.get_object(val.oid);
            if (!obj_result.has_value()) {
                co_return std::unexpected(log_and_convert(
                  obj_result.error(), "Error getting object metadata: "));
            }
            if (!obj_result.value().has_value()) {
                vlog(
                  cd_log.error, "Object {} metadata is missing: {}", val.oid);
                co_return std::unexpected(errc::transport_error);
            }
            const auto& obj = *obj_result.value();
            em.object_info = extent_object_info{
              .oid = val.oid,
              .footer_pos = obj.footer_pos,
              .object_size = obj.object_size,
            };
        }
        extents.push_back(std::move(em));
        if (extents.size() >= max_num_extents) {
            end_of_stream = false;
            break;
        }
    }
    co_return extent_metadata_response{
      .extents = std::move(extents), .end_of_stream = end_of_stream};
}

ss::future<
  std::expected<l1::metastore::extent_metadata_response, l1::metastore::errc>>
snapshot_metastore::get_extent_metadata_backwards(
  const model::topic_id_partition&, kafka::offset, kafka::offset, size_t) {
    co_return std::unexpected(errc::invalid_request);
}

ss::future<std::expected<std::nullopt_t, l1::metastore::errc>>
snapshot_metastore::flush() {
    co_return std::unexpected(errc::invalid_request);
}

ss::future<std::expected<std::nullopt_t, l1::metastore::errc>>
snapshot_metastore::restore(const cloud_storage::remote_label&) {
    co_return std::unexpected(errc::invalid_request);
}

} // namespace cloud_topics::read_replica

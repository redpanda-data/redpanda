/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#include "cloud_topics/level_one/metastore/lsm/debug_serde.h"

#include "cloud_topics/level_one/metastore/lsm/keys.h"
#include "cloud_topics/level_one/metastore/lsm/values.h"
#include "cloud_topics/level_one/metastore/state.h"
#include "serde/rw/rw.h"
#include "utils/uuid.h"

namespace cloud_topics::l1 {

// Ensure debug serde stays in sync when fields are added to row value types.
namespace pb = proto::admin::metastore;
template<typename T>
constexpr size_t serde_field_count() {
    return std::tuple_size_v<decltype(std::declval<T>().serde_fields())>;
}
static_assert(
  serde_field_count<metadata_row_value>() == pb::metadata_value::field_count);
static_assert(
  serde_field_count<extent_row_value>() == pb::extent_value::field_count);
static_assert(
  serde_field_count<term_row_value>() == pb::term_value::field_count);
static_assert(
  serde_field_count<compaction_state>() == pb::compaction_value::field_count);
static_assert(
  serde_field_count<compaction_state::cleaned_range_with_tombstones>()
  == pb::cleaned_range_with_tombstones::field_count);
static_assert(
  serde_field_count<object_entry>() == pb::object_value::field_count);

std::string_view to_string_view(debug_serde_errc e) {
    switch (e) {
    case debug_serde_errc::invalid_uuid:
        return "invalid_uuid";
    case debug_serde_errc::missing_key:
        return "missing_key";
    case debug_serde_errc::missing_value:
        return "missing_value";
    case debug_serde_errc::unknown_row_type:
        return "unknown_row_type";
    case debug_serde_errc::decode_failure:
        return "decode_failure";
    }
    return "unknown";
}

namespace {

std::optional<uuid_t> parse_uuid(const std::string& s) {
    try {
        return uuid_t::from_string(s);
    } catch (...) {
        return std::nullopt;
    }
}

std::optional<model::topic_id_partition>
parse_tidp(const std::string& topic_id_str, int32_t partition_id) {
    auto uuid = parse_uuid(topic_id_str);
    if (!uuid) {
        return std::nullopt;
    }
    return model::topic_id_partition(
      model::topic_id(*uuid), model::partition_id(partition_id));
}

} // namespace

using error = debug_serde_error;
using errc = debug_serde_errc;

std::expected<ss::sstring, error>
debug_encode_key(const proto::admin::metastore::row_key& key) {
    if (key.has_metadata()) {
        auto& k = key.get_metadata();
        auto tidp = parse_tidp(k.get_topic_id(), k.get_partition_id());
        if (!tidp) {
            return std::unexpected(
              error(errc::invalid_uuid, "metadata key: {}", k.get_topic_id()));
        }
        return metadata_row_key::encode(*tidp);
    }
    if (key.has_extent()) {
        auto& k = key.get_extent();
        auto tidp = parse_tidp(k.get_topic_id(), k.get_partition_id());
        if (!tidp) {
            return std::unexpected(
              error(errc::invalid_uuid, "extent key: {}", k.get_topic_id()));
        }
        return extent_row_key::encode(
          *tidp, kafka::offset{k.get_base_offset()});
    }
    if (key.has_term()) {
        auto& k = key.get_term();
        auto tidp = parse_tidp(k.get_topic_id(), k.get_partition_id());
        if (!tidp) {
            return std::unexpected(
              error(errc::invalid_uuid, "term key: {}", k.get_topic_id()));
        }
        return term_row_key::encode(*tidp, model::term_id{k.get_term_id()});
    }
    if (key.has_compaction()) {
        auto& k = key.get_compaction();
        auto tidp = parse_tidp(k.get_topic_id(), k.get_partition_id());
        if (!tidp) {
            return std::unexpected(error(
              errc::invalid_uuid, "compaction key: {}", k.get_topic_id()));
        }
        return compaction_row_key::encode(*tidp);
    }
    if (key.has_object()) {
        auto& k = key.get_object();
        auto uuid = parse_uuid(k.get_object_id());
        if (!uuid) {
            return std::unexpected(
              error(errc::invalid_uuid, "object key: {}", k.get_object_id()));
        }
        return object_row_key::encode(object_id{*uuid});
    }
    return std::unexpected(error(errc::missing_key, "no key oneof set"));
}

std::expected<iobuf, error>
debug_encode_value(const proto::admin::metastore::row_value& val) {
    if (val.has_metadata()) {
        auto& v = val.get_metadata();
        metadata_row_value rv{
          .start_offset = kafka::offset{v.get_start_offset()},
          .next_offset = kafka::offset{v.get_next_offset()},
          .compaction_epoch
          = partition_state::compaction_epoch_t{v.get_compaction_epoch()},
          .size = static_cast<size_t>(v.get_size()),
          .num_extents = static_cast<size_t>(v.get_num_extents()),
        };
        return serde::to_iobuf(std::move(rv));
    }
    if (val.has_extent()) {
        auto& v = val.get_extent();
        auto uuid = parse_uuid(v.get_object_id());
        if (!uuid) {
            return std::unexpected(error(
              errc::invalid_uuid,
              "extent value object_id: {}",
              v.get_object_id()));
        }
        extent_row_value rv{
          .last_offset = kafka::offset{v.get_last_offset()},
          .max_timestamp = model::timestamp{v.get_max_timestamp()},
          .filepos = static_cast<size_t>(v.get_filepos()),
          .len = static_cast<size_t>(v.get_len()),
          .oid = object_id{*uuid},
        };
        return serde::to_iobuf(std::move(rv));
    }
    if (val.has_term()) {
        auto& v = val.get_term();
        term_row_value rv{
          .term_start_offset = kafka::offset{v.get_term_start_offset()},
        };
        return serde::to_iobuf(std::move(rv));
    }
    if (val.has_compaction()) {
        auto& v = val.get_compaction();
        compaction_state cs;
        for (const auto& r : v.get_cleaned_ranges()) {
            cs.cleaned_ranges.insert(
              kafka::offset{r.get_base_offset()},
              kafka::offset{r.get_last_offset()});
        }
        for (const auto& r : v.get_cleaned_ranges_with_tombstones()) {
            cs.cleaned_ranges_with_tombstones.insert(
              compaction_state::cleaned_range_with_tombstones{
                .base_offset = kafka::offset{r.get_base_offset()},
                .last_offset = kafka::offset{r.get_last_offset()},
                .cleaned_with_tombstones_at
                = model::timestamp{r.get_cleaned_with_tombstones_at()},
              });
        }
        compaction_row_value rv{
          .state = std::move(cs),
        };
        return serde::to_iobuf(std::move(rv));
    }
    if (val.has_object()) {
        auto& v = val.get_object();
        object_entry entry{
          .total_data_size = static_cast<size_t>(v.get_total_data_size()),
          .removed_data_size = static_cast<size_t>(v.get_removed_data_size()),
          .footer_pos = static_cast<size_t>(v.get_footer_pos()),
          .object_size = static_cast<size_t>(v.get_object_size()),
          .last_updated = model::timestamp{v.get_last_updated()},
          .is_preregistration = v.get_is_preregistration(),
        };
        object_row_value rv{
          .object = std::move(entry),
        };
        return serde::to_iobuf(std::move(rv));
    }
    return std::unexpected(error(errc::missing_value, "no value oneof set"));
}

} // namespace cloud_topics::l1

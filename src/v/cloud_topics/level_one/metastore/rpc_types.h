/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#pragma once

#include "cloud_topics/level_one/common/object_id.h"
#include "cloud_topics/level_one/metastore/domain_uuid.h"
#include "cloud_topics/level_one/metastore/offset_interval_set.h"
#include "cloud_topics/level_one/metastore/state_update.h"
#include "model/fundamental.h"
#include "serde/envelope.h"
#include "serde/rw/enum.h"
#include "serde/rw/envelope.h"
#include "serde/rw/optional.h"

#include <fmt/format.h>

#include <optional>

namespace cloud_topics::l1::rpc {

enum class errc : int16_t {
    ok = 0,
    incorrect_partition,
    timed_out,
    not_leader,
    concurrent_requests,
    missing_ntp,
    out_of_range,
};

struct add_objects_reply
  : serde::
      envelope<add_objects_reply, serde::version<0>, serde::compat_version<0>> {
    auto serde_fields() { return std::tie(ec, corrected_next_offsets); }

    errc ec;

    // Corrected next offsets for subsequent add_objects_requests to try.
    // Expected to only be set on success.
    chunked_hash_map<model::topic_id_partition, kafka::offset>
      corrected_next_offsets;
};
struct add_objects_request
  : serde::envelope<
      add_objects_request,
      serde::version<0>,
      serde::compat_version<0>> {
    using resp_t = add_objects_reply;
    auto serde_fields() {
        return std::tie(metastore_partition, new_objects, new_terms);
    }

    model::partition_id metastore_partition;
    chunked_vector<new_object> new_objects;
    term_state_update_t new_terms;
};

struct replace_objects_reply
  : serde::envelope<
      replace_objects_reply,
      serde::version<0>,
      serde::compat_version<0>> {
    auto serde_fields() { return std::tie(ec); }

    errc ec;
};
struct replace_objects_request
  : serde::envelope<
      replace_objects_request,
      serde::version<0>,
      serde::compat_version<0>> {
    using resp_t = replace_objects_reply;
    auto serde_fields() {
        return std::tie(metastore_partition, new_objects, compaction_updates);
    }

    model::partition_id metastore_partition;
    chunked_vector<new_object> new_objects;
    chunked_hash_map<model::topic_id_partition, compaction_state_update>
      compaction_updates;
};

struct object_metadata
  : serde::
      envelope<object_metadata, serde::version<0>, serde::compat_version<0>> {
    auto serde_fields() {
        return std::tie(
          oid, footer_pos, object_size, first_offset, last_offset);
    }

    object_id oid;
    size_t footer_pos;
    size_t object_size;

    // The first offset (inclusive) that is within this object.
    kafka::offset first_offset;
    // The last offset (inclusive) that is within this object.
    kafka::offset last_offset;
};

struct get_first_offset_ge_reply
  : serde::envelope<
      get_first_offset_ge_reply,
      serde::version<0>,
      serde::compat_version<0>> {
    auto serde_fields() { return std::tie(ec, object); }

    errc ec;
    object_metadata object;
};
struct get_first_offset_ge_request
  : serde::envelope<
      get_first_offset_ge_request,
      serde::version<0>,
      serde::compat_version<0>> {
    using resp_t = get_first_offset_ge_reply;
    auto serde_fields() { return std::tie(tp, o); }

    model::topic_id_partition tp;
    kafka::offset o;
};

struct get_first_timestamp_ge_reply
  : serde::envelope<
      get_first_timestamp_ge_reply,
      serde::version<0>,
      serde::compat_version<0>> {
    auto serde_fields() { return std::tie(ec, object); }

    errc ec;
    object_metadata object;
};
struct get_first_timestamp_ge_request
  : serde::envelope<
      get_first_timestamp_ge_request,
      serde::version<0>,
      serde::compat_version<0>> {
    using resp_t = get_first_timestamp_ge_reply;
    auto serde_fields() { return std::tie(tp, o, ts); }

    model::topic_id_partition tp;
    kafka::offset o;
    model::timestamp ts;
};

struct get_first_offset_for_bytes_reply
  : serde::envelope<
      get_first_offset_for_bytes_reply,
      serde::version<0>,
      serde::compat_version<0>> {
    auto serde_fields() { return std::tie(offset, ec); }

    kafka::offset offset;
    errc ec{};
};

struct get_first_offset_for_bytes_request
  : serde::envelope<
      get_first_offset_for_bytes_request,
      serde::version<0>,
      serde::compat_version<0>> {
    using resp_t = get_first_offset_for_bytes_reply;
    auto serde_fields() { return std::tie(tp, size); }

    model::topic_id_partition tp;
    uint64_t size{};
};

struct get_offsets_reply
  : serde::
      envelope<get_offsets_reply, serde::version<0>, serde::compat_version<0>> {
    auto serde_fields() { return std::tie(ec, start_offset, next_offset); }

    errc ec;
    kafka::offset start_offset;
    kafka::offset next_offset;
};
struct get_offsets_request
  : serde::envelope<
      get_offsets_request,
      serde::version<0>,
      serde::compat_version<0>> {
    using resp_t = get_offsets_reply;
    auto serde_fields() { return std::tie(tp); }

    model::topic_id_partition tp;
};

struct get_size_reply
  : serde::
      envelope<get_size_reply, serde::version<1>, serde::compat_version<0>> {
    auto serde_fields() { return std::tie(ec, size, num_extents); }

    errc ec;
    size_t size{0};
    size_t num_extents{0};
};
struct get_size_request
  : serde::
      envelope<get_size_request, serde::version<0>, serde::compat_version<0>> {
    using resp_t = get_size_reply;
    auto serde_fields() { return std::tie(tp); }

    model::topic_id_partition tp;
};

struct extent_object_info
  : serde::envelope<
      extent_object_info,
      serde::version<0>,
      serde::compat_version<0>> {
    auto serde_fields() { return std::tie(oid, footer_pos, object_size); }

    object_id oid;
    size_t footer_pos{0};
    size_t object_size{0};
};

struct extent_metadata
  : serde::
      envelope<extent_metadata, serde::version<0>, serde::compat_version<0>> {
    auto serde_fields() {
        return std::tie(base_offset, last_offset, max_timestamp, object_info);
    }

    kafka::offset base_offset;
    kafka::offset last_offset;
    model::timestamp max_timestamp;
    // Only populated when include_object_metadata is set on the request.
    std::optional<extent_object_info> object_info;
};

struct get_compaction_info_reply
  : serde::envelope<
      get_compaction_info_reply,
      serde::version<1>,
      serde::compat_version<0>> {
    auto serde_fields() {
        return std::tie(
          ec,
          dirty_ranges,
          removable_tombstone_ranges,
          dirty_ratio,
          earliest_dirty_ts,
          compaction_epoch,
          start_offset);
    }

    errc ec;
    offset_interval_set dirty_ranges;
    offset_interval_set removable_tombstone_ranges;
    double dirty_ratio;
    std::optional<model::timestamp> earliest_dirty_ts;
    partition_state::compaction_epoch_t compaction_epoch;
    kafka::offset start_offset;
};
struct get_compaction_info_request
  : serde::envelope<
      get_compaction_info_request,
      serde::version<0>,
      serde::compat_version<0>> {
    using resp_t = get_compaction_info_reply;
    auto serde_fields() {
        return std::tie(tp, tombstone_removal_upper_bound_ts);
    }

    model::topic_id_partition tp;

    // Cleaned ranges with tombstones that were cleaned at or below this
    // timestamp are eligible to have tombstones entirely removed. These ranges
    // will be returned in the removable_tombstone_ranges field.
    model::timestamp tombstone_removal_upper_bound_ts;
};

struct get_term_for_offset_reply
  : serde::envelope<
      get_term_for_offset_reply,
      serde::version<0>,
      serde::compat_version<0>> {
    auto serde_fields() { return std::tie(ec, term); }

    errc ec;
    model::term_id term;
};
struct get_term_for_offset_request
  : serde::envelope<
      get_term_for_offset_request,
      serde::version<0>,
      serde::compat_version<0>> {
    using resp_t = get_term_for_offset_reply;
    auto serde_fields() { return std::tie(tp, offset); }

    model::topic_id_partition tp;
    kafka::offset offset;
};

struct get_end_offset_for_term_reply
  : serde::envelope<
      get_end_offset_for_term_reply,
      serde::version<0>,
      serde::compat_version<0>> {
    auto serde_fields() { return std::tie(ec, end_offset); }

    errc ec;
    kafka::offset end_offset;
};
struct get_end_offset_for_term_request
  : serde::envelope<
      get_end_offset_for_term_request,
      serde::version<0>,
      serde::compat_version<0>> {
    using resp_t = get_end_offset_for_term_reply;
    auto serde_fields() { return std::tie(tp, term); }

    model::topic_id_partition tp;
    model::term_id term;
};

struct set_start_offset_reply
  : serde::envelope<
      set_start_offset_reply,
      serde::version<1>,
      serde::compat_version<0>> {
    auto serde_fields() { return std::tie(ec, has_more); }

    errc ec;
    bool has_more{false};
};
struct set_start_offset_request
  : serde::envelope<
      set_start_offset_request,
      serde::version<0>,
      serde::compat_version<0>> {
    using resp_t = set_start_offset_reply;
    auto serde_fields() { return std::tie(tp, start_offset); }

    model::topic_id_partition tp;
    kafka::offset start_offset;
};

struct remove_topics_reply
  : serde::envelope<
      remove_topics_reply,
      serde::version<0>,
      serde::compat_version<0>> {
    auto serde_fields() { return std::tie(ec, not_removed); }

    errc ec;
    chunked_vector<model::topic_id> not_removed;
};
struct remove_topics_request
  : serde::envelope<
      remove_topics_request,
      serde::version<0>,
      serde::compat_version<0>> {
    using resp_t = remove_topics_reply;
    auto serde_fields() { return std::tie(metastore_partition, topics); }

    model::partition_id metastore_partition;
    chunked_vector<model::topic_id> topics;
};

struct get_compaction_infos_reply
  : serde::envelope<
      get_compaction_infos_reply,
      serde::version<0>,
      serde::compat_version<0>> {
    auto serde_fields() { return std::tie(ec, responses); }

    errc ec;

    chunked_hash_map<model::topic_id_partition, get_compaction_info_reply>
      responses;
};
struct get_compaction_infos_request
  : serde::envelope<
      get_compaction_infos_request,
      serde::version<0>,
      serde::compat_version<0>> {
    using resp_t = get_compaction_infos_reply;
    auto serde_fields() { return std::tie(metastore_partition, logs); }

    model::partition_id metastore_partition;
    chunked_vector<get_compaction_info_request> logs;
};

struct get_extent_metadata_reply
  : serde::envelope<
      get_extent_metadata_reply,
      serde::version<0>,
      serde::compat_version<0>> {
    auto serde_fields() { return std::tie(ec, extents, end_of_stream); }

    errc ec;
    chunked_vector<extent_metadata> extents;
    // True when no more extents exist beyond this response (end of iteration).
    // False when more extents may exist (hit max_num_extents limit).
    bool end_of_stream{true};
};
struct get_extent_metadata_request
  : serde::envelope<
      get_extent_metadata_request,
      serde::version<0>,
      serde::compat_version<0>> {
    using resp_t = get_extent_metadata_reply;

    enum class order { forwards, backwards };

    auto serde_fields() {
        return std::tie(
          tp,
          min_offset,
          max_offset,
          o,
          max_num_extents,
          include_object_metadata);
    }

    model::topic_id_partition tp;
    kafka::offset min_offset;
    kafka::offset max_offset;
    order o;
    size_t max_num_extents;
    bool include_object_metadata{false};
};

struct restore_domain_reply
  : serde::envelope<
      restore_domain_reply,
      serde::version<0>,
      serde::compat_version<0>> {
    auto serde_fields() { return std::tie(ec); }
    errc ec;
};
struct restore_domain_request
  : serde::envelope<
      restore_domain_request,
      serde::version<1>,
      serde::compat_version<0>> {
    using resp_t = restore_domain_reply;
    auto serde_fields() { return std::tie(metastore_partition, new_uuid); }
    model::partition_id metastore_partition;
    domain_uuid new_uuid;
};

struct flush_domain_reply
  : serde::envelope<
      flush_domain_reply,
      serde::version<0>,
      serde::compat_version<0>> {
    auto serde_fields() { return std::tie(ec, uuid); }
    errc ec;
    domain_uuid uuid;
};
struct flush_domain_request
  : serde::envelope<
      flush_domain_request,
      serde::version<0>,
      serde::compat_version<0>> {
    using resp_t = flush_domain_reply;
    auto serde_fields() { return std::tie(metastore_partition); }
    model::partition_id metastore_partition;
};

struct preregister_objects_reply
  : serde::envelope<
      preregister_objects_reply,
      serde::version<0>,
      serde::compat_version<0>> {
    auto serde_fields() { return std::tie(ec, object_ids); }
    errc ec{errc::ok};
    chunked_vector<object_id> object_ids;
};

struct preregister_objects_request
  : serde::envelope<
      preregister_objects_request,
      serde::version<0>,
      serde::compat_version<0>> {
    using resp_t = preregister_objects_reply;
    auto serde_fields() { return std::tie(metastore_partition, count); }
    model::partition_id metastore_partition;
    uint32_t count{0};
};

} //  namespace cloud_topics::l1::rpc

template<>
struct fmt::formatter<cloud_topics::l1::rpc::errc> final
  : fmt::formatter<std::string_view> {
    using errc = cloud_topics::l1::rpc::errc;
    template<typename FormatContext>
    auto format(const errc& ec, FormatContext& ctx) const {
        switch (ec) {
        case errc::ok:
            return fmt::format_to(ctx.out(), "rpc::errc::ok");
        case errc::incorrect_partition:
            return fmt::format_to(ctx.out(), "rpc::errc::incorrect_partition");
        case errc::timed_out:
            return fmt::format_to(ctx.out(), "rpc::errc::timed_out");
        case errc::not_leader:
            return fmt::format_to(ctx.out(), "rpc::errc::not_leader");
        case errc::concurrent_requests:
            return fmt::format_to(ctx.out(), "rpc::errc::concurrent_requests");
        case errc::missing_ntp:
            return fmt::format_to(ctx.out(), "rpc::errc::missing_ntp");
        case errc::out_of_range:
            return fmt::format_to(ctx.out(), "rpc::errc::out_of_range");
        }
        return fmt::format_to(
          ctx.out(), "rpc::errc::unknown({})", static_cast<int>(ec));
    }
};

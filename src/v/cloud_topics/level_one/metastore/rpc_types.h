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
#include "cloud_topics/level_one/metastore/offset_interval_set.h"
#include "cloud_topics/level_one/metastore/state_update.h"
#include "model/fundamental.h"
#include "serde/envelope.h"
#include "serde/rw/enum.h"
#include "serde/rw/envelope.h"

namespace experimental::cloud_topics::l1::rpc {

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
    auto serde_fields() { return std::tie(metastore_partition, new_objects); }

    model::partition_id metastore_partition;
    chunked_vector<new_object> new_objects;
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
    auto serde_fields() { return std::tie(oid, footer_pos); }

    object_id oid;
    size_t footer_pos;
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
    auto serde_fields() { return std::tie(tp, ts); }

    model::topic_id_partition tp;
    model::timestamp ts;
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

struct get_compaction_offsets_reply
  : serde::envelope<
      get_compaction_offsets_reply,
      serde::version<0>,
      serde::compat_version<0>> {
    auto serde_fields() {
        return std::tie(ec, dirty_ranges, removable_tombstone_ranges);
    }

    errc ec;
    offset_interval_set dirty_ranges;
    offset_interval_set removable_tombstone_ranges;
};
struct get_compaction_offsets_request
  : serde::envelope<
      get_compaction_offsets_request,
      serde::version<0>,
      serde::compat_version<0>> {
    using resp_t = get_compaction_offsets_reply;
    auto serde_fields() {
        return std::tie(tp, tombstone_removal_upper_bound_ts);
    }

    model::topic_id_partition tp;

    // Cleaned ranges with tombstones that were cleaned at or below this
    // timestamp are eligible to have tombstones entirely removed. These ranges
    // will be returned in the removable_tombstone_ranges field.
    model::timestamp tombstone_removal_upper_bound_ts;
};

} //  namespace experimental::cloud_topics::l1::rpc

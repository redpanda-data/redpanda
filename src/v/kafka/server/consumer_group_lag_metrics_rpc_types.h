/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once
#include "container/chunked_hash_map.h"
#include "model/fundamental.h"
#include "serde/envelope.h"
#include "serde/rw/envelope.h"
#include "serde/rw/map.h"
#include "serde/rw/set.h"

namespace kafka {

struct partition_offsets_request
  : serde::envelope<
      partition_offsets_request,
      serde::version<0>,
      serde::compat_version<0>> {
    using partitions
      = chunked_hash_map<model::topic, chunked_hash_set<model::partition_id>>;
    partitions data;

    auto serde_fields() { return std::tie(data); }
};

struct partition_offsets_reply
  : serde::envelope<
      partition_offsets_reply,
      serde::version<0>,
      serde::compat_version<0>> {
    using offsets = chunked_hash_map<
      model::topic,
      chunked_hash_map<model::partition_id, kafka::offset>>;
    offsets data;

    auto serde_fields() { return std::tie(data); }
};

} // namespace kafka

/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#pragma once

#include "cloud_topics/types.h"
#include "model/fundamental.h"
#include "model/timestamp.h"
#include "serde/envelope.h"

#include <absl/container/btree_map.h>

// The header file contains list of commands consumed by the
// dl_stm.
//
// The commands are replicated as dl_stm_cmd record batches.
// Every command batch may contain multiple records. Every record has
// a key of type 'dl_stm_key' and payload. The payload is producing
// by serializing one of the structures from this header using serde.

namespace cloud_topics {

struct dl_overlay
  : serde::envelope<dl_overlay, serde::version<0>, serde::compat_version<0>> {
    kafka::offset base_offset;
    kafka::offset last_offset;
    model::timestamp base_ts;
    model::timestamp last_ts;
    // Mapping between offsets and terms. If any new terms
    // start between the 'base_offset' and 'last_offset' the map should
    // have corresponding entries for each of them.
    absl::btree_map<model::term_id, kafka::offset> terms;

    // Object name in the cloud storage
    object_id id;
    dl_stm_object_ownership ownership;
    // Byte range. If the ownership is unique the 'offset' should be
    // zero and 'size_bytes' should be equal to size of the object.
    first_byte_offset_t offset;
    byte_range_size_t size_bytes;
    // TODO: add tx metadata - LSO and list of transactions which are not
    // committed yet

    auto serde_fields() {
        return std::tie(
          base_offset,
          last_offset,
          base_ts,
          last_ts,
          terms,
          id,
          ownership,
          offset,
          size_bytes);
    }

    bool operator==(const dl_overlay& other) const noexcept = default;

    auto operator<=>(const dl_overlay& other) const noexcept {
        return std::tuple(base_offset, last_offset)
               <=> std::tuple(other.base_offset, other.last_offset);
    }
};

} // namespace cloud_topics

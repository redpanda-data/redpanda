// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#pragma once

#include "cloud_topics/types.h"
#include "model/fundamental.h"
#include "model/timestamp.h"
#include "serde/envelope.h"

#include <absl/container/btree_map.h>
#include <fmt/core.h>

namespace experimental::cloud_topics {

/// Physical object description in cloud storage.
///
/// Objects are immutable.
struct dl_overlay_object
  : serde::
      envelope<dl_overlay_object, serde::version<0>, serde::compat_version<0>> {
    dl_overlay_object() = default;
    dl_overlay_object(
      object_id id,
      first_byte_offset_t first_byte_offset,
      byte_range_size_t byte_range_size,
      dl_stm_object_ownership ownership)
      : id(id)
      , first_byte_offset(first_byte_offset)
      , byte_range_size(byte_range_size)
      , ownership(ownership) {}

    auto serde_fields() {
        return std::tie(id, first_byte_offset, byte_range_size, ownership);
    }

    friend bool
    operator==(const dl_overlay_object& lhs, const dl_overlay_object& rhs)
      = default;

    object_id id;
    first_byte_offset_t first_byte_offset;
    byte_range_size_t byte_range_size;
    dl_stm_object_ownership ownership;
};

/// An overlay represents a segment (sequence of kafka record batches) that is
/// stored in cloud storage. Inline metadata is stored in the overlay to support
/// efficient access to the segment.
///
/// Overlays are immutable.
struct dl_overlay
  : serde::envelope<dl_overlay, serde::version<0>, serde::compat_version<0>> {
    dl_overlay() = default;
    dl_overlay(
      kafka::offset base_offset,
      kafka::offset last_offset,
      model::timestamp base_timestamp,
      model::timestamp last_timestamp,
      absl::btree_map<model::term_id, kafka::offset> last_offset_by_term,
      dl_overlay_object object)
      : base_offset(base_offset)
      , last_offset(last_offset)
      , base_timestamp(base_timestamp)
      , last_timestamp(last_timestamp)
      , last_offset_by_term(std::move(last_offset_by_term))
      , object(std::move(object)) {}

    auto serde_fields() {
        return std::tie(
          base_offset,
          last_offset,
          base_timestamp,
          last_timestamp,
          last_offset_by_term,
          object);
    }

    friend bool operator==(const dl_overlay& lhs, const dl_overlay& rhs)
      = default;

    kafka::offset base_offset;
    kafka::offset last_offset;

    model::timestamp base_timestamp;
    model::timestamp last_timestamp;

    /// Last offset for each term in the segment to support efficient
    /// OffsetForLeaderEpoch requests.
    /// https://kafka.apache.org/protocol.html#The_Messages_OffsetForLeaderEpoch.
    absl::btree_map<model::term_id, kafka::offset> last_offset_by_term;

    /// The overlay object that contains the segment data.
    dl_overlay_object object;

    friend std::ostream&
    operator<<(std::ostream& os, const dl_overlay& dl_overlay);
};

} // namespace experimental::cloud_topics

template<>
struct fmt::formatter<experimental::cloud_topics::dl_overlay_object>
  : fmt::formatter<std::string_view> {
    auto format(
      const experimental::cloud_topics::dl_overlay_object&,
      fmt::format_context& ctx) const -> decltype(ctx.out());
};

template<>
struct fmt::formatter<experimental::cloud_topics::dl_overlay>
  : fmt::formatter<std::string_view> {
    auto format(
      const experimental::cloud_topics::dl_overlay&,
      fmt::format_context& ctx) const -> decltype(ctx.out());
};

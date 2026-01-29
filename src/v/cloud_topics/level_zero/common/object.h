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

#include "absl/container/btree_map.h"
#include "base/seastarx.h"
#include "model/fundamental.h"
#include "serde/rw/envelope.h"
#include "serde/rw/map.h"

#include <variant>

namespace cloud_topics::l0 {

// clang-format off
// L0 Object File Format:
// =====================
//
// An L0 object consists of multiple NTPs' data written sequentially,
// followed by a footer with partition location data.
//
// Structure:
// [NTP 1 Data][NTP 2 Data]...[Footer][Footer Size]
//
// Components:
// 1. NTP Data: Serialized record batches for each partition, written
//    contiguously. Data for each NTP is grouped together.
//
// 2. Footer: Serde-serialized footer struct containing:
//    - A map from model::ntp to partition_info (file_position, length)
//    - Uses serde::checksum_envelope for integrity verification
//
// 3. Footer Size: Final 4 bytes containing uint32_t size of the footer data
//    (little-endian). This enables efficient tail-reading of the footer.
//
// clang-format on

// This struct defines the footer for level zero objects. It maps each NTP
// to its location within the object, enabling efficient partition data lookup.
struct footer
  : serde::checksum_envelope<footer, serde::version<0>, serde::version<0>> {
    // Information about each NTP region in the object.
    struct partition_info
      : serde::envelope<partition_info, serde::version<0>, serde::version<0>> {
        // The byte offset in the object where this NTP's data starts.
        size_t file_position = 0;
        // The total size of this NTP's data in bytes.
        size_t length = 0;

        auto serde_fields() { return std::tie(file_position, length); }
        bool operator==(const partition_info&) const = default;
    };

    // Maps NTP to the location of its data in the object.
    // Using btree_map for deterministic iteration order.
    absl::btree_map<model::ntp, partition_info> partitions;

    footer copy() const;

    auto serde_fields() { return std::tie(partitions); }

    bool operator==(const footer&) const = default;

    // Read the footer using the suffix of an L0 object.
    //
    // Returns either the footer, or the *additional* bytes needed to be
    // prepended to the iobuf in order to complete reading the footer.
    //
    // REQUIRES: that the iobuf is at least the last 4 bytes of the file, but
    // likely you want to optimistically read more data (say 512KiB) if you
    // don't know the exact footer location.
    //
    // Example usage:
    //
    // ```c++
    // size_t object_size = ...;
    // auto iobuf = co_await read_object(
    //   handle,
    //   {.offset = object_size - 1_KiB, .size = 1_KiB},
    // );
    // auto result = l0::footer::read(iobuf.share());
    // if (std::holds_alternative<l0::footer>(result)) {
    //   return std::get<l0::footer>(result);
    // }
    // size_t extra = std::get<size_t>(result);
    // auto missing = co_await read_object(
    //   handle,
    //   {.offset = object_size - 1_KiB - extra, .size = extra},
    // );
    // missing.append(std::move(iobuf));
    // result = l0::footer::read(std::move(missing));
    // return std::get<l0::footer>(result);
    // ```
    static std::variant<footer, size_t> read(iobuf);
};

} // namespace cloud_topics::l0

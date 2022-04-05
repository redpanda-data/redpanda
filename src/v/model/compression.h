/*
 * Copyright 2020 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include <cstdint>
#include <iosfwd>
#include <limits>
#include <ostream>
#include <type_traits>

namespace model {

/*
 * This enum serves three purposes:
 *
 * 1. It is used represent batch compression types (eg none, lz4)
 * 2. It is used represent compression policies (eg producer)
 *
 * And (3) the enum values for batch compression types must be identical to
 * those values used in the encoding of kafka batch attributes.
 *
 *    https://kafka.apache.org/documentation/#compression.type
 *
 * Compression types like `producer` have values outside the range of values
 * used in any wire encoded representation and are only used internally.
 */
enum class compression : uint8_t {
    none = 0,
    gzip = 1,
    // NOTE: This is *NOT* standard snappy compression. It uses the java-snappy
    // framing compression which is fundamentally not compatible with upstream
    // google snappy
    snappy = 2,
    lz4 = 3,
    zstd = 4,

    // values below must not intersect with the value range used to encode
    // compression codecs in kafka batch attributes.
    producer = std::numeric_limits<std::underlying_type_t<compression>>::max()
};

/// operators needed for boost::lexical_cast<compression>
/// inline to prevent library depdency with the v::compression module
inline std::ostream& operator<<(std::ostream& os, const compression& c) {
    switch (c) {
    case compression::none:
        os << "none";
        break;
    case compression::gzip:
        os << "gzip";
        break;
    case compression::snappy:
        os << "snappy";
        break;
    case compression::lz4:
        os << "lz4";
        break;
    case compression::zstd:
        os << "zstd";
        break;
    case compression::producer:
        os << "producer";
        break;
    default:
        os << "ERROR";
        break;
    }
    return os;
}
std::istream& operator>>(std::istream&, compression&);

} // namespace model

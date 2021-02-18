/*
 * Copyright 2020 Vectorized, Inc.
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

namespace model {

/// https://kafka.apache.org/documentation/#compression.type
/// we use the same enum as kafka
///
enum class compression : uint8_t {
    none = 0,
    gzip = 1,
    // NOTE: This is *NOT* standard snappy compression. It uses the java-snappy
    // framing compression which is fundamentally not compatible with upstream
    // google snappy
    snappy = 2,
    lz4 = 3,
    zstd = 4
};

/// operators needed for boost::lexical_cast<compression>
std::ostream& operator<<(std::ostream&, const compression&);
std::istream& operator>>(std::istream&, compression&);

} // namespace model

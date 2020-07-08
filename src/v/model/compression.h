#pragma once

#include <cstdint>
#include <iosfwd>
#include <iostream>

namespace model {

/// https://kafka.apache.org/documentation/#compression.type
/// we use the same enum as kafka
///
enum class compression : uint8_t {
    none = 0,
    gzip = 1,
    snappy = 2,
    lz4 = 3,
    zstd = 4
};

/// operators needed for boost::lexical_cast<compression>
std::ostream& operator<<(std::ostream&, const compression&);
std::istream& operator>>(std::istream&, compression&);

} // namespace model

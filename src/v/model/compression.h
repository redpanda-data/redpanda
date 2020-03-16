#pragma once

#include <cstdint>
#include <iosfwd>
#include <iostream>

namespace model {

enum class compression : uint8_t { none, gzip, snappy, lz4, zstd };

/// operators needed for boost::lexical_cast<compression>
std::ostream& operator<<(std::ostream&, const compression&);
std::istream& operator>>(std::istream&, compression&);

} // namespace model

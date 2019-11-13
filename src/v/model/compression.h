#pragma once

#include <cstdint>
#include <iosfwd>

namespace model {

enum class compression : uint8_t { none, gzip, snappy, lz4, zstd };

std::ostream& operator<<(std::ostream&, const compression&);
std::istream& operator>>(std::istream&, compression&);

} // namespace model

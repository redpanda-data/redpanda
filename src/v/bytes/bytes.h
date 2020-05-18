#pragma once

#include "bytes/iobuf.h"
#include "seastarx.h"

#include <seastar/core/sstring.hh>

#include <cstdint>
#include <iosfwd>

using bytes = ss::basic_sstring<
  int8_t,   // Must be different from char to not leak to std::string_view
  uint32_t, // size type - 4 bytes - 4GB max
  31,       // short string optimization size
  false     // not null terminated
  >;

using bytes_view = std::basic_string_view<int8_t>;
using bytes_opt = std::optional<bytes>;

ss::sstring to_hex(bytes_view b);
ss::sstring to_hex(const bytes& b);

std::ostream& operator<<(std::ostream& os, const bytes& b);
std::ostream& operator<<(std::ostream& os, const bytes_opt& b);

static inline bytes iobuf_to_bytes(const iobuf& in) {
    auto out = ss::uninitialized_string<bytes>(in.size_bytes());
    {
        iobuf::iterator_consumer it(in.cbegin(), in.cend());
        it.consume_to(in.size_bytes(), out.begin());
    }
    return out;
}

static inline iobuf bytes_to_iobuf(const bytes& in) {
    iobuf out;
    out.append(reinterpret_cast<const char*>(in.c_str()), in.size());
    return out;
}

namespace std {

template<>
struct hash<bytes_view> {
    size_t operator()(bytes_view v) const {
        return hash<std::string_view>()(
          {reinterpret_cast<const char*>(v.begin()), v.size()});
    }
};

std::ostream& operator<<(std::ostream& os, const bytes_view& b);

} // namespace std

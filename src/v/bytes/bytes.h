#pragma once

#include "seastarx.h"

#include <seastar/core/sstring.hh>

#include <cstdint>
#include <iosfwd>

using bytes = ss::basic_sstring<int8_t, uint32_t, 31, false>;
using bytes_view = std::basic_string_view<int8_t>;
using bytes_opt = std::optional<bytes>;

ss::sstring to_hex(bytes_view b);
ss::sstring to_hex(const bytes& b);

std::ostream& operator<<(std::ostream& os, const bytes& b);
std::ostream& operator<<(std::ostream& os, const bytes_opt& b);

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

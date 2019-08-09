#pragma once

#include "bytes/bytes_ostream.h"
#include "rpc/for_each_field.h"
#include "seastarx.h"
#include "utils/fragmented_temporary_buffer.h"
#include "utils/vint.h"

#include <seastar/core/byteorder.hh>
#include <seastar/core/unaligned.hh>

#include <fmt/format.h>

namespace rpc {

/// \brief is_standard_layout && is_trivially_copyable cover _most_!
/// fields including [[gnu::packed]] and all numeric types
template<typename T>
void serialize(bytes_ostream& out, T& t) {
    constexpr bool is_fragmented_buffer
      = std::is_same_v<T, fragmented_temporary_buffer>;
    constexpr bool is_standard_layout = std::is_standard_layout_v<T>;
    constexpr bool is_trivially_copyable = std::is_trivially_copyable_v<T>;

    if constexpr (is_fragmented_buffer) {
        const int32_t sz = t.size_bytes();
        out.write(reinterpret_cast<const char*>(&sz), sizeof(sz));
        auto vec = std::move(t).release();
        for (temporary_buffer<char>& b : vec) {
            out.write(std::move(b));
        }
        return;
    } else if constexpr (
      !is_fragmented_buffer && is_standard_layout && is_trivially_copyable) {
        // std::is_pod_v is deprecated
        // Deprecating the notion of “plain old data” (POD). It has been
        // replaced with two more nuanced categories of types, “trivial” and
        // “standard-layout”. “POD” is equivalent to “trivial and standard
        // layout”, but for many code patterns, a narrower restriction to just
        // “trivial” or just “standard layout” is appropriate; to encourage such
        // precision, the notion of “POD” was therefore deprecated. The library
        // trait is_pod has also been deprecated correspondingly.
        constexpr auto sz = sizeof(T);
        out.write(reinterpret_cast<const char*>(&t), sz);
        return;
    } else if constexpr (!is_fragmented_buffer && is_standard_layout) {
        for_each_field(t, [&out](auto& field) {
            serialize<std::decay_t<decltype(field)>>(out, field);
        });
        return;
    }
    throw std::runtime_error(fmt::format(
      "rpc: no serializer registered. is_fragmented_buffer:{}, is_trivial:{}, "
      "is_standard_layout:{}, is_copy_constructible:{}",
      is_fragmented_buffer,
      std::is_trivial_v<T>,
      is_standard_layout,
      is_trivially_copyable));
}

} // namespace rpc

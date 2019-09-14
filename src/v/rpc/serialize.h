#pragma once

#include "bytes/bytes_ostream.h"
#include "rpc/for_each_field.h"
#include "rpc/is_std_helpers.h"
#include "seastarx.h"
#include "utils/fragbuf.h"
#include "utils/named_type.h"

#include <seastar/core/byteorder.hh>
#include <seastar/core/sstring.hh>
#include <seastar/core/unaligned.hh>

#include <fmt/format.h>

namespace rpc {
/// \brief is_standard_layout && is_trivially_copyable cover _most_!
/// fields including [[gnu::packed]] and all numeric types
template<typename T>
void serialize(bytes_ostream& out, T&& t) {
    constexpr bool is_sstring = std::is_same_v<T, sstring>;
    constexpr bool is_vector = is_std_vector_v<T>;
    constexpr bool is_fragmented_buffer = std::is_same_v<T, fragbuf>;
    constexpr bool is_standard_layout = std::is_standard_layout_v<T>;
    constexpr bool is_trivially_copyable = std::is_trivially_copyable_v<T>;

    if constexpr (is_sstring) {
        serialize<int32_t>(out, t.size());
        out.write(t.data(), t.size());
        return;
    } else if constexpr (is_vector) {
        using value_type = typename std::decay_t<T>::value_type;
        serialize<int32_t>(out, t.size());
        for (value_type& i : t) {
            serialize<value_type>(out, std::move(i));
        }
        return;
    } else if constexpr (is_fragmented_buffer) {
        serialize<int32_t>(out, t.size_bytes());
        auto vec = std::move(t).release();
        for (temporary_buffer<char>& b : vec) {
            out.write(std::move(b));
        }
        return;
    } else if constexpr (is_standard_layout && is_trivially_copyable) {
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
    } else if constexpr (is_standard_layout) {
        for_each_field(
          t, [&out](auto& field) { serialize(out, std::move(field)); });
        return;
    }
    static_assert(
      (is_vector || is_fragmented_buffer || is_standard_layout
       || is_trivially_copyable),
      "rpc: no serializer registered");
}
template<typename T, typename Tag>
void serialize(bytes_ostream& out, const named_type<T, Tag>& r) {
    serialize(out, r());
}
} // namespace rpc

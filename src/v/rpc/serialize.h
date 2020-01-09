#pragma once

#include "bytes/iobuf.h"
#include "rpc/for_each_field.h"
#include "rpc/is_std_helpers.h"
#include "seastarx.h"
#include "utils/named_type.h"

#include <seastar/core/byteorder.hh>
#include <seastar/core/sstring.hh>
#include <seastar/core/unaligned.hh>

#include <fmt/format.h>

namespace rpc {
/// \brief is_standard_layout && is_trivially_copyable cover _most_!
/// fields including [[gnu::packed]] and all numeric types
template<typename T>
void serialize(iobuf& out, T&& t) {
    static_assert(
      std::is_rvalue_reference_v<decltype(t)>,
      "Must be an rvalue. Use std::move()");
    constexpr bool is_optional = is_std_optional_v<T>;
    constexpr bool is_sstring = std::is_same_v<T, ss::sstring>;
    constexpr bool is_vector = is_std_vector_v<T>;
    constexpr bool is_iobuf = std::is_same_v<T, iobuf>;
    constexpr bool is_standard_layout = std::is_standard_layout_v<T>;
    constexpr bool is_trivially_copyable = std::is_trivially_copyable_v<T>;

    if constexpr (is_optional) {
        /// sizeof(bool) is implementation defined, and the standard puts
        /// notable emphasis on this fact.
        //  section: §5.3.3/1 of the standard:
        using value_type = typename std::decay_t<T>::value_type;
        if (t) {
            serialize<int8_t>(out, 1);
            serialize<value_type>(out, std::move(t.value()));
        } else {
            serialize<int8_t>(out, 0);
        }
        return;
    } else if constexpr (is_sstring) {
        serialize(out, int32_t(t.size()));
        out.append(t.data(), t.size());
        return;
    } else if constexpr (is_vector) {
        using value_type = typename std::decay_t<T>::value_type;
        serialize<int32_t>(out, t.size());
        for (value_type& i : t) {
            serialize<value_type>(out, std::move(i));
        }
        return;
    } else if constexpr (is_iobuf) {
        serialize<int32_t>(out, t.size_bytes());
        out.append(std::move(t));
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
        out.append(reinterpret_cast<const char*>(&t), sz);
        return;
    } else if constexpr (is_standard_layout) {
        for_each_field(
          t, [&out](auto& field) { serialize(out, std::move(field)); });
        return;
    }
    static_assert(
      (is_optional || is_sstring || is_vector || is_iobuf
       || (is_standard_layout) || is_trivially_copyable),
      "rpc: no serializer registered");
}
template<typename T, typename Tag>
void serialize(iobuf& out, const named_type<T, Tag>& r) {
    serialize(out, r());
}
template<typename... T>
void serialize(iobuf& out, T&&... args) {
    (serialize(out, std::move(args)), ...);
}

template<typename T>
inline iobuf serialize(T&& val) {
    iobuf out;
    rpc::serialize(out, std::forward<T>(val));
    return out;
}

} // namespace rpc

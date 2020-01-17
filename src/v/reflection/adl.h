#pragma once

#include "bytes/iobuf.h"
#include "bytes/iobuf_parser.h"
#include "reflection/for_each_field.h"
#include "seastarx.h"
#include "utils/named_type.h"

#include <seastar/core/sstring.hh>

#include <optional>
#include <type_traits>
#include <vector>

namespace reflection {
template<typename T>
struct is_std_vector : std::false_type {};
template<typename... Args>
struct is_std_vector<std::vector<Args...>> : std::true_type {};
template<typename T>
inline constexpr bool is_std_vector_v = is_std_vector<T>::value;

template<typename T>
struct is_std_optional : std::false_type {};
template<typename... Args>
struct is_std_optional<std::optional<Args...>> : std::true_type {};
template<typename T>
inline constexpr bool is_std_optional_v = is_std_optional<T>::value;

template<typename T>
struct is_named_type : std::false_type {};
template<typename T, typename Tag>
struct is_named_type<named_type<T, Tag>> : std::true_type {};
template<typename T>
inline constexpr bool is_named_type_v = is_named_type<T>::value;

template<typename T>
struct adl {
    using type = std::remove_reference_t<std::decay_t<T>>;
    static constexpr bool is_optional = is_std_optional_v<type>;
    static constexpr bool is_sstring = std::is_same_v<type, ss::sstring>;
    static constexpr bool is_vector = is_std_vector_v<type>;
    static constexpr bool is_named_type = is_named_type_v<type>;
    static constexpr bool is_iobuf = std::is_same_v<type, iobuf>;
    static constexpr bool is_standard_layout = std::is_standard_layout_v<type>;
    static constexpr bool is_trivially_copyable
      = std::is_trivially_copyable_v<type>;
    static_assert(
      is_optional || is_sstring || is_vector || is_named_type || is_iobuf
        || is_standard_layout || is_trivially_copyable,
      "rpc: no adl registered");

    type from(iobuf io) {
        auto parser = iobuf_parser(std::move(io));
        return adl<type>{}.from(parser);
    }
    type from(iobuf_parser& in) {
        if constexpr (is_named_type) {
            using value_type = typename type::type;
            return type(adl<value_type>{}.from(in));
        } else if constexpr (is_optional) {
            using value_type = typename type::value_type;
            int8_t is_set = in.consume_type<int8_t>();
            if (is_set == 0) {
                return std::nullopt;
            }
            return adl<value_type>{}.from(in);
        } else if constexpr (is_sstring) {
            return in.read_string(in.consume_type<int32_t>());
        } else if constexpr (is_vector) {
            using value_type = typename type::value_type;
            int32_t n = in.consume_type<int32_t>();
            std::vector<value_type> ret;
            ret.reserve(n);
            while (n-- > 0) {
                ret.push_back(adl<value_type>{}.from(in));
            }
            return ret;
        } else if constexpr (is_iobuf) {
            return in.share(in.consume_type<int32_t>());
        } else if constexpr (is_standard_layout && is_trivially_copyable) {
            return in.consume_type<T>();
        } else if constexpr (is_standard_layout) {
            T t;
            reflection::for_each_field(t, [&in](auto& field) mutable {
                field = std::move(
                  adl<std::decay_t<decltype(field)>>{}.from(in));
            });
            return t;
        }
    }

    void to(iobuf& out, type& t) { adl<type>{}.to(out, std::move(t)); }

    void to(iobuf& out, type t) {
        if constexpr (is_named_type) {
            using value_type = typename type::type;
            adl<value_type>{}.to(out, value_type(t()));
            return;
        } else if constexpr (is_optional) {
            /// sizeof(bool) is implementation defined, and the standard puts
            /// notable emphasis on this fact.
            //  section: §5.3.3/1 of the standard:
            using value_type = typename type::value_type;
            if (t) {
                adl<int8_t>{}.to(out, 1);
                adl<value_type>{}.to(out, std::move(t.value()));
            } else {
                adl<int8_t>{}.to(out, 0);
            }
            return;
        } else if constexpr (is_sstring) {
            adl<int32_t>{}.to(out, int32_t(t.size()));
            out.append(t.data(), t.size());
            return;
        } else if constexpr (is_vector) {
            using value_type = typename type::value_type;
            adl<int32_t>{}.to(out, t.size());
            for (value_type& i : t) {
                adl<value_type>{}.to(out, std::move(i));
            }
            return;
        } else if constexpr (is_iobuf) {
            adl<int32_t>{}.to(out, t.size_bytes());
            out.append(std::move(t));
            return;
        } else if constexpr (is_standard_layout && is_trivially_copyable) {
            // std::is_pod_v is deprecated
            // Deprecating the notion of “plain old data” (POD). It has been
            // replaced with two more nuanced categories of types, “trivial” and
            // “standard-layout”. “POD” is equivalent to “trivial and standard
            // layout”, but for many code patterns, a narrower restriction to
            // just “trivial” or just “standard layout” is appropriate; to
            // encourage such precision, the notion of “POD” was therefore
            // deprecated. The library trait is_pod has also been deprecated
            // correspondingly.
            constexpr auto sz = sizeof(T);
            out.append(reinterpret_cast<const char*>(&t), sz);
            return;
        } else if constexpr (is_standard_layout) {
            /*
            std::apply(
              [&out](auto&&... args) {
                  (adl<decltype(args)>{}.to(out, std::move(args)), ...);
              },
              reflection::to_tuple(t)); */
            reflection::for_each_field(t, [&out](auto& field) {
                adl<std::decay_t<decltype(field)>>{}.to(out, std::move(field));
            });
            return;
        }
    }
};

// variadic helper
template<typename... T>
void serialize(iobuf& out, T&&... args) {
    (adl<std::decay_t<T>>{}.to(out, std::move(args)), ...);
}
template<typename T>
iobuf to_iobuf(T&& val) {
    iobuf out;
    adl<std::decay_t<T>>{}.to(out, std::forward<T>(val));
    return out;
}
template<typename T>
T from_iobuf(iobuf b) {
    iobuf_parser parser(std::move(b));
    return adl<std::decay_t<T>>{}.from(parser);
}

} // namespace reflection

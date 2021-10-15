// Copyright 2021 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#pragma once

#include "bytes/iobuf_parser.h"
#include "reflection/type_traits.h"
#include "serde/envelope_for_each_field.h"
#include "serde/logger.h"
#include "serde/serde_exception.h"
#include "serde/type_str.h"
#include "ssx/future-util.h"
#include "ssx/sformat.h"
#include "utils/named_type.h"
#include "vlog.h"

#include <iosfwd>
#include <numeric>
#include <string>
#include <string_view>

namespace serde {

template<typename To, typename From>
To bit_cast(From const& f) {
    static_assert(sizeof(From) == sizeof(To));
    static_assert(std::is_trivially_copyable_v<To>);
    To to;
    std::memcpy(&to, &f, sizeof(To));
    return to;
}

struct header {
    version_t _version, _compat_version;
    size_t _bytes_left_limit;
};

template<typename T, typename = void>
struct help_has_serde_read : std::false_type {};

template<typename T>
struct help_has_serde_read<
  T,
  std::void_t<decltype(std::declval<T>().serde_read(
    std::declval<std::add_lvalue_reference_t<iobuf_parser>>(),
    std::declval<header>()))>> : std::true_type {};

template<typename T>
inline constexpr auto const has_serde_read = help_has_serde_read<T>::value;

template<typename T, typename = void>
struct help_has_serde_write : std::false_type {};

template<typename T>
struct help_has_serde_write<
  T,
  std::void_t<decltype(std::declval<T>().serde_write(
    std::declval<std::add_lvalue_reference_t<iobuf>>()))>> : std::true_type {};

template<typename T>
inline constexpr auto const has_serde_write = help_has_serde_write<T>::value;

template<typename T, typename = void>
struct help_has_serde_async_read : std::false_type {};

template<typename T>
struct help_has_serde_async_read<
  T,
  std::void_t<decltype(std::declval<T>().serde_async_read(
    std::declval<std::add_lvalue_reference_t<iobuf_parser>>(),
    std::declval<header>()))>> : std::true_type {};

template<typename T>
inline constexpr auto const has_serde_async_read
  = help_has_serde_async_read<T>::value;

template<typename T, typename = void>
struct help_has_serde_async_write : std::false_type {};

template<typename T>
struct help_has_serde_async_write<
  T,
  std::void_t<decltype(std::declval<T>().serde_async_write(
    std::declval<std::add_lvalue_reference_t<iobuf>>()))>> : std::true_type {};

template<typename T>
inline constexpr auto const has_serde_async_write
  = help_has_serde_async_write<T>::value;

template<typename, typename = void>
struct is_blessed_enum : std::false_type {};

template<typename T>
struct is_blessed_enum<T, std::void_t<decltype(serde_bless_enum(T{}))>>
  : std::true_type {};

template<typename T>
struct enum_type_matches {
    static constexpr auto const value = std::
      is_same_v<std::underlying_type_t<T>, decltype(serde_bless_enum(T{}))>;
};

template<typename T>
struct is_blessed_size_enum {
    static constexpr auto const value
      = std::conjunction_v<is_blessed_enum<T>, enum_type_matches<T>>;
};

template<typename T>
inline constexpr auto const is_serde_compatible_v
  = is_envelope_v<T>
    || (std::is_scalar_v<T>  //
         && (!std::is_same_v<float, T> || std::numeric_limits<float>::is_iec559)
         && (!std::is_same_v<double, T> || std::numeric_limits<double>::is_iec559)
         && (!std::is_enum_v<T> ||
              std::conjunction_v<std::is_enum<T>, is_blessed_size_enum<T>>))
    || reflection::is_std_vector_v<T>
    || reflection::is_named_type_v<T>
    || reflection::is_ss_bool_v<T>
    || reflection::is_std_optional_v<T>
    || std::is_same_v<T, std::chrono::milliseconds>
    || std::is_same_v<T, iobuf>
    || std::is_same_v<T, ss::sstring>;

#if defined(SERDE_TEST)
using serde_size_t = uint16_t;
#else
using serde_size_t = uint32_t;
#endif

template<typename T>
void write(iobuf&, T);

template<typename T>
void write(iobuf& out, T t) {
    using Type = std::decay_t<T>;
    static_assert(has_serde_write<Type> || is_serde_compatible_v<Type>);

    if constexpr (is_envelope_v<Type>) {
        write(out, Type::redpanda_serde_version);
        write(out, Type::redpanda_serde_compat_version);

        auto size_placeholder = out.reserve(sizeof(serde_size_t));
        auto const size_before = out.size_bytes();

        if constexpr (has_serde_write<Type>) {
            t.serde_write(out);
        } else {
            envelope_for_each_field(
              t, [&out](auto& f) { write(out, std::move(f)); });
        }

        auto const written_size = out.size_bytes() - size_before;
        if (unlikely(written_size > std::numeric_limits<serde_size_t>::max())) {
            throw serde_exception("envelope too big");
        }
        auto const size = ss::cpu_to_le(
          static_cast<serde_size_t>(written_size));
        size_placeholder.write(
          reinterpret_cast<char const*>(&size), sizeof(serde_size_t));
    } else if constexpr (std::is_same_v<bool, Type>) {
        write(out, static_cast<int8_t>(t));
    } else if constexpr (std::is_enum_v<Type>) {
        write(out, static_cast<std::underlying_type_t<Type>>(t));
    } else if constexpr (std::is_scalar_v<Type> && !std::is_enum_v<Type>) {
        if constexpr (sizeof(Type) == 1) {
            out.append(reinterpret_cast<char const*>(&t), sizeof(t));
        } else if constexpr (std::is_same_v<float, Type>) {
            auto const le_t = htole32(bit_cast<uint32_t>(t));
            static_assert(sizeof(le_t) == sizeof(Type));
            out.append(reinterpret_cast<char const*>(&le_t), sizeof(le_t));
        } else if constexpr (std::is_same_v<double, Type>) {
            auto const le_t = htole64(bit_cast<uint64_t>(t));
            static_assert(sizeof(le_t) == sizeof(Type));
            out.append(reinterpret_cast<char const*>(&le_t), sizeof(le_t));
        } else {
            auto const le_t = ss::cpu_to_le(t);
            static_assert(sizeof(le_t) == sizeof(Type));
            out.append(reinterpret_cast<char const*>(&le_t), sizeof(le_t));
        }
    } else if constexpr (reflection::is_std_vector_v<Type>) {
        if (unlikely(t.size() > std::numeric_limits<serde_size_t>::max())) {
            throw serde_exception(fmt_with_ctx(
              ssx::sformat,
              "serde: vector size {} exceeds serde_size_t",
              t.size()));
        }
        write(out, static_cast<serde_size_t>(t.size()));
        for (auto& el : t) {
            write(out, std::move(el));
        }
    } else if constexpr (reflection::is_named_type_v<Type>) {
        return write(out, static_cast<typename Type::type>(t));
    } else if constexpr (reflection::is_ss_bool_v<Type>) {
        write(out, static_cast<int8_t>(bool(t)));
    } else if constexpr (std::is_same_v<Type, std::chrono::milliseconds>) {
        write<int64_t>(out, t.count());
    } else if constexpr (std::is_same_v<Type, iobuf>) {
        write<serde_size_t>(out, t.size_bytes());
        out.append(t.share(0, t.size_bytes()));
    } else if constexpr (std::is_same_v<Type, ss::sstring>) {
        write<serde_size_t>(out, t.size());
        out.append(t.data(), t.size());
    } else if constexpr (reflection::is_std_optional_v<Type>) {
        if (t) {
            write(out, true);
            write(out, std::move(t.value()));
        } else {
            write(out, false);
        }
    }
}

template<typename T>
std::decay_t<T> read_nested(iobuf_parser&, std::size_t bytes_left_limit);

template<typename T>
std::decay_t<T> read(iobuf_parser& in) {
    auto ret = read_nested<T>(in, 0U);
    if (unlikely(in.bytes_left() != 0)) {
        throw serde_exception{fmt_with_ctx(
          ssx::sformat,
          "serde: not all bytes consumed after read<{}>(), bytes_left={}",
          type_str<T>(),
          in.bytes_left())};
    }
    return std::move(ret);
}

template<typename T>
header read_header(iobuf_parser& in, std::size_t const bytes_left_limit) {
    using Type = std::decay_t<T>;

    auto const version = read_nested<version_t>(in, bytes_left_limit);
    auto const compat_version = read_nested<version_t>(in, bytes_left_limit);
    auto const size = read_nested<serde_size_t>(in, bytes_left_limit);

    if (unlikely(in.bytes_left() < size)) {
        throw serde_exception(fmt_with_ctx(
          ssx::sformat,
          "bytes_left={}, size={}",
          in.bytes_left(),
          static_cast<int>(size)));
    }

    if (unlikely(in.bytes_left() - size < bytes_left_limit)) {
        throw serde_exception(fmt_with_ctx(
          ssx::sformat,
          "envelope does not fit into bytes left: bytes_left={}, size={}, "
          "bytes_left_limit={}",
          in.bytes_left(),
          static_cast<int>(size),
          bytes_left_limit));
    }

    if (unlikely(compat_version > Type::redpanda_serde_version)) {
        throw serde_exception(fmt_with_ctx(
          ssx::sformat,
          "read {}: compat_version={} > {}::version={}",
          type_str<Type>(),
          static_cast<int>(compat_version),
          type_str<T>(),
          static_cast<int>(Type::redpanda_serde_version)));
    }

    return header{
      ._version = version,
      ._compat_version = compat_version,
      ._bytes_left_limit = in.bytes_left() - size};
}

template<typename T>
void read_nested(iobuf_parser& in, T& t, std::size_t const bytes_left_limit) {
    using Type = std::decay_t<T>;
    static_assert(has_serde_read<T> || is_serde_compatible_v<Type>);

    if constexpr (is_envelope_v<Type>) {
        auto const h = read_header<Type>(in, bytes_left_limit);
        if constexpr (has_serde_read<Type>) {
            t.serde_read(in, h);
        } else {
            envelope_for_each_field(t, [&](auto& f) {
                using FieldType = std::decay_t<decltype(f)>;
                if (h._bytes_left_limit == in.bytes_left()) {
                    return false;
                }
                if (unlikely(in.bytes_left() < h._bytes_left_limit)) {
                    throw serde_exception(fmt_with_ctx(
                      ssx::sformat,
                      "field spill over in {}, field type {}: envelope_end={}, "
                      "in.bytes_left()={}",
                      type_str<Type>(),
                      type_str<FieldType>(),
                      h._bytes_left_limit,
                      in.bytes_left()));
                }
                f = read_nested<FieldType>(in, bytes_left_limit);
                return true;
            });
        }
        if (in.bytes_left() > h._bytes_left_limit) {
            in.skip(in.bytes_left() - h._bytes_left_limit);
        }
    } else if constexpr (std::is_same_v<Type, bool>) {
        t = read_nested<int8_t>(in, bytes_left_limit) != 0;
    } else if constexpr (std::is_enum_v<Type>) {
        t = static_cast<Type>(
          read_nested<std::underlying_type_t<T>>(in, bytes_left_limit));
    } else if constexpr (std::is_scalar_v<Type> && !std::is_enum_v<Type>) {
        if (unlikely(in.bytes_left() < sizeof(Type))) {
            throw serde_exception{"message too short"};
        }

        if constexpr (sizeof(Type) == 1) {
            t = in.consume_type<Type>();
        } else if constexpr (std::is_same_v<float, Type>) {
            t = bit_cast<float>(le32toh(in.consume_type<uint32_t>()));
        } else if constexpr (std::is_same_v<double, Type>) {
            t = bit_cast<double>(le64toh(in.consume_type<uint64_t>()));
        } else {
            t = ss::le_to_cpu(in.consume_type<Type>());
        }
    } else if constexpr (reflection::is_std_vector_v<Type>) {
        using value_type = typename Type::value_type;
        t.resize(read_nested<serde_size_t>(in, bytes_left_limit));
        for (auto i = 0U; i < t.size(); ++i) {
            t[i] = read_nested<value_type>(in, bytes_left_limit);
        }
    } else if constexpr (reflection::is_named_type_v<Type>) {
        t = Type{read_nested<typename Type::type>(in, bytes_left_limit)};
    } else if constexpr (reflection::is_ss_bool_v<Type>) {
        t = Type{read_nested<int8_t>(in, bytes_left_limit) != 0};
    } else if constexpr (std::is_same_v<Type, std::chrono::milliseconds>) {
        t = std::chrono::milliseconds{
          read_nested<int64_t>(in, bytes_left_limit)};
    } else if constexpr (std::is_same_v<Type, iobuf>) {
        t = in.share(read_nested<serde_size_t>(in, bytes_left_limit));
    } else if constexpr (std::is_same_v<Type, ss::sstring>) {
        auto str = ss::uninitialized_string(
          read_nested<serde_size_t>(in, bytes_left_limit));
        in.consume_to(str.size(), str.begin());
        t = str;
    } else if constexpr (reflection::is_std_optional_v<Type>) {
        t = read_nested<bool>(in, bytes_left_limit)
              ? Type{read_nested<typename Type::value_type>(
                in, bytes_left_limit)}
              : std::nullopt;
    }
}

template<typename T>
std::decay_t<T>
read_nested(iobuf_parser& in, std::size_t const bytes_left_limit) {
    using Type = std::decay_t<T>;
    auto t = Type();
    read_nested(in, t, bytes_left_limit);
    return t;
}

template<typename T>
ss::future<std::decay_t<T>>
read_async_nested(iobuf_parser& in, size_t const bytes_left_limit) {
    using Type = std::decay_t<T>;
    if constexpr (has_serde_async_read<Type>) {
        auto const h = read_header<Type>(in, bytes_left_limit);
        return ss::do_with(Type{}, [&in, h](Type& t) {
            return t.serde_async_read(in, h).then(
              [&t]() { return std::move(t); });
        });
    } else {
        return ss::make_ready_future<std::decay_t<T>>(read<T>(in));
    }
}

template<typename T>
ss::future<std::decay_t<T>> read_async(iobuf_parser& in) {
    return read_async_nested<T>(in, 0).then([&](std::decay_t<T>&& t) {
        if (likely(in.bytes_left() == 0)) {
            return ss::make_ready_future<std::decay_t<T>>(t);
        } else {
            return ss::make_exception_future<std::decay_t<T>>(
              serde_exception{fmt_with_ctx(
                ssx::sformat,
                "serde: not all bytes consumed after read_async<{}>(), "
                "bytes_left={}",
                type_str<T>(),
                in.bytes_left())});
        }
    });
}

template<typename T>
ss::future<> write_async(iobuf& out, T const& t) {
    using Type = std::decay_t<T>;
    if constexpr (is_envelope_v<Type> && has_serde_async_write<Type>) {
        write(out, Type::redpanda_serde_version);
        write(out, Type::redpanda_serde_compat_version);

        auto size_placeholder = out.reserve(sizeof(serde_size_t));
        auto const size_before = out.size_bytes();

        return t.serde_async_write(out).then(
          [&out,
           size_before,
           size_placeholder = std::move(size_placeholder)]() mutable {
              auto const written_size = out.size_bytes() - size_before;
              if (unlikely(
                    written_size > std::numeric_limits<serde_size_t>::max())) {
                  throw serde_exception{"envelope too big"};
              }
              auto const size = ss::cpu_to_le(
                static_cast<serde_size_t>(written_size));
              size_placeholder.write(
                reinterpret_cast<char const*>(&size), sizeof(serde_size_t));

              return ss::make_ready_future<>();
          });
    } else {
        write(out, t);
        return ss::make_ready_future<>();
    }
}

template<typename T>
iobuf to_iobuf(T&& t) {
    iobuf b;
    write(b, std::forward<T>(t));
    return b;
}

template<typename T>
T from_iobuf(iobuf b) {
    auto in = iobuf_parser{std::move(b)};
    return read<T>(in);
}

} // namespace serde

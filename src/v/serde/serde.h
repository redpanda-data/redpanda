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

template<typename T, typename = void>
struct help_has_serde_read : std::false_type {};

template<typename T>
struct help_has_serde_read<
  T,
  std::void_t<decltype(std::declval<T>().serde_read(
    std::declval<std::add_lvalue_reference_t<iobuf_parser>>(),
    version_t{0},
    version_t{0},
    size_t{0U}))>> : std::true_type {};

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
    version_t{0},
    version_t{0},
    size_t{0U}))>> : std::true_type {};

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

template<typename T>
inline constexpr auto const is_serde_compatible_v
  = is_envelope_v<T> || (std::is_scalar_v<T> && !std::is_enum_v<T>)
    || reflection::is_std_vector_v<
      T> || reflection::is_named_type_v<T> || reflection::is_ss_bool_v<T> || std::is_same_v<T, std::chrono::milliseconds> || std::is_same_v<T, iobuf> || std::is_same_v<T, ss::sstring> || reflection::is_std_optional_v<T>;

using header_t = std::tuple<version_t, version_t, size_t>;

#if defined(SERDE_TEST)
using serde_size_t = uint16_t;
#else
using serde_size_t = int32_t;
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
        write<int8_t>(out, t);
    } else if constexpr (std::is_scalar_v<Type> && !std::is_enum_v<Type>) {
        if constexpr (sizeof(Type) == 1) {
            out.append(reinterpret_cast<char const*>(&t), sizeof(t));
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
        for (auto const& el : t) {
            write(out, el);
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
            write(out, t.value());
        } else {
            write(out, false);
        }
    }
}

template<typename T>
std::decay_t<T> read(iobuf_parser&);

template<typename T>
header_t read_header(iobuf_parser& in) {
    using Type = std::decay_t<T>;

    auto const version = read<version_t>(in);
    auto const compat_version = read<version_t>(in);
    auto const size = read<serde_size_t>(in);

    if (unlikely(compat_version > Type::redpanda_serde_version)) {
        throw serde_exception(fmt_with_ctx(
          ssx::sformat,
          "read compat_version={} > {}::version={}",
          static_cast<int>(compat_version),
          type_str<T>(),
          static_cast<int>(Type::redpanda_serde_version)));
    }

    if (unlikely(in.bytes_left() < size)) {
        throw serde_exception(fmt_with_ctx(
          ssx::sformat,
          "bytes_left={}, size={}",
          in.bytes_left(),
          static_cast<int>(size)));
    }

    return std::make_tuple(version, compat_version, size);
}

template<typename T>
std::decay_t<T> read(iobuf_parser& in) {
    using Type = std::decay_t<T>;
    static_assert(has_serde_read<T> || is_serde_compatible_v<Type>);

    auto t = Type();
    if constexpr (is_envelope_v<Type>) {
        [[maybe_unused]] auto const [version, compat_version, size]
          = read_header<Type>(in);
        if constexpr (has_serde_read<Type>) {
            t.serde_read(in, version, compat_version, size);
        } else {
            envelope_for_each_field(
              t, [&](auto& f) { f = read<std::decay_t<decltype(f)>>(in); });
        }
    } else if constexpr (std::is_same_v<Type, bool>) {
        t = read<int8_t>(in) != 0;
    } else if constexpr (std::is_scalar_v<Type> && !std::is_enum_v<Type>) {
        if (unlikely(in.bytes_left() < sizeof(Type))) {
            throw serde_exception{"message too short"};
        }

        if constexpr (sizeof(Type) == 1) {
            t = in.consume_type<Type>();
        } else {
            t = ss::le_to_cpu(in.consume_type<Type>());
        }
    } else if constexpr (reflection::is_std_vector_v<Type>) {
        using value_type = typename Type::value_type;
        t.resize(read<serde_size_t>(in));
        for (auto i = 0U; i < t.size(); ++i) {
            t[i] = read<value_type>(in);
        }
    } else if constexpr (reflection::is_named_type_v<Type>) {
        t = Type{read<typename Type::type>(in)};
    } else if constexpr (reflection::is_ss_bool_v<Type>) {
        t = Type{read<int8_t>(in) != 0};
    } else if constexpr (std::is_same_v<Type, std::chrono::milliseconds>) {
        t = std::chrono::milliseconds(read<int64_t>(in));
    } else if constexpr (std::is_same_v<Type, iobuf>) {
        return in.share(read<serde_size_t>(in));
    } else if constexpr (std::is_same_v<Type, ss::sstring>) {
        return in.read_string(read<serde_size_t>(in));
    } else if constexpr (reflection::is_std_optional_v<Type>) {
        return read<bool>(in) ? Type{read<typename Type::value_type>(in)}
                              : std::nullopt;
    }

    return t;
}

template<typename T>
ss::future<std::decay_t<T>> read_async(iobuf_parser& in) {
    using Type = std::decay_t<T>;
    if constexpr (has_serde_async_read<Type>) {
        auto const h = read_header<Type>(in);
        return ss::do_with(Type{}, [&in, h](Type& t) {
            auto const& [version, compat_version, size] = h;
            return t.serde_async_read(in, version, compat_version, size)
              .then([&t]() { return std::move(t); });
        });
    } else {
        return ss::make_ready_future<std::decay_t<T>>(read<T>(in));
    }
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

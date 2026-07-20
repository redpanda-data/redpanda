// Copyright 2026 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#pragma once

#include "bytes/details/io_allocation_size.h"
#include "bytes/iobuf.h"
#include "serde/envelope.h"
#include "serde/envelope_for_each_field.h"
#include "serde/rw/rw.h"
#include "serde/serde_exception.h"
#include "serde/serde_is_enum.h"
#include "serde/serde_size_t.h"
#include "serde/type_str.h"
#include "ssx/sformat.h"

#include <seastar/core/byteorder.hh>

#include <bit>
#include <concepts>
#include <cstring>
#include <limits>
#include <tuple>
#include <type_traits>

namespace serde {

/// Set to true when a custom writer changes the canonical encoding of a type
/// that would otherwise be eligible for fixed-width serialization.
template<typename T>
inline constexpr bool disable_fixed_serde_v = false;

namespace detail {

class fixed_writer {
public:
    explicit fixed_writer(char* cursor)
      : _cursor(cursor) {}

    template<std::integral T>
    void write(T value) {
        const auto encoded = ss::cpu_to_le(value);
        std::memcpy(_cursor, &encoded, sizeof(encoded));
        // NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-pointer-arithmetic)
        _cursor += sizeof(encoded);
    }

    void write(float value) { write(std::bit_cast<uint32_t>(value)); }
    void write(double value) { write(std::bit_cast<uint64_t>(value)); }

private:
    char* _cursor;
};

template<typename T>
struct fixed_serde_traits {
    static constexpr bool supported = false;
    static constexpr size_t size = 0;
    static constexpr bool requires_validation = false;
};

template<typename T>
inline constexpr bool fixed_serde_v
  = fixed_serde_traits<std::remove_cvref_t<T>>::supported;

template<typename T>
inline constexpr size_t fixed_serde_size_v
  = fixed_serde_traits<std::remove_cvref_t<T>>::size;

template<typename T>
requires(
  std::is_arithmetic_v<T> && !std::same_as<T, bool>
  && !has_nonmember_write_nested<T> && !disable_fixed_serde_v<T>)
struct fixed_serde_traits<T> {
    static constexpr bool supported = true;
    static constexpr size_t size = sizeof(T);
    static constexpr bool requires_validation = false;

    static void write(fixed_writer& out, T value) { out.write(value); }
};

template<>
struct fixed_serde_traits<bool> {
    static constexpr bool supported = true;
    static constexpr size_t size = sizeof(int8_t);
    static constexpr bool requires_validation = false;

    static void write(fixed_writer& out, bool value) {
        out.write(static_cast<int8_t>(value));
    }
};

template<typename T>
requires(
  serde_is_enum_v<T> && std::is_scoped_enum_v<T>
  && !has_nonmember_write_nested<T> && !disable_fixed_serde_v<T>)
struct fixed_serde_traits<T> {
    using underlying_type = std::underlying_type_t<T>;

    static constexpr bool supported = true;
    static constexpr size_t size = sizeof(serde_enum_serialized_t);
    static constexpr bool requires_validation
      = !std::in_range<serde_enum_serialized_t>(
          std::numeric_limits<underlying_type>::lowest())
        || !std::in_range<serde_enum_serialized_t>(
          std::numeric_limits<underlying_type>::max());

    static void validate(T value)
    requires requires_validation
    {
        const auto raw = static_cast<underlying_type>(value);
        if (unlikely(!std::in_range<serde_enum_serialized_t>(raw))) {
            throw serde_exception{fmt_with_ctx(
              ssx::sformat,
              "serde: enum of type {} has value {} which is out of bounds "
              "for serde_enum_serialized_t",
              type_str<T>(),
              raw)};
        }
    }

    static void write(fixed_writer& out, T value) {
        const auto raw = static_cast<underlying_type>(value);
        out.write(static_cast<serde_enum_serialized_t>(raw));
    }
};

template<typename Tuple, size_t... Index>
consteval bool fixed_tuple_supported(std::index_sequence<Index...>) {
    return (fixed_serde_v<std::tuple_element_t<Index, Tuple>> && ...);
}

template<typename Tuple>
inline constexpr bool fixed_tuple_supported_v = fixed_tuple_supported<Tuple>(
  std::make_index_sequence<std::tuple_size_v<Tuple>>{});

template<typename Tuple, size_t... Index>
consteval size_t fixed_tuple_size(std::index_sequence<Index...>) {
    return (fixed_serde_size_v<std::tuple_element_t<Index, Tuple>> + ... + 0);
}

template<typename Tuple>
inline constexpr size_t fixed_tuple_size_v = fixed_tuple_size<Tuple>(
  std::make_index_sequence<std::tuple_size_v<Tuple>>{});

template<typename Tuple, size_t... Index>
consteval bool fixed_tuple_requires_validation(std::index_sequence<Index...>) {
    return (
      fixed_serde_traits<std::remove_cvref_t<
        std::tuple_element_t<Index, Tuple>>>::requires_validation
      || ...);
}

template<typename Tuple>
inline constexpr bool fixed_tuple_requires_validation_v
  = fixed_tuple_requires_validation<Tuple>(
    std::make_index_sequence<std::tuple_size_v<Tuple>>{});

template<typename T>
concept has_fixed_serde_fields = requires(T value) { value.serde_fields(); };

template<typename T>
concept has_custom_serde_write = requires(T value, iobuf& out) {
    value.serde_write(out);
};

template<typename T>
requires(
  is_envelope<T> && !is_checksum_envelope<T> && has_fixed_serde_fields<T>
  && !has_custom_serde_write<T> && !has_nonmember_write_nested<T>
  && !disable_fixed_serde_v<T>)
struct fixed_serde_traits<T> {
    using fields_type = decltype(envelope_to_tuple(std::declval<const T&>()));

    static constexpr size_t body_size = fixed_tuple_size_v<fields_type>;
    static constexpr size_t size = 2 * sizeof(version_t) + sizeof(serde_size_t)
                                   + body_size;
    static constexpr bool supported
      = fixed_tuple_supported_v<fields_type>
        && body_size <= std::numeric_limits<serde_size_t>::max()
        && size <= ::details::io_allocation_size::ss_max_small_allocation;
    static constexpr bool requires_validation
      = fixed_tuple_requires_validation_v<fields_type>;

    static void validate(const T& value)
    requires(supported && requires_validation)
    {
        std::apply(
          [](const auto&... field) {
              (
                []<typename Field>(const Field& field) {
                    if constexpr (
                      fixed_serde_traits<Field>::requires_validation) {
                        fixed_serde_traits<Field>::validate(field);
                    }
                }(field),
                ...);
          },
          envelope_to_tuple(value));
    }

    static void write(fixed_writer& out, const T& value)
    requires supported
    {
        out.write(T::redpanda_serde_version);
        out.write(T::redpanda_serde_compat_version);
        out.write(static_cast<serde_size_t>(body_size));
        std::apply(
          [&out](const auto&... field) {
              (fixed_serde_traits<std::remove_cvref_t<decltype(field)>>::write(
                 out, field),
               ...);
          },
          envelope_to_tuple(value));
    }
};

template<typename T>
requires fixed_serde_v<T>
void write_fixed(iobuf& out, const T& value) {
    if constexpr (fixed_serde_traits<T>::requires_validation) {
        fixed_serde_traits<T>::validate(value);
    }
    auto storage = out.reserve(fixed_serde_size_v<T>);
    fixed_writer writer(storage.mutable_index());
    fixed_serde_traits<std::remove_cvref_t<T>>::write(writer, value);
}

} // namespace detail

} // namespace serde

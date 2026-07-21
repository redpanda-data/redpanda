// Copyright 2023 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

// IWYU pragma: always_keep; symbols for ADL
#pragma once

#include "hashing/crc32c.h"
#include "serde/checksum_t.h"
#include "serde/envelope.h"
#include "serde/envelope_for_each_field.h"
#include "serde/read_header.h"
#include "serde/rw/fixed.h"
#include "serde/rw/rw.h"
#include "serde/serde_size_t.h"

#include <type_traits>

namespace serde {

template<typename T>
concept has_serde_write = requires(T t, iobuf& out) { t.serde_write(out); };

template<typename T>
concept has_serde_read = requires(T t, iobuf_parser& in, const header& h) {
    t.serde_read(in, h);
};

template<typename T>
concept has_serde_fields = requires(T t) { t.serde_fields(); };

template<typename T>
requires is_envelope<std::decay_t<T>>
void tag_invoke(
  tag_t<read_tag>, iobuf_parser& in, T& t, const std::size_t bytes_left_limit) {
    using Type = std::decay_t<T>;

    const auto h = read_header<Type>(in, bytes_left_limit);

    if constexpr (is_checksum_envelope<Type>) {
        const auto shared = in.share_no_consume(
          in.bytes_left() - h._bytes_left_limit);
        auto read_only_in = iobuf_const_parser{shared};
        auto crc = crc::crc32c{};
        read_only_in.consume(
          read_only_in.bytes_left(),
          [&crc](const char* src, const std::size_t n) {
              crc.extend(src, n);
              return ss::stop_iteration::no;
          });
        if (unlikely(crc.value() != h._checksum)) {
            throw serde_exception(fmt_with_ctx(
              ssx::sformat,
              "serde: envelope {} (ends at bytes_left={}) has bad "
              "checksum: stored={}, actual={}",
              type_str<Type>(),
              h._bytes_left_limit,
              h._checksum,
              crc.value()));
        }
    }

    if constexpr (has_serde_read<Type>) {
        static_assert(!has_serde_fields<Type>);
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
}

namespace detail {

[[gnu::always_inline]] inline crc::crc32c
envelope_body_crc(iobuf& out, size_t body_offset) {
    auto crc = crc::crc32c{};
    auto in = iobuf_const_parser{out};
    in.skip(body_offset);
    in.consume(in.bytes_left(), [&crc](const char* src, const std::size_t n) {
        crc.extend(src, n);
        return ss::stop_iteration::no;
    });
    return crc;
}

[[gnu::always_inline]] inline crc::crc32c
envelope_body_crc(contiguous_output& out, size_t body_offset) {
    const auto body = out.written_from(body_offset);
    auto crc = crc::crc32c{};
    crc.extend(body.data(), body.size());
    return crc;
}

template<typename Type, SerdeWriteOutput Output, typename Func>
[[gnu::always_inline]] inline void
write_sized_envelope(Output& out, Func&& write_body) {
    auto size_placeholder = [&out] {
        if constexpr (fixed_serde_v<Type>) {
            write(
              out,
              static_cast<serde_size_t>(fixed_serde_traits<Type>::body_size));
            return typename Output::placeholder{};
        } else {
            static_assert(std::same_as<Output, iobuf>);
            return out.reserve(sizeof(serde_size_t));
        }
    }();

    auto checksum_placeholder = [&] {
        if constexpr (is_checksum_envelope<Type>) {
            return out.reserve(sizeof(checksum_t));
        } else {
            return typename Output::placeholder{};
        }
    }();

    const auto body_offset = out.size_bytes();
    std::forward<Func>(write_body)();

    if constexpr (!fixed_serde_v<Type>) {
        const auto written_size = out.size_bytes() - body_offset;
        if (unlikely(written_size > std::numeric_limits<serde_size_t>::max())) {
            throw serde_exception("envelope too big");
        }
        const auto size = ss::cpu_to_le(
          static_cast<serde_size_t>(written_size));
        size_placeholder.write(
          reinterpret_cast<const char*>(&size), sizeof(serde_size_t));
    }

    if constexpr (is_checksum_envelope<Type>) {
        const auto crc = envelope_body_crc(out, body_offset);
        const auto checksum = ss::cpu_to_le(crc.value());
        static_assert(
          std::is_same_v<std::decay_t<decltype(checksum)>, checksum_t>);
        checksum_placeholder.write(
          reinterpret_cast<const char*>(&checksum), sizeof(checksum_t));
    }
}

template<typename Type, SerdeWriteOutput Output, typename T>
[[gnu::always_inline]] inline void write_envelope(Output& out, T&& value) {
    write(out, Type::redpanda_serde_version);
    write(out, Type::redpanda_serde_compat_version);
    write_sized_envelope<Type>(out, [&out, &value] {
        if constexpr (has_serde_write<Type>) {
            static_assert(!has_serde_fields<Type>);
            value.serde_write(out);
        } else {
            envelope_for_each_field(value, [&out](auto& field) {
                write(out, std::forward_like<T>(field));
            });
        }
    });
}

template<typename Type, typename T>
[[gnu::always_inline]] inline void
with_serialization_output(iobuf& out, T&& value) {
    // Reserve fixed envelopes once at the iobuf boundary. Nested serialization
    // then uses the contiguous cursor and avoids per-field fragment checks.
    if constexpr (fixed_serde_v<Type>) {
        if constexpr (fixed_serde_traits<Type>::requires_validation) {
            fixed_serde_traits<Type>::validate(value);
        }
        auto storage = out.reserve(fixed_serde_size_v<Type>);
        contiguous_output writer(storage.mutable_index());
        write_envelope<Type>(writer, std::forward<T>(value));
    } else {
        write_envelope<Type>(out, std::forward<T>(value));
    }
}

template<typename Type, typename T>
[[gnu::always_inline]] inline void
with_serialization_output(contiguous_output& out, T&& value) {
    write_envelope<Type>(out, std::forward<T>(value));
}

} // namespace detail

template<SerdeWriteOutput Output, typename T>
requires(
  is_envelope<std::decay_t<T>>
  && (std::same_as<Output, iobuf> || detail::fixed_serde_v<std::decay_t<T>>))
[[gnu::always_inline]] inline void
tag_invoke(tag_t<write_tag>, Output& out, T&& t) {
    using Type = std::decay_t<T>;

    detail::with_serialization_output<Type>(out, std::forward<T>(t));
}

} // namespace serde

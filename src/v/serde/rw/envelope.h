// Copyright 2023 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#pragma once

#include "hashing/crc32c.h"
#include "serde/checksum_t.h"
#include "serde/envelope.h"
#include "serde/envelope_for_each_field.h"
#include "serde/read_header.h"
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

template<typename T>
requires is_envelope<std::decay_t<T>>
void tag_invoke(tag_t<write_tag>, iobuf& out, T t) {
    using Type = std::decay_t<T>;

    write(out, Type::redpanda_serde_version);
    write(out, Type::redpanda_serde_compat_version);

    auto size_placeholder = out.reserve(sizeof(serde_size_t));

    auto checksum_placeholder = iobuf::placeholder{};
    if constexpr (is_checksum_envelope<Type>) {
        checksum_placeholder = out.reserve(sizeof(checksum_t));
    }

    const auto size_before = out.size_bytes();
    if constexpr (has_serde_write<Type>) {
        static_assert(!has_serde_fields<Type>);
        t.serde_write(out);
    } else {
        envelope_for_each_field(
          t, [&out](auto& f) { write(out, std::move(f)); });
    }

    const auto written_size = out.size_bytes() - size_before;
    if (unlikely(written_size > std::numeric_limits<serde_size_t>::max())) {
        throw serde_exception("envelope too big");
    }
    const auto size = ss::cpu_to_le(static_cast<serde_size_t>(written_size));
    size_placeholder.write(
      reinterpret_cast<const char*>(&size), sizeof(serde_size_t));

    if constexpr (is_checksum_envelope<Type>) {
        auto crc = crc::crc32c{};
        auto in = iobuf_const_parser{out};
        in.skip(size_before);
        in.consume(
          in.bytes_left(), [&crc](const char* src, const std::size_t n) {
              crc.extend(src, n);
              return ss::stop_iteration::no;
          });
        const auto checksum = ss::cpu_to_le(crc.value());
        static_assert(
          std::is_same_v<std::decay_t<decltype(checksum)>, checksum_t>);
        checksum_placeholder.write(
          reinterpret_cast<const char*>(&checksum), sizeof(checksum_t));
    }
}

} // namespace serde

// Copyright 2023 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#pragma once

#include "base/likely.h"
#include "base/vlog.h"
#include "bytes/iobuf_parser.h"
#include "hashing/crc32c.h"
#include "serde/read_header.h"
#include "serde/rw/rw.h"
#include "ssx/sformat.h"

#include <seastar/coroutine/maybe_yield.hh>

namespace serde {

template<typename T>
concept has_serde_async_read = requires(T t, iobuf_parser& in, header h) {
    { t.serde_async_read(in, h) } -> seastar::Future;
};

template<typename T>
concept has_serde_async_write = requires(T t, iobuf& out) {
    { t.serde_async_write(out) } -> seastar::Future;
};

template<typename T>
concept has_serde_async_direct_read = requires(iobuf_parser& in, header h) {
    { T::serde_async_direct_read(in, h) } -> seastar::Future;
};

inline ss::future<crc::crc32c> calculate_crc_async(iobuf_const_parser in) {
    crc::crc32c checksum;
    while (in.bytes_left() > 0) {
        in.consume(
          in.bytes_left(), [&checksum](const char* src, const size_t size) {
              checksum.extend(src, size);
              return ss::need_preempt() ? ss::stop_iteration::yes
                                        : ss::stop_iteration::no;
          });
        if (in.bytes_left() > 0) {
            co_await ss::coroutine::maybe_yield();
        }
    }
    co_return checksum;
}

namespace detail {

template<typename Type>
ss::future<Type> read_async_nested_impl(
  iobuf_parser& in, header h, std::optional<iobuf> shared) {
    if constexpr (is_checksum_envelope<Type>) {
        auto checksum = co_await calculate_crc_async(
          iobuf_const_parser{std::move(*shared)});
        if (unlikely(checksum.value() != h._checksum)) {
            throw serde_exception(fmt_with_ctx(
              ssx::sformat,
              "serde: envelope {} (ends at bytes_left={}) has bad checksum: "
              "stored={}, actual={}",
              type_str<Type>(),
              h._bytes_left_limit,
              h._checksum,
              checksum.value()));
        }
    }

    if constexpr (has_serde_async_direct_read<Type>) {
        co_return co_await Type::serde_async_direct_read(in, h);
    } else {
        Type result;
        co_await result.serde_async_read(in, h);
        co_return result;
    }
}

template<typename Type>
ss::future<> write_async_envelope(
  iobuf& out,
  Type value,
  size_t size_before,
  iobuf::placeholder size_placeholder,
  iobuf::placeholder checksum_placeholder) {
    co_await value.serde_async_write(out);
    const auto written_size = out.size_bytes() - size_before;
    if (unlikely(written_size > std::numeric_limits<serde_size_t>::max())) {
        throw serde_exception{"envelope too big"};
    }
    const auto size = ss::cpu_to_le(static_cast<serde_size_t>(written_size));
    size_placeholder.write(
      reinterpret_cast<const char*>(&size), sizeof(serde_size_t));

    if constexpr (is_checksum_envelope<Type>) {
        auto in = iobuf_const_parser{out};
        in.skip(size_before);
        auto crc = co_await calculate_crc_async(std::move(in));
        const auto checksum = ss::cpu_to_le(crc.value());
        static_assert(
          std::is_same_v<std::decay_t<decltype(checksum)>, checksum_t>);
        checksum_placeholder.write(
          reinterpret_cast<const char*>(&checksum), sizeof(checksum_t));
    }
}

} // namespace detail

template<typename T>
ss::future<std::decay_t<T>>
read_async_nested(iobuf_parser& in, const size_t bytes_left_limit) {
    using Type = std::decay_t<T>;
    if constexpr (
      has_serde_async_direct_read<Type> || has_serde_async_read<Type>) {
        const auto h = read_header<Type>(in, bytes_left_limit);
        std::optional<iobuf> shared;
        if constexpr (is_checksum_envelope<Type>) {
            shared.emplace(
              in.share_no_consume(in.bytes_left() - h._bytes_left_limit));
        }
        co_return co_await detail::read_async_nested_impl<Type>(
          in, h, std::move(shared));
    } else {
        co_return read_nested<T>(in, bytes_left_limit);
    }
}

template<typename T>
ss::future<std::decay_t<T>> read_async(iobuf_parser& in) {
    using Type = std::decay_t<T>;
    if constexpr (
      has_serde_async_direct_read<Type> || has_serde_async_read<Type>) {
        auto value = co_await read_async_nested<T>(in, 0);
        if (unlikely(in.bytes_left() != 0)) {
            throw serde_exception{fmt_with_ctx(
              ssx::sformat,
              "serde: not all bytes consumed after read_async<{}>(), "
              "bytes_left={}",
              type_str<Type>(),
              in.bytes_left())};
        }
        co_return value;
    } else {
        auto value = read_nested<T>(in, 0);
        if (likely(in.bytes_left() == 0)) {
            co_return value;
        }
        throw serde_exception{fmt_with_ctx(
          ssx::sformat,
          "serde: not all bytes consumed after read_async<{}>(), bytes_left={}",
          type_str<T>(),
          in.bytes_left())};
    }
}

template<typename T>
ss::future<> write_async(iobuf& out, T t) {
    using Type = std::decay_t<T>;
    if constexpr (is_envelope<Type> && has_serde_async_write<Type>) {
        write(out, Type::redpanda_serde_version);
        write(out, Type::redpanda_serde_compat_version);

        auto size_placeholder = out.reserve(sizeof(serde_size_t));

        auto checksum_placeholder = iobuf::placeholder{};
        if constexpr (is_checksum_envelope<Type>) {
            checksum_placeholder = out.reserve(sizeof(checksum_t));
        }

        const auto size_before = out.size_bytes();

        co_await detail::write_async_envelope<Type>(
          out,
          std::move(t),
          size_before,
          std::move(size_placeholder),
          std::move(checksum_placeholder));
    } else {
        write(out, std::move(t));
    }
}

} // namespace serde

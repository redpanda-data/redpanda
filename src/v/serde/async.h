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
#include "serde/logger.h"
#include "serde/read_header.h"
#include "serde/rw/rw.h"
#include "ssx/future-util.h"
#include "ssx/sformat.h"

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

// TODO: coroutinize async functions after we switch to clang 16 (see
// https://github.com/llvm/llvm-project/issues/49689)

inline ss::future<crc::crc32c> calculate_crc_async(iobuf_const_parser in) {
    return ss::do_with(
      crc::crc32c{},
      std::move(in),
      [](crc::crc32c& crc, iobuf_const_parser& in) {
          return ss::do_until(
                   [&in] { return in.bytes_left() == 0; },
                   [&in, &crc] {
                       in.consume(
                         in.bytes_left(),
                         [&crc](const char* src, const size_t n) {
                             crc.extend(src, n);
                             return (
                               ss::need_preempt() ? ss::stop_iteration::yes
                                                  : ss::stop_iteration::no);
                         });
                       return ss::now();
                   })
            .then([&crc] { return crc; });
      });
}

template<typename T>
ss::future<std::decay_t<T>>
read_async_nested(iobuf_parser& in, const size_t bytes_left_limit) {
    using Type = std::decay_t<T>;
    if constexpr (
      has_serde_async_direct_read<Type> || has_serde_async_read<Type>) {
        const auto h = read_header<Type>(in, bytes_left_limit);
        auto f = ss::now();
        if constexpr (is_checksum_envelope<Type>) {
            auto shared = in.share_no_consume(
              in.bytes_left() - h._bytes_left_limit);
            f = ss::do_with(std::move(shared), [h](const iobuf& shared) {
                return calculate_crc_async(iobuf_const_parser{shared})
                  .then([h](const crc::crc32c crc) {
                      if (unlikely(crc.value() != h._checksum)) {
                          throw serde_exception(fmt_with_ctx(
                            ssx::sformat,
                            "serde: envelope {} (ends at bytes_left={}) has "
                            "bad checksum: stored={}, actual={}",
                            type_str<Type>(),
                            h._bytes_left_limit,
                            h._checksum,
                            crc.value()));
                      }
                  });
            });
        }

        if constexpr (has_serde_async_direct_read<Type>) {
            return f.then(
              [&in, h] { return Type::serde_async_direct_read(in, h); });
        } else if constexpr (has_serde_async_read<Type>) {
            return f.then([&in, h] {
                return ss::do_with(Type{}, [&in, h](Type& t) {
                    return t.serde_async_read(in, h).then(
                      [&t]() { return std::move(t); });
                });
            });
        }
    } else {
        return ss::make_ready_future<std::decay_t<T>>(
          read_nested<T>(in, bytes_left_limit));
    }
}

template<typename T>
ss::future<std::decay_t<T>> read_async(iobuf_parser& in) {
    return read_async_nested<T>(in, 0).then([&](std::decay_t<T>&& t) {
        if (likely(in.bytes_left() == 0)) {
            return ss::make_ready_future<std::decay_t<T>>(std::move(t));
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

        return ss::do_with(
          std::move(t),
          [&out,
           size_before,
           size_placeholder = std::move(size_placeholder),
           checksum_placeholder = std::move(checksum_placeholder)](
            T& t) mutable {
              return t.serde_async_write(out).then(
                [&out,
                 size_before,
                 size_placeholder = std::move(size_placeholder),
                 checksum_placeholder = std::move(
                   checksum_placeholder)]() mutable {
                    const auto written_size = out.size_bytes() - size_before;
                    if (unlikely(
                          written_size
                          > std::numeric_limits<serde_size_t>::max())) {
                        throw serde_exception{"envelope too big"};
                    }
                    const auto size = ss::cpu_to_le(
                      static_cast<serde_size_t>(written_size));
                    size_placeholder.write(
                      reinterpret_cast<const char*>(&size),
                      sizeof(serde_size_t));

                    if constexpr (is_checksum_envelope<Type>) {
                        auto in = iobuf_const_parser{out};
                        in.skip(size_before);
                        return calculate_crc_async(std::move(in))
                          .then([checksum_placeholder = std::move(
                                   checksum_placeholder)](
                                  const crc::crc32c crc) mutable {
                              const auto checksum = ss::cpu_to_le(crc.value());
                              static_assert(std::is_same_v<
                                            std::decay_t<decltype(checksum)>,
                                            checksum_t>);
                              checksum_placeholder.write(
                                reinterpret_cast<const char*>(&checksum),
                                sizeof(checksum_t));
                          });
                    } else {
                        return ss::now();
                    }
                });
          });
    } else {
        write(out, std::move(t));
        return ss::make_ready_future<>();
    }
}

} // namespace serde

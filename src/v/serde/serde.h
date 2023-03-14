
// Copyright 2021 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#pragma once

#include "bytes/iobuf_parser.h"
#include "hashing/crc32c.h"
#include "likely.h"
#include "reflection/type_traits.h"
#include "serde/checksum_t.h"
#include "serde/envelope_for_each_field.h"
#include "serde/logger.h"
#include "serde/read_header.h"
#include "serde/serde_exception.h"
#include "serde/serde_is_enum.h"
#include "serde/serde_size_t.h"
#include "serde/type_str.h"
#include "ssx/future-util.h"
#include "ssx/sformat.h"
#include "tristate.h"
#include "utils/fragmented_vector.h"
#include "utils/named_type.h"
#include "utils/uuid.h"
#include "vlog.h"

#include <seastar/core/future.hh>
#include <seastar/net/inet_address.hh>

#include <absl/container/btree_map.h>
#include <absl/container/btree_set.h>
#include <absl/container/flat_hash_map.h>
#include <absl/container/flat_hash_set.h>
#include <absl/container/node_hash_map.h>
#include <absl/container/node_hash_set.h>

#include <chrono>
#include <iosfwd>
#include <numeric>
#include <ratio>
#include <string>
#include <string_view>
#include <type_traits>

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
concept has_serde_direct_read = requires(iobuf_parser& in, const header& h) {
    { T::serde_direct_read(in, h) };
};

template<typename T>
concept has_serde_async_direct_read = requires(iobuf_parser& in, header h) {
    { T::serde_async_direct_read(in, h) } -> seastar::Future;
};

template<typename T>
concept is_absl_btree_map
  = ::detail::is_specialization_of_v<T, absl::btree_map>;

template<typename T>
concept is_absl_flat_hash_map
  = ::detail::is_specialization_of_v<T, absl::flat_hash_map>;

template<typename T>
concept is_absl_btree_set
  = ::detail::is_specialization_of_v<T, absl::btree_set>;

template<typename T>
concept is_absl_flat_hash_set
  = ::detail::is_specialization_of_v<T, absl::flat_hash_set>;

using serde_enum_serialized_t = int32_t;

namespace detail {

template<class T, template<class, size_t> class C>
struct is_specialization_of_sized : std::false_type {};
template<template<class, size_t> class C, class T, size_t N>
struct is_specialization_of_sized<C<T, N>, C> : std::true_type {};
template<typename T, template<class, size_t> class C>
inline constexpr bool is_specialization_of_sized_v
  = is_specialization_of_sized<T, C>::value;

} // namespace detail

template<typename T>
concept is_fragmented_vector
  = detail::is_specialization_of_sized_v<T, fragmented_vector>;

template<typename T>
concept is_chunked_fifo
  = detail::is_specialization_of_sized_v<T, ss::chunked_fifo>;
template<typename T>
concept is_std_unordered_map
  = ::detail::is_specialization_of_v<T, std::unordered_map>;

template<typename T>
concept is_absl_node_hash_set
  = ::detail::is_specialization_of_v<T, absl::node_hash_set>;

template<typename T>
concept is_absl_node_hash_map
  = ::detail::is_specialization_of_v<T, absl::node_hash_map>;

template<class T>
concept is_chrono_duration
  = ::detail::is_specialization_of_v<T, std::chrono::duration>;

template<class T>
concept is_chrono_time_point
  = ::detail::is_specialization_of_v<T, std::chrono::time_point>;

template<typename T>
inline constexpr auto const is_serde_compatible_v
  = is_envelope<T>
    || (std::is_scalar_v<T>  //
         && (!std::is_same_v<float, T> || std::numeric_limits<float>::is_iec559)
         && (!std::is_same_v<double, T> || std::numeric_limits<double>::is_iec559)
         && (!serde_is_enum_v<T> || sizeof(std::decay_t<T>) <= sizeof(serde_enum_serialized_t)))
    || reflection::is_std_vector<T>
    || reflection::is_std_array<T>
    || reflection::is_rp_named_type<T>
    || reflection::is_ss_bool_class<T>
    || reflection::is_std_optional<T>
    || std::is_same_v<T, std::chrono::milliseconds>
    || std::is_same_v<T, iobuf>
    || std::is_same_v<T, ss::sstring>
    || std::is_same_v<T, bytes>
    || std::is_same_v<T, uuid_t>
    || is_absl_btree_set<T>
    || is_absl_btree_map<T>
    || is_absl_flat_hash_set<T>
    || is_absl_flat_hash_map<T>
    || is_absl_node_hash_set<T>
    || is_absl_node_hash_map<T>
    || is_chrono_duration<T>
    || is_std_unordered_map<T>
    || is_fragmented_vector<T> || is_chunked_fifo<T> || reflection::is_tristate<T> || std::is_same_v<T, ss::net::inet_address>;

template<typename T>
void write(iobuf& out, std::optional<T> t);

template<typename T>
void write(iobuf& out, std::optional<T> t) {
    if (t) {
        write(out, true);
        write(out, std::move(t.value()));
    } else {
        write(out, false);
    }
}

template<typename T>
void read_nested(iobuf_parser& in, T& t, std::size_t const bytes_left_limit) {
    using Type = std::decay_t<T>;
    static_assert(has_serde_read<T> || is_serde_compatible_v<Type>);

    if constexpr (reflection::is_std_optional<Type>) {
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
    static_assert(
      std::is_default_constructible_v<T> || has_serde_direct_read<T>);
    if constexpr (has_serde_direct_read<T>) {
        auto const h = read_header<Type>(in, bytes_left_limit);
        return Type::serde_direct_read(in, h);
    } else {
        auto t = Type();
        read_nested(in, t, bytes_left_limit);
        return t;
    }
}

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
                         [&crc](char const* src, size_t const n) {
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
read_async_nested(iobuf_parser& in, size_t const bytes_left_limit) {
    using Type = std::decay_t<T>;
    if constexpr (
      has_serde_async_direct_read<Type> || has_serde_async_read<Type>) {
        auto const h = read_header<Type>(in, bytes_left_limit);
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

        auto const size_before = out.size_bytes();

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
                    auto const written_size = out.size_bytes() - size_before;
                    if (unlikely(
                          written_size
                          > std::numeric_limits<serde_size_t>::max())) {
                        throw serde_exception{"envelope too big"};
                    }
                    auto const size = ss::cpu_to_le(
                      static_cast<serde_size_t>(written_size));
                    size_placeholder.write(
                      reinterpret_cast<char const*>(&size),
                      sizeof(serde_size_t));

                    if constexpr (is_checksum_envelope<Type>) {
                        auto in = iobuf_const_parser{out};
                        in.skip(size_before);
                        return calculate_crc_async(std::move(in))
                          .then([checksum_placeholder = std::move(
                                   checksum_placeholder)](
                                  const crc::crc32c crc) mutable {
                              auto const checksum = ss::cpu_to_le(crc.value());
                              static_assert(std::is_same_v<
                                            std::decay_t<decltype(checksum)>,
                                            checksum_t>);
                              checksum_placeholder.write(
                                reinterpret_cast<char const*>(&checksum),
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

inline version_t peek_version(iobuf_parser& in) {
    if (unlikely(in.bytes_left() < sizeof(serde::version_t))) {
        throw serde_exception{"cannot peek version"};
    }
    auto version_reader = iobuf_parser{in.peek(sizeof(serde::version_t))};
    return serde::read_nested<serde::version_t>(version_reader, 0);
}

inline serde::serde_size_t peek_body_size(iobuf_parser& in) {
    if (unlikely(in.bytes_left() < envelope_header_size)) {
        throw serde_exception{"cannot peek size"};
    }

    // Take bytes 2-6
    auto header_buf = in.peek(envelope_header_size);
    header_buf.trim_front(2);
    auto size_reader = iobuf_parser{std::move(header_buf)};
    return serde::read_nested<serde::serde_size_t>(size_reader, 0);
}

} // namespace serde
#include "serde/rw/array.h"
#include "serde/rw/bool_class.h"
#include "serde/rw/chrono.h"
#include "serde/rw/enum.h"
#include "serde/rw/envelope.h"
#include "serde/rw/inet_address.h"
#include "serde/rw/iobuf.h"
#include "serde/rw/map.h"
#include "serde/rw/named_type.h"
#include "serde/rw/scalar.h"
#include "serde/rw/set.h"
#include "serde/rw/sstring.h"
#include "serde/rw/tristate_rw.h"
#include "serde/rw/uuid.h"
#include "serde/rw/vector.h"

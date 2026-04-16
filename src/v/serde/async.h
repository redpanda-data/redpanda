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
#include "serde/rw/variant.h"
#include "ssx/future-util.h"
#include "ssx/sformat.h"

#include <seastar/core/do_with.hh>
#include <seastar/core/future.hh>
#include <seastar/core/loop.hh>

#include <ranges>
#include <type_traits>
#include <utility>

namespace serde {

template<typename T>
concept is_serde_variant = requires {
    []<typename... Ts>(variant<Ts...>) {
    }(std::declval<std::remove_cvref_t<T>>());
};

template<typename T>
concept is_pair = requires {
    []<typename A, typename B>(std::pair<A, B>) {
    }(std::declval<std::remove_cvref_t<T>>());
};

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

namespace detail {
template<typename V, size_t... Is>
ss::future<V> read_variant_async_impl(
  iobuf_parser& in,
  size_t bytes_left_limit,
  size_t index,
  std::index_sequence<Is...>);
} // namespace detail

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
    } else if constexpr (is_pair<Type>) {
        return read_async_nested<typename Type::first_type>(
                 in, bytes_left_limit)
          .then([&in, bytes_left_limit](auto first) {
              return read_async_nested<typename Type::second_type>(
                       in, bytes_left_limit)
                .then([first = std::move(first)](auto second) mutable {
                    return Type{std::move(first), std::move(second)};
                });
          });
    } else if constexpr (is_serde_variant<Type>) {
        auto size = read_nested<size_t>(in, bytes_left_limit);
        auto index = read_nested<size_t>(in, bytes_left_limit);
        if (size != std::variant_size_v<typename Type::type>) [[unlikely]] {
            throw serde_exception(fmt_with_ctx(
              ssx::sformat,
              "reading type {} of size {}: {} bytes left - unexpected variant "
              "size: {}, current variant size: {}, likely backwards compat "
              "issues.",
              type_str<Type>(),
              sizeof(Type),
              in.bytes_left(),
              size,
              std::variant_size_v<typename Type::type>));
        }
        if (index >= std::variant_size_v<typename Type::type>) [[unlikely]] {
            throw serde_exception(fmt_with_ctx(
              ssx::sformat,
              "reading type {} of size {}: {} bytes left - unexpected variant "
              "index: {}, variant size: {}",
              type_str<Type>(),
              sizeof(Type),
              in.bytes_left(),
              index,
              std::variant_size_v<typename Type::type>));
        }
        return detail::read_variant_async_impl<Type>(
          in,
          bytes_left_limit,
          index,
          std::make_index_sequence<std::variant_size_v<typename Type::type>>{});
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
ss::future<> write_async(iobuf& out, T&& t) {
    using Type = std::remove_cvref_t<T>;
    if constexpr (is_envelope<Type> && has_serde_async_write<Type>) {
        write(out, Type::redpanda_serde_version);
        write(out, Type::redpanda_serde_compat_version);

        auto size_placeholder = out.reserve(sizeof(serde_size_t));

        auto checksum_placeholder = iobuf::placeholder{};
        if constexpr (is_checksum_envelope<Type>) {
            checksum_placeholder = out.reserve(sizeof(checksum_t));
        }

        const auto size_before = out.size_bytes();

        return ssx::do_with_fwd(
          std::forward<T>(t),
          [&out,
           size_before,
           size_placeholder = std::move(size_placeholder),
           checksum_placeholder = std::move(checksum_placeholder)](
            Type& t) mutable {
              return t.serde_async_write(out).then(
                [&out,
                 size_before,
                 size_placeholder = std::move(size_placeholder),
                 checksum_placeholder = std::move(
                   checksum_placeholder)]() mutable {
                    const auto written_size = out.size_bytes() - size_before;
                    if (
                      unlikely(
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
                        std::ignore = checksum_placeholder;
                        return ss::now();
                    }
                });
          });
    } else if constexpr (is_pair<Type>) {
        return ssx::do_with_fwd(std::forward<T>(t), [&out](auto& p) {
            return write_async(out, std::forward_like<T>(p.first))
              .then([&out, &p] {
                  return write_async(out, std::forward_like<T>(p.second));
              });
        });
    } else if constexpr (is_serde_variant<Type>) {
        write<size_t>(out, std::variant_size_v<typename Type::type>);
        write<size_t>(out, t.index());
        return ssx::do_with_fwd(std::forward<T>(t), [&out](auto& v) {
            return std::visit(
              [&out](auto& item) {
                  return write_async(out, std::forward_like<T>(item));
              },
              v);
        });
    } else {
        write(out, std::forward<T>(t));
        return ss::make_ready_future<>();
    }
}

namespace detail {

template<typename V, size_t... Is>
ss::future<V> read_variant_async_impl(
  iobuf_parser& in,
  size_t bytes_left_limit,
  size_t index,
  std::index_sequence<Is...>) {
    using func_t = ss::future<V> (*)(iobuf_parser&, size_t);
    static constexpr std::array<func_t, sizeof...(Is)> table = {
      [](iobuf_parser& p, size_t bll) {
          return read_async_nested<
                   std::variant_alternative_t<Is, typename V::type>>(p, bll)
            .then([](auto val) {
                return V{std::in_place_index<Is>, std::move(val)};
            });
      }...};
    return table[index](in, bytes_left_limit);
}

} // namespace detail

/// Async write for vector-like containers. Call explicitly — not dispatched
/// automatically from write_async to limit asynchronicity depth.
template<typename V>
ss::future<> write_async_vector(iobuf& out, V&& v) {
    write(out, static_cast<serde_size_t>(v.size()));
    return ssx::do_with_fwd(std::forward<V>(v), [&out](auto& v) {
        return ss::do_for_each(
          v, [&out](auto& el) { return write_async(out, el); });
    });
}

/// Async read for vector-like containers.
template<typename V>
ss::future<V> read_async_vector(iobuf_parser& in, size_t bytes_left_limit) {
    auto sz = read_nested<serde_size_t>(in, bytes_left_limit);
    return ss::do_with(
      V{},
      std::views::iota(serde_size_t{0}, sz),
      [&in, bytes_left_limit](V& result, auto& range) {
          result.reserve(std::ranges::size(range));
          return ss::do_for_each(
                   range,
                   [&in, bytes_left_limit, &result](serde_size_t) {
                       return read_async_nested<typename V::value_type>(
                                in, bytes_left_limit)
                         .then([&result](auto val) {
                             result.push_back(std::move(val));
                         });
                   })
            .then([&result] { return std::move(result); });
      });
}

} // namespace serde


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
concept has_serde_read = requires(T t, iobuf_parser& in, const header& h) {
    t.serde_read(in, h);
};

template<typename T>
concept has_serde_write = requires(T t, iobuf& out) { t.serde_write(out); };

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
inline constexpr auto const are_bytes_and_string_different = !(
  std::is_same_v<T, ss::sstring> && std::is_same_v<T, bytes>);

template<class R, class P>
int64_t checked_duration_cast_to_nanoseconds(
  const std::chrono::duration<R, P>& duration) {
    static_assert(
      __has_builtin(__builtin_mul_overflow),
      "__builtin_mul_overflow not supported.");
    using nano_period = std::chrono::nanoseconds::period;
    using nano_rep = typename std::chrono::nanoseconds::rep;
    // This is how duration_cast determines the output type.
    using output_type = typename std::common_type<R, nano_rep, intmax_t>::type;
    using ratio = std::ratio_divide<P, nano_period>;

    static_assert(
      std::is_same_v<output_type, nano_rep>,
      "Output type is not the same rep as std::chrono::nanoseconds");

    // Extra check to ensure the output type is same as the underlying type
    // supported in lib[std]c++.
    static_assert(
      std::is_same_v<output_type, int64_t>
        || std::is_same_v<output_type, long long>,
      "Output type not in supported integer types.");

    constexpr auto ratio_num = static_cast<output_type>(ratio::num);
    const auto dur = static_cast<output_type>(duration.count());
    output_type mul;
    if (unlikely(__builtin_mul_overflow(dur, ratio_num, &mul))) {
        // Clamp the value.
        // Log here to ensure that it is picked up by duck tape. Ideally this
        // should never happen, but in case it does, we have a log trail.
        //
        // If you are here because your ducktape test failed with this
        // BadLogLine check, it means that you serialized a duration type that
        // caused an overflow when casting to nanoseconds. Clamp your duration
        // to nanoseconds::max().
        using input_type = typename std::chrono::duration<R, P>;
        vlog(
          serde_log.error,
          "Overflow or underflow detected when casting to nanoseconds, "
          "clamping to a limit. Input: {}  type: {}",
          duration.count(),
          type_str<input_type>());
        return duration.count() < 0 ? std::chrono::nanoseconds::min().count()
                                    : std::chrono::nanoseconds::max().count();
    }
    // No overflow/underflow detected, we are safe to cast.
    return std::chrono::duration_cast<std::chrono::nanoseconds>(duration)
      .count();
}

inline void write(iobuf& out, uuid_t t);

template<typename Rep, typename Period>
void write(iobuf& out, std::chrono::duration<Rep, Period> t);

inline void write(iobuf& out, ss::sstring t);

inline void write(iobuf& out, ss::net::inet_address t);

template<typename T, typename Tag, typename IsConstexpr>
void write(iobuf& out, ::detail::base_named_type<T, Tag, IsConstexpr> t);

template<typename T>
void write(iobuf& out, std::optional<T> t);

inline void write(iobuf& out, bytes t);

template<typename T, size_t fragment_size>
void write(iobuf& out, fragmented_vector<T, fragment_size> t);

template<typename T>
void write(iobuf& out, tristate<T> t);

template<typename T>
requires is_absl_node_hash_set<std::decay_t<T>>
         || is_absl_flat_hash_set<std::decay_t<T>>
         || is_absl_btree_set<std::decay_t<T>>
void write(iobuf& out, T t);

template<typename T>
requires is_absl_node_hash_map<std::decay_t<T>>
         || is_absl_flat_hash_map<std::decay_t<T>>
         || is_std_unordered_map<std::decay_t<T>>
         || is_absl_btree_map<std::decay_t<T>>
void write(iobuf& out, T t);

template<typename T>
void write(iobuf& out, std::vector<T> t);

template<typename T, std::size_t Size>
void write(iobuf& out, std::array<T, Size> const& t);

template<typename T>
requires is_envelope<std::decay_t<T>>
void write(iobuf& out, T t);

inline void write(iobuf& out, iobuf t);

inline void write(iobuf& out, uuid_t t) {
    out.append(t.uuid().data, uuid_t::length);
}

template<typename Rep, typename Period>
void write(iobuf& out, std::chrono::duration<Rep, Period> t) {
    // We explicitly serialize it as ns to avoid any surprises like
    // seastar updating underlying duration types without
    // notice. See https://github.com/redpanda-data/redpanda/pull/5002
    //
    // Check for overflows/underflows.
    // For ex: a millisecond and nanosecond use the same underlying
    // type int64_t but converting from one to other can easily overflow,
    // this is by design.
    // Since we serialize with ns precision, there is a restriction of
    // nanoseconds::max()'s equivalent on the duration to be serialized.
    // On a typical platform which uses int64_t for 'rep', it roughly
    // translates to ~292 years.
    //
    // If we detect an overflow, we will clamp it to maximum supported
    // duration, which is nanosecond::max() and if there is an underflow,
    // we clamp it to minimum supported duration which is nanosecond::min().
    static_assert(
      !std::is_floating_point_v<Rep>,
      "Floating point duration conversions are prone to precision and "
      "rounding issues.");
    write<int64_t>(out, checked_duration_cast_to_nanoseconds(t));
}

inline void write(iobuf& out, ss::sstring t) {
    write<serde_size_t>(out, t.size());
    out.append(t.data(), t.size());
}

inline void write(iobuf& out, ss::net::inet_address t) {
    iobuf address_bytes;

    // NOLINTNEXTLINE
    address_bytes.append((const char*)t.data(), t.size());

    write(out, t.is_ipv4());
    write(out, std::move(address_bytes));
}

template<typename T, typename Tag, typename IsConstexpr>
void write(iobuf& out, ::detail::base_named_type<T, Tag, IsConstexpr> t) {
    return write(out, static_cast<T>(t));
}

template<typename T>
void write(iobuf& out, std::optional<T> t) {
    if (t) {
        write(out, true);
        write(out, std::move(t.value()));
    } else {
        write(out, false);
    }
}

inline void write(iobuf& out, bytes t) {
    write<serde_size_t>(out, t.size());
    out.append(t.data(), t.size());
}

template<typename T, size_t fragment_size>
void write(iobuf& out, fragmented_vector<T, fragment_size> t) {
    if (unlikely(t.size() > std::numeric_limits<serde_size_t>::max())) {
        throw serde_exception(fmt_with_ctx(
          ssx::sformat,
          "serde: fragmented vector size {} exceeds serde_size_t",
          t.size()));
    }
    write(out, static_cast<serde_size_t>(t.size()));
    for (auto& el : t) {
        write(out, std::move(el));
    }
}

template<typename T>
void write(iobuf& out, ss::chunked_fifo<T> t) {
    if (unlikely(t.size() > std::numeric_limits<serde_size_t>::max())) {
        throw serde_exception(fmt_with_ctx(
          ssx::sformat,
          "serde: chunked fifo size {} exceeds serde_size_t",
          t.size()));
    }
    write(out, static_cast<serde_size_t>(t.size()));
    for (auto& el : t) {
        write(out, std::move(el));
    }
}

template<typename T>
void write(iobuf& out, tristate<T> t) {
    if (t.is_disabled()) {
        write<int8_t>(out, -1);
    } else if (!t.has_optional_value()) {
        write<int8_t>(out, 0);
    } else {
        write<int8_t>(out, 1);
        write(out, std::move(t.value()));
    }
}

template<typename T>
requires is_absl_node_hash_set<std::decay_t<T>>
         || is_absl_flat_hash_set<std::decay_t<T>>
         || is_absl_btree_set<std::decay_t<T>>
void write(iobuf& out, T t) {
    if (unlikely(t.size() > std::numeric_limits<serde_size_t>::max())) {
        throw serde_exception(fmt_with_ctx(
          ssx::sformat,
          "serde: {} size {} exceeds serde_size_t",
          type_str<T>(),
          t.size()));
    }
    write(out, static_cast<serde_size_t>(t.size()));
    for (auto& e : t) {
        write(out, e);
    }
}

template<typename T>
requires is_absl_node_hash_map<std::decay_t<T>>
         || is_absl_flat_hash_map<std::decay_t<T>>
         || is_std_unordered_map<std::decay_t<T>>
         || is_absl_btree_map<std::decay_t<T>>
void write(iobuf& out, T t) {
    if (unlikely(t.size() > std::numeric_limits<serde_size_t>::max())) {
        throw serde_exception(fmt_with_ctx(
          ssx::sformat,
          "serde: {} size {} exceeds serde_size_t",
          type_str<T>(),
          t.size()));
    }
    write(out, static_cast<serde_size_t>(t.size()));
    for (auto& v : t) {
        write(out, v.first);
        write(out, std::move(v.second));
    }
}

template<typename T>
void write(iobuf& out, std::vector<T> t) {
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
}

template<typename T, std::size_t Size>
void write(iobuf& out, std::array<T, Size> const& t) {
    if (unlikely(t.size() > std::numeric_limits<serde_size_t>::max())) {
        throw serde_exception(fmt_with_ctx(
          ssx::sformat, "serde: array size {} exceeds serde_size_t", t.size()));
    }
    write(out, static_cast<serde_size_t>(t.size()));
    for (auto& el : t) {
        write(out, el);
    }
}

template<typename T>
requires is_envelope<std::decay_t<T>>
void write(iobuf& out, T t) {
    using Type = std::decay_t<T>;

    write(out, Type::redpanda_serde_version);
    write(out, Type::redpanda_serde_compat_version);

    auto size_placeholder = out.reserve(sizeof(serde_size_t));

    auto checksum_placeholder = iobuf::placeholder{};
    if constexpr (is_checksum_envelope<Type>) {
        checksum_placeholder = out.reserve(sizeof(checksum_t));
    }

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
    auto const size = ss::cpu_to_le(static_cast<serde_size_t>(written_size));
    size_placeholder.write(
      reinterpret_cast<char const*>(&size), sizeof(serde_size_t));

    if constexpr (is_checksum_envelope<Type>) {
        auto crc = crc::crc32c{};
        auto in = iobuf_const_parser{out};
        in.skip(size_before);
        in.consume(in.bytes_left(), [&crc](char const* src, size_t const n) {
            crc.extend(src, n);
            return ss::stop_iteration::no;
        });
        auto const checksum = ss::cpu_to_le(crc.value());
        static_assert(
          std::is_same_v<std::decay_t<decltype(checksum)>, checksum_t>);
        checksum_placeholder.write(
          reinterpret_cast<char const*>(&checksum), sizeof(checksum_t));
    }
}

inline void write(iobuf& out, iobuf t) {
    write<serde_size_t>(out, t.size_bytes());
    out.append(t.share(0, t.size_bytes()));
}

template<typename Clock, typename Duration>
void write(iobuf&, std::chrono::time_point<Clock, Duration> t) {
    static_assert(
      !is_chrono_time_point<decltype(t)>,
      "Time point serialization is risky and can have unintended "
      "consequences. Check with Redpanda team before fixing this.");
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
    return ret;
}

template<typename T>
void read_nested(iobuf_parser& in, T& t, std::size_t const bytes_left_limit) {
    using Type = std::decay_t<T>;
    static_assert(
      !is_chrono_time_point<Type>,
      "Time point serialization is risky and can have unintended "
      "consequences. Check with Redpanda team before fixing this.");
    static_assert(are_bytes_and_string_different<Type>);
    static_assert(has_serde_read<T> || is_serde_compatible_v<Type>);

    if constexpr (is_envelope<Type>) {
        auto const h = read_header<Type>(in, bytes_left_limit);

        if constexpr (is_checksum_envelope<Type>) {
            auto const shared = in.share_no_consume(
              in.bytes_left() - h._bytes_left_limit);
            auto read_only_in = iobuf_const_parser{shared};
            auto crc = crc::crc32c{};
            read_only_in.consume(
              read_only_in.bytes_left(),
              [&crc](char const* src, size_t const n) {
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
    } else if constexpr (reflection::is_std_vector<Type>) {
        using value_type = typename Type::value_type;
        const auto size = read_nested<serde_size_t>(in, bytes_left_limit);
        t.reserve(size);
        for (auto i = 0U; i < size; ++i) {
            t.push_back(read_nested<value_type>(in, bytes_left_limit));
        }
    } else if constexpr (reflection::is_std_array<Type>) {
        using value_type = typename Type::value_type;
        const auto size = read_nested<serde_size_t>(in, bytes_left_limit);
        if (unlikely(size != std::tuple_size_v<Type>)) {
            throw serde_exception(fmt_with_ctx(
              ssx::sformat,
              "reading type {}, size mismatch. expected {} go {}",
              type_str<Type>(),
              std::tuple_size_v<Type>,
              size));
        }
        for (auto i = 0U; i < std::tuple_size_v<Type>; ++i) {
            t[i] = read_nested<value_type>(in, bytes_left_limit);
        }
    } else if constexpr (reflection::is_rp_named_type<Type>) {
        t = Type{read_nested<typename Type::type>(in, bytes_left_limit)};
    } else if constexpr (std::is_same_v<Type, iobuf>) {
        t = in.share(read_nested<serde_size_t>(in, bytes_left_limit));
    } else if constexpr (std::is_same_v<Type, ss::sstring>) {
        auto str = ss::uninitialized_string(
          read_nested<serde_size_t>(in, bytes_left_limit));
        in.consume_to(str.size(), str.begin());
        t = str;
    } else if constexpr (std::is_same_v<Type, bytes>) {
        auto str = ss::uninitialized_string<bytes>(
          read_nested<serde_size_t>(in, bytes_left_limit));
        in.consume_to(str.size(), str.begin());
        t = str;
    } else if constexpr (std::is_same_v<Type, uuid_t>) {
        in.consume_to(uuid_t::length, t.mutable_uuid().begin());
    } else if constexpr (reflection::is_std_optional<Type>) {
        t = read_nested<bool>(in, bytes_left_limit)
              ? Type{read_nested<typename Type::value_type>(
                in, bytes_left_limit)}
              : std::nullopt;
    } else if constexpr (
      is_absl_node_hash_set<Type> || is_absl_flat_hash_set<Type>) {
        const auto size = read_nested<serde_size_t>(in, bytes_left_limit);
        t.reserve(size);
        for (auto i = 0U; i < size; ++i) {
            auto elem = read_nested<typename Type::key_type>(
              in, bytes_left_limit);
            t.emplace(std::move(elem));
        }
    } else if constexpr (is_absl_btree_set<Type>) {
        const auto size = read_nested<serde_size_t>(in, bytes_left_limit);
        for (auto i = 0U; i < size; ++i) {
            auto elem = read_nested<typename Type::key_type>(
              in, bytes_left_limit);
            t.emplace(std::move(elem));
        }
    } else if constexpr (
      is_absl_node_hash_map<Type> || is_absl_flat_hash_map<Type>
      || is_std_unordered_map<Type>) {
        const auto size = read_nested<serde_size_t>(in, bytes_left_limit);
        t.reserve(size);
        for (auto i = 0U; i < size; ++i) {
            auto key = read_nested<typename Type::key_type>(
              in, bytes_left_limit);
            auto value = read_nested<typename Type::mapped_type>(
              in, bytes_left_limit);
            t.emplace(std::move(key), std::move(value));
        }
    } else if constexpr (is_absl_btree_map<Type>) {
        const auto size = read_nested<serde_size_t>(in, bytes_left_limit);
        for (auto i = 0U; i < size; ++i) {
            auto key = read_nested<typename Type::key_type>(
              in, bytes_left_limit);
            auto value = read_nested<typename Type::mapped_type>(
              in, bytes_left_limit);
            t.emplace(std::move(key), std::move(value));
        }
    } else if constexpr (is_fragmented_vector<Type> || is_chunked_fifo<Type>) {
        using value_type = typename Type::value_type;
        const auto size = read_nested<serde_size_t>(in, bytes_left_limit);
        for (auto i = 0U; i < size; ++i) {
            t.push_back(read_nested<value_type>(in, bytes_left_limit));
        }
        t.shrink_to_fit();
    } else if constexpr (is_chrono_duration<Type>) {
        static_assert(
          !std::is_floating_point_v<typename Type::rep>,
          "Floating point duration conversions are prone to precision and "
          "rounding issues.");
        auto rep = read_nested<int64_t>(in, bytes_left_limit);
        t = std::chrono::duration_cast<Type>(std::chrono::nanoseconds{rep});
    } else if constexpr (reflection::is_tristate<T>) {
        int8_t flag = read_nested<int8_t>(in, bytes_left_limit);
        if (flag == -1) {
            // disabled
            t = T{};
        } else if (flag == 0) {
            // empty
            t = T(std::nullopt);
        } else if (flag == 1) {
            t = T(read_nested<typename T::value_type>(in, bytes_left_limit));
        } else {
            throw serde_exception(fmt_with_ctx(
              ssx::sformat,
              "reading type {} of size {}: {} bytes left - unexpected tristate "
              "state flag: {}, expected states are -1,0,1",
              type_str<Type>(),
              sizeof(Type),
              in.bytes_left(),
              flag));
        }
    } else if constexpr (std::is_same_v<T, ss::net::inet_address>) {
        bool is_ipv4 = read_nested<bool>(in, bytes_left_limit);
        auto address_buf = read_nested<iobuf>(in, bytes_left_limit);
        auto address_bytes = iobuf_to_bytes(address_buf);
        if (is_ipv4) {
            ::in_addr addr{};
            if (unlikely(address_bytes.size() != sizeof(addr))) {
                throw serde_exception(fmt_with_ctx(
                  ssx::sformat,
                  "reading type {} of size {}: {} bytes left - unexpected ipv4 "
                  "address size, read: {}, expected: {}",
                  type_str<Type>(),
                  sizeof(Type),
                  in.bytes_left(),
                  address_bytes.size(),
                  sizeof(addr)));
            }

            std::memcpy(&addr, address_bytes.c_str(), sizeof(addr));
            t = ss::net::inet_address(addr);
        } else {
            ::in6_addr addr{};
            if (unlikely(address_bytes.size() != sizeof(addr))) {
                throw serde_exception(fmt_with_ctx(
                  ssx::sformat,
                  "reading type {} of size {}: {} bytes left - unexpected ipv6 "
                  "address size, read: {}, expected: {}",
                  type_str<Type>(),
                  sizeof(Type),
                  in.bytes_left(),
                  address_bytes.size(),
                  sizeof(addr)));
            }
            std::memcpy(&addr, address_bytes.c_str(), sizeof(addr));
            t = ss::net::inet_address(addr);
        }
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
#include "serde/rw/bool_class.h"
#include "serde/rw/enum.h"
#include "serde/rw/scalar.h"

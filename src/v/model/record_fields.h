/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "bytes/iobuf.h"
#include "bytes/iobuf_parser.h"
#include "container/chunked_vector.h"
#include "model/record.h"

#include <seastar/core/future.hh>

#include <array>
#include <cstdint>
#include <stdexcept>
#include <type_traits>
#include <utility>

namespace model {

/// \brief Compile-time selectable record fields for `for_each_record<>()`.
///
/// Pass a subset of these to `record_batch::for_each_record<Fields...>()` (or
/// directly to `parse_record_fields<Fields...>()`) to control which parts of a
/// record are materialized while iterating.
enum class record_field : uint8_t {
    /// `int32_t`, the record's encoded size: everything after the size varint
    /// itself. Matches `record::size_bytes()`.
    size_bytes,
    /// `model::record_attributes`.
    attributes,
    /// `int64_t` delta from the batch first timestamp.
    timestamp_delta,
    /// `int32_t` delta from the batch base offset.
    offset_delta,
    /// `iobuf`, empty when the record has a null/empty key.
    key,
    /// `bool`, true when the record has a null value.
    is_tombstone,
    /// `iobuf`, empty when the record has a null/empty value.
    value,
    /// `chunked_vector<record_header>`.
    headers,
};

namespace detail {

template<record_field F>
struct record_field_holder;

template<>
struct record_field_holder<record_field::size_bytes> {
    int32_t size_bytes{0};
};

template<>
struct record_field_holder<record_field::attributes> {
    record_attributes attributes{};
};

template<>
struct record_field_holder<record_field::timestamp_delta> {
    int64_t timestamp_delta{0};
};

template<>
struct record_field_holder<record_field::offset_delta> {
    int32_t offset_delta{0};
};

template<>
struct record_field_holder<record_field::key> {
    iobuf key;
};

template<>
struct record_field_holder<record_field::is_tombstone> {
    bool is_tombstone{false};
};

template<>
struct record_field_holder<record_field::value> {
    iobuf value;
};

template<>
struct record_field_holder<record_field::headers> {
    chunked_vector<record_header> headers;
};

/// Canonical base ordering for `parsed_record`, descending by member
/// size/alignment so that any requested subset packs with minimal padding,
/// regardless of the order the fields were requested in.
inline constexpr std::array record_field_layout_order{
  record_field::headers,
  record_field::key,
  record_field::value,
  record_field::timestamp_delta,
  record_field::size_bytes,
  record_field::offset_delta,
  record_field::attributes,
  record_field::is_tombstone,
};

template<record_field... Fields>
consteval auto layout_sorted_fields() {
    std::array<record_field, sizeof...(Fields)> out{};
    size_t i = 0;
    for (auto f : record_field_layout_order) {
        if (((Fields == f) || ...)) {
            out[i++] = f;
        }
    }
    if (i != out.size()) {
        // Compile-time failure: a duplicate field was requested, or a
        // `record_field` is missing from `record_field_layout_order`.
        throw std::logic_error("invalid record_field pack");
    }
    return out;
}

template<record_field... Fields>
struct field_holder_pack : record_field_holder<Fields>... {};

template<auto SortedFields, size_t... Is>
auto make_layout_pack(std::index_sequence<Is...>)
  -> field_holder_pack<SortedFields[Is]...>;

template<record_field... Fields>
using layout_sorted_pack_t
  = decltype(make_layout_pack<layout_sorted_fields<Fields...>()>(
    std::make_index_sequence<sizeof...(Fields)>{}));

} // namespace detail

/// \brief The subset of a record materialized by `parse_record_fields<>()`.
template<record_field... Fields>
struct parsed_record : detail::layout_sorted_pack_t<Fields...> {
    static_assert(
      sizeof...(Fields) > 0, "for_each_record<> requires at least one field");

    /// True iff `F` was requested; usable in `if constexpr` by callers.
    template<record_field F>
    static constexpr bool has = ((Fields == F) || ...);
};

/// \brief Parse a single record from `p`, materializing only `Fields`.
///
/// A record is a strictly ordered sequence of variable-length fields:
///
///   size | attributes | ts_delta | offset_delta | key | value | headers
///
/// Because the fields are variable length there is no way to seek to one:
/// reaching a field requires decoding everything before it.
///
/// Consumes exactly one record from `p`.
///
/// With `FullyParse` set, every field is decoded (though still only `Fields`
/// are materialized) and the record's declared size is validated against the
/// bytes its fields actually occupy, so a malformed record throws instead of
/// being silently skipped over.
template<bool FullyParse, record_field... Fields>
parsed_record<Fields...> parse_record_fields(iobuf_const_parser& p) {
    constexpr auto wants = [](record_field f) {
        return ((Fields == f) || ...);
    };

    // A field must be decoded (though not necessarily materialized) when it is
    // requested, when any later field is requested, or when `FullyParse`
    // requires walking the whole record.
    constexpr bool reach_headers = wants(record_field::headers) || FullyParse;
    constexpr bool reach_value = reach_headers || wants(record_field::value)
                                 || wants(record_field::is_tombstone);
    constexpr bool reach_key = reach_value || wants(record_field::key);
    constexpr bool reach_offset = reach_key
                                  || wants(record_field::offset_delta);
    constexpr bool reach_ts = reach_offset
                              || wants(record_field::timestamp_delta);
    constexpr bool reach_attr = reach_ts || wants(record_field::attributes);

    parsed_record<Fields...> out;

    // `record_size` covers everything after the size varint itself, so it lets
    // us skip whatever remains of the record from any point.
    auto [record_size, rv] = p.read_varlong();
    if (static_cast<size_t>(record_size) > p.bytes_left()) [[unlikely]] {
        throw std::out_of_range(
          fmt::format(
            "Expected record size {} but only {} bytes left",
            record_size,
            p.bytes_left()));
    }
    if constexpr (wants(record_field::size_bytes)) {
        out.size_bytes = static_cast<int32_t>(record_size);
    }
    const size_t body_start = p.bytes_consumed();
    auto body_bytes_consumed = [&] { return p.bytes_consumed() - body_start; };
    [[maybe_unused]] auto skip_tail = [&] {
        const size_t consumed = body_bytes_consumed();
        if (consumed > static_cast<size_t>(record_size)) [[unlikely]] {
            throw std::out_of_range(
              fmt::format(
                "Record fields consumed {} bytes, overrunning the declared "
                "record size {}",
                consumed,
                record_size));
        }
        p.skip(static_cast<size_t>(record_size) - consumed);
    };

    if constexpr (!reach_attr) {
        skip_tail();
        return out;
    }
    auto attr = p.consume_type<record_attributes::type>();
    if constexpr (wants(record_field::attributes)) {
        out.attributes = record_attributes(attr);
    }

    if constexpr (!reach_ts) {
        skip_tail();
        return out;
    }
    auto [timestamp_delta, tv] = p.read_varlong();
    if constexpr (wants(record_field::timestamp_delta)) {
        out.timestamp_delta = static_cast<int64_t>(timestamp_delta);
    }

    if constexpr (!reach_offset) {
        skip_tail();
        return out;
    }
    auto [offset_delta, ov] = p.read_varlong();
    if constexpr (wants(record_field::offset_delta)) {
        out.offset_delta = static_cast<int32_t>(offset_delta);
    }

    if constexpr (!reach_key) {
        skip_tail();
        return out;
    }
    auto [key_length, kv] = p.read_varlong();
    if (key_length > record_size) [[unlikely]] {
        throw std::out_of_range(
          fmt::format(
            "Expected key length {} but record has only {} bytes in total",
            key_length,
            record_size));
    }
    if constexpr (wants(record_field::key)) {
        if (key_length > 0) {
            out.key = p.copy(static_cast<size_t>(key_length));
        }
    } else if constexpr (reach_value) {
        // Key not requested, but a later field is: advance past it. When key is
        // the last requested field the `skip_tail` below covers these bytes.
        if (key_length > 0) {
            p.skip(static_cast<size_t>(key_length));
        }
    }

    if constexpr (!reach_value) {
        skip_tail();
        return out;
    }
    auto [value_length, vv] = p.read_varlong();
    if constexpr (wants(record_field::is_tombstone)) {
        out.is_tombstone = value_length < 0;
    }
    if (value_length > record_size) [[unlikely]] {
        throw std::out_of_range(
          fmt::format(
            "Expected value length {} but record has only {} bytes in total",
            value_length,
            record_size));
    }
    if constexpr (wants(record_field::value)) {
        if (value_length > 0) {
            out.value = p.copy(static_cast<size_t>(value_length));
        }
    } else if constexpr (reach_headers) {
        if (value_length > 0) {
            p.skip(static_cast<size_t>(value_length));
        }
    }

    if constexpr (!reach_headers) {
        skip_tail();
        return out;
    }
    auto [header_count, hcv] = p.read_varlong();
    if (static_cast<size_t>(header_count) > p.bytes_left()) [[unlikely]] {
        throw std::out_of_range(
          fmt::format(
            "Expected {} headers, but only {} bytes left",
            header_count,
            p.bytes_left()));
    }
    if constexpr (wants(record_field::headers)) {
        out.headers.reserve(header_count);
    }
    for (int64_t i = 0; i < header_count; ++i) {
        auto [hk_len, hkv] = p.read_varlong();
        [[maybe_unused]] iobuf hkey;
        if (hk_len > 0) {
            if constexpr (wants(record_field::headers)) {
                hkey = p.copy(static_cast<size_t>(hk_len));
            } else {
                p.skip(static_cast<size_t>(hk_len));
            }
        }
        auto [hv_len, hvv] = p.read_varlong();
        [[maybe_unused]] iobuf hval;
        if (hv_len > 0) {
            if constexpr (wants(record_field::headers)) {
                hval = p.copy(static_cast<size_t>(hv_len));
            } else {
                p.skip(static_cast<size_t>(hv_len));
            }
        }
        if constexpr (wants(record_field::headers)) {
            out.headers.emplace_back(
              static_cast<int32_t>(hk_len),
              std::move(hkey),
              static_cast<int32_t>(hv_len),
              std::move(hval));
        }
    }
    // The whole record was decoded, so its declared size must match the bytes
    // its fields occupy exactly.
    const size_t consumed = body_bytes_consumed();
    if (consumed != static_cast<size_t>(record_size)) [[unlikely]] {
        throw std::out_of_range(
          fmt::format(
            "Record size mismatch: expected {} but parsed {} bytes",
            record_size,
            consumed));
    }
    return out;
}

/// The common, non-validating parse: unrequested trailing fields are skipped
/// wholesale rather than decoded. Overload resolution cannot confuse the two:
/// a leading `bool` never binds to `record_field` and vice versa.
template<record_field... Fields>
parsed_record<Fields...> parse_record_fields(iobuf_const_parser& p) {
    return parse_record_fields<false, Fields...>(p);
}

template<record_field First, record_field... Rest, typename Func>
void record_batch::for_each_record(Func f) const {
    using parsed = parsed_record<First, Rest...>;
    iobuf_const_parser parser(_records);
    for (int32_t i = 0; i < _header.record_count; ++i) {
        auto pr = parse_record_fields<First, Rest...>(parser);
        if constexpr (std::is_void_v<std::invoke_result_t<Func, parsed&&>>) {
            f(std::move(pr));
        } else {
            ss::stop_iteration s = f(std::move(pr));
            if (s == ss::stop_iteration::yes) {
                return;
            }
        }
    }
}

template<record_field First, record_field... Rest, typename Func>
ss::future<> record_batch::for_each_record_async(Func f) const {
    using parsed = parsed_record<First, Rest...>;
    iobuf_const_parser parser(_records);
    for (int32_t i = 0; i < _header.record_count; ++i) {
        auto pr = parse_record_fields<First, Rest...>(parser);
        if constexpr (
          std::is_same_v<
            ss::futurize_t<std::invoke_result_t<Func, parsed&&>>,
            ss::future<ss::stop_iteration>>) {
            ss::stop_iteration s = co_await ss::futurize_invoke(
              f, std::move(pr));
            if (s == ss::stop_iteration::yes) {
                co_return;
            }
        } else {
            co_await ss::futurize_invoke(f, std::move(pr));
        }
    }
}

} // namespace model

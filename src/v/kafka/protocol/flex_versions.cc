// Copyright 2022 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "kafka/protocol/flex_versions.h"

#include "base/units.h"
#include "kafka/protocol/messages.h"
#include "kafka/protocol/types.h"
#include "kafka/protocol/wire.h"
#include "net/types.h"
#include "utils/vint_iostream.h"

#include <seastar/core/iostream.hh>

namespace kafka {

namespace {

/// Not every value from 0 -> max_api_key is a valid request, non-supported
/// requests will map to a value of api_version(-2)
constexpr api_version invalid_api = api_version(-2);

template<typename... Ts>
consteval void
fill_flex(api_key_table<api_version>& versions, type_list<Ts...>) {
    /// Values map to the first flex version for a given api; apis absent from
    /// the lists keep \ref invalid_api.
    ((versions[Ts::key] = Ts::min_flexible), ...);
}

consteval auto get_flexible_request_min_versions_list() {
    api_key_table<api_version> versions{invalid_api};
    fill_flex(versions, request_types{});
    fill_flex(versions, redpanda_request_types{});
    return versions;
}

constexpr auto g_flex_mapping = get_flexible_request_min_versions_list();

struct protocol_parse_exception : public net::parsing_exception {
    explicit protocol_parse_exception(const std::string& m)
      : net::parsing_exception(m) {}
};

} // namespace

bool flex_versions::is_flexible_request(api_key key, api_version version) {
    /// If bounds checking is desired call is_api_in_schema(key) beforehand
    const api_version first_flex_version = g_flex_mapping[key];
    return (version >= first_flex_version)
           && (first_flex_version != never_flexible);
}

bool flex_versions::is_api_in_schema(api_key key) noexcept {
    // find() does the region routing once and returns null for out-of-range.
    const api_version* first_flex_version = g_flex_mapping.find(key);
    return first_flex_version != nullptr && *first_flex_version != invalid_api;
}

namespace {
// TODO(C++26): replace with std::sub_sat
size_t sub_sat(size_t a, size_t b) {
    size_t c = 0;
    if (!__builtin_sub_overflow(a, b, &c)) {
        return c;
    }
    return 0;
}
} // namespace

ss::future<std::pair<std::optional<tagged_fields>, size_t>>
// NOLINTNEXTLINE(cppcoreguidelines-avoid-reference-coroutine-parameters)
parse_tags(ss::input_stream<char>& src, size_t max_bytes) {
    size_t total_bytes_read = 0;
    auto read_unsigned_vint =
      // NOLINTNEXTLINE(cppcoreguidelines-avoid-reference-coroutine-parameters)
      [](size_t& total_bytes_read, ss::input_stream<char>& src) {
          return unsigned_vint::stream_deserialize(src).then(
            [&total_bytes_read](std::pair<uint32_t, size_t> pair) {
                auto& [n, bytes_read] = pair;
                total_bytes_read += bytes_read;
                return n;
            });
      };

    auto num_tags = co_await read_unsigned_vint(total_bytes_read, src);
    if (num_tags == 0) {
        /// In the likely event that no tags are parsed as headers, return
        /// nullopt instead of an empty vector to reduce the memory overhead
        /// (that will never be used) per request
        co_return std::make_pair(std::nullopt, total_bytes_read);
    }

    tagged_fields::type tags;
    while (num_tags-- > 0) {
        auto id = co_await read_unsigned_vint(total_bytes_read, src);
        auto next_len = co_await read_unsigned_vint(total_bytes_read, src);
        if (next_len > sub_sat(max_bytes, total_bytes_read)) {
            throw protocol_parse_exception(
              fmt::format(
                "tagged field {} length {} exceeds remaining message budget {}",
                id,
                next_len,
                max_bytes - total_bytes_read));
        }
        if (next_len > 128_KiB) {
            throw protocol_parse_exception(
              fmt::format(
                "tagged field {} length {} exceeds 128 KiB limit",
                id,
                next_len));
        }
        auto buf = co_await src.read_exactly(next_len);
        if (buf.size() != next_len) {
            throw protocol_parse_exception(
              fmt::format(
                "short read for tagged field {} length {} but got {}",
                id,
                next_len,
                buf.size()));
        }
        bytes data(bytes::initialized_later{}, buf.size());
        std::copy_n(buf.begin(), buf.size(), data.begin());
        total_bytes_read += next_len;
        auto [_, succeded] = tags.emplace(tag_id(id), std::move(data));
        if (!succeded) {
            throw protocol_parse_exception(
              fmt::format("duplicate tag id detected: {}", id));
        }
    }
    co_return {{tagged_fields{std::move(tags)}}, total_bytes_read};
}

namespace {
struct invalid_buffer_size_exception : public net::parsing_exception {
    explicit invalid_buffer_size_exception(const std::string& m)
      : net::parsing_exception(m) {}
};
size_t parse_size_buffer(ss::temporary_buffer<char> buf) {
    iobuf data;
    data.append(std::move(buf));
    protocol::decoder reader(std::move(data));
    auto size = reader.read_int32();
    if (size < 0) {
        throw invalid_buffer_size_exception(
          "kafka::parse_size_buffer is negative");
    }
    return size_t(size);
}
} // namespace

namespace protocol {
// NOLINTNEXTLINE(cppcoreguidelines-avoid-reference-coroutine-parameters)
ss::future<std::optional<size_t>> parse_size(ss::input_stream<char>& src) {
    auto buf = co_await src.read_exactly(sizeof(int32_t));
    if (!buf) {
        co_return std::nullopt;
    }
    co_return parse_size_buffer(std::move(buf));
}
} // namespace protocol

} // namespace kafka

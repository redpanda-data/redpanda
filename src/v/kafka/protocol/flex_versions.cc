// Copyright 2022 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "kafka/protocol/flex_versions.h"

#include "kafka/protocol/types.h"
#include "kafka/server/handlers/handlers.h"
#include "utils/vint_iostream.h"
namespace kafka {

/// Not every value from 0 -> max_api_key is a valid request, non-supported
/// requests will map to a value of api_key(-2)
static constexpr api_version invalid_api = api_version(-2);

template<typename... RequestTypes>
static constexpr auto
get_flexible_request_min_versions_list(type_list<RequestTypes...> r) {
    /// An std::array where the indicies map to api_keys and values at an index
    /// map to the first flex version for a given api. If an api doesn't exist
    /// at an index -2 or \ref invalid_api will be the value at the index.
    std::array<api_version, max_api_key(r) + 1> versions;
    versions.fill(invalid_api);
    ((versions[RequestTypes::api::key()] = RequestTypes::api::min_flexible),
     ...);
    return versions;
}

static constexpr auto g_flex_mapping = get_flexible_request_min_versions_list(
  request_types());

bool flex_versions::is_flexible_request(api_key key, api_version version) {
    /// If bounds checking is desired call is_api_in_schema(key) beforehand
    const api_version first_flex_version = g_flex_mapping[key()];
    return (version >= first_flex_version)
           && (first_flex_version != never_flexible);
}

bool flex_versions::is_api_in_schema(api_key key) noexcept {
    constexpr auto max_version = g_flex_mapping.max_size() - 1;
    if (key() < 0 || static_cast<size_t>(key()) > max_version) {
        return false;
    }
    const api_version first_flex_version = g_flex_mapping[key()];
    return first_flex_version != invalid_api;
}

ss::future<std::pair<std::optional<tagged_fields>, size_t>>
parse_tags(ss::input_stream<char>& src) {
    size_t total_bytes_read = 0;
    auto read_unsigned_vint =
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
        auto buf = co_await src.read_exactly(next_len);
        auto data = ss::uninitialized_string<bytes>(buf.size());
        std::copy_n(buf.begin(), buf.size(), data.begin());
        total_bytes_read += next_len;
        auto [_, succeded] = tags.emplace(tag_id(id), std::move(data));
        if (!succeded) {
            throw std::logic_error(
              fmt::format("Protocol error, duplicate tag id detected, {}", id));
        }
    }
    co_return std::make_pair(std::move(tags), total_bytes_read);
}

namespace {
size_t parse_size_buffer(ss::temporary_buffer<char> buf) {
    iobuf data;
    data.append(std::move(buf));
    protocol::decoder reader(std::move(data));
    auto size = reader.read_int32();
    if (size < 0) {
        throw std::runtime_error("kafka::parse_size_buffer is negative");
    }
    return size_t(size);
}
} // namespace

namespace protocol {
ss::future<std::optional<size_t>> parse_size(ss::input_stream<char>& src) {
    auto buf = co_await src.read_exactly(sizeof(int32_t));
    if (!buf) {
        co_return std::nullopt;
    }
    co_return parse_size_buffer(std::move(buf));
}
} // namespace protocol

} // namespace kafka

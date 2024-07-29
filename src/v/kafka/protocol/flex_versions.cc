// Copyright 2022 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "kafka/protocol/flex_versions.h"

#include "kafka/protocol/schemata/add_offsets_to_txn_request.h"
#include "kafka/protocol/schemata/add_partitions_to_txn_request.h"
#include "kafka/protocol/schemata/alter_client_quotas_request.h"
#include "kafka/protocol/schemata/alter_configs_request.h"
#include "kafka/protocol/schemata/alter_partition_reassignments_request.h"
#include "kafka/protocol/schemata/api_versions_request.h"
#include "kafka/protocol/schemata/create_acls_request.h"
#include "kafka/protocol/schemata/create_partitions_request.h"
#include "kafka/protocol/schemata/create_topics_request.h"
#include "kafka/protocol/schemata/delete_acls_request.h"
#include "kafka/protocol/schemata/delete_groups_request.h"
#include "kafka/protocol/schemata/delete_records_request.h"
#include "kafka/protocol/schemata/delete_topics_request.h"
#include "kafka/protocol/schemata/describe_acls_request.h"
#include "kafka/protocol/schemata/describe_client_quotas_request.h"
#include "kafka/protocol/schemata/describe_configs_request.h"
#include "kafka/protocol/schemata/describe_groups_request.h"
#include "kafka/protocol/schemata/describe_log_dirs_request.h"
#include "kafka/protocol/schemata/describe_producers_request.h"
#include "kafka/protocol/schemata/describe_transactions_request.h"
#include "kafka/protocol/schemata/end_txn_request.h"
#include "kafka/protocol/schemata/fetch_request.h"
#include "kafka/protocol/schemata/find_coordinator_request.h"
#include "kafka/protocol/schemata/heartbeat_request.h"
#include "kafka/protocol/schemata/incremental_alter_configs_request.h"
#include "kafka/protocol/schemata/init_producer_id_request.h"
#include "kafka/protocol/schemata/join_group_request.h"
#include "kafka/protocol/schemata/leave_group_request.h"
#include "kafka/protocol/schemata/list_groups_request.h"
#include "kafka/protocol/schemata/list_offset_request.h"
#include "kafka/protocol/schemata/list_partition_reassignments_request.h"
#include "kafka/protocol/schemata/list_transactions_request.h"
#include "kafka/protocol/schemata/metadata_request.h"
#include "kafka/protocol/schemata/offset_commit_request.h"
#include "kafka/protocol/schemata/offset_delete_request.h"
#include "kafka/protocol/schemata/offset_fetch_request.h"
#include "kafka/protocol/schemata/offset_for_leader_epoch_request.h"
#include "kafka/protocol/schemata/produce_request.h"
#include "kafka/protocol/schemata/sasl_authenticate_request.h"
#include "kafka/protocol/schemata/sasl_handshake_request.h"
#include "kafka/protocol/schemata/sync_group_request.h"
#include "kafka/protocol/schemata/txn_offset_commit_request.h"
#include "kafka/protocol/types.h"
#include "kafka/protocol/wire.h"
#include "utils/vint_iostream.h"

#include <seastar/core/iostream.hh>

namespace kafka {

template<typename... Ts>
struct type_list {};

template<typename... Requests>
using make_request_types = type_list<Requests...>;

using request_types = make_request_types<
  produce_api,
  fetch_api,
  list_offsets_api,
  metadata_api,
  offset_fetch_api,
  offset_delete_api,
  find_coordinator_api,
  list_groups_api,
  api_versions_api,
  join_group_api,
  heartbeat_api,
  delete_records_api,
  leave_group_api,
  sync_group_api,
  create_topics_api,
  offset_commit_api,
  describe_configs_api,
  alter_configs_api,
  delete_topics_api,
  describe_groups_api,
  sasl_handshake_api,
  sasl_authenticate_api,
  incremental_alter_configs_api,
  delete_groups_api,
  describe_acls_api,
  describe_log_dirs_api,
  create_acls_api,
  delete_acls_api,
  init_producer_id_api,
  add_partitions_to_txn_api,
  txn_offset_commit_api,
  add_offsets_to_txn_api,
  end_txn_api,
  create_partitions_api,
  offset_for_leader_epoch_api,
  alter_partition_reassignments_api,
  list_partition_reassignments_api,
  describe_producers_api,
  describe_transactions_api,
  list_transactions_api,
  alter_client_quotas_api,
  describe_client_quotas_api>;

namespace {

template<typename... RequestTypes>
consteval size_t max_api_key(type_list<RequestTypes...>) {
    /// Black magic here is an overload of std::max() that takes an
    /// std::initializer_list
    return std::max({RequestTypes::key()...});
}

/// Not every value from 0 -> max_api_key is a valid request, non-supported
/// requests will map to a value of api_key(-2)
constexpr api_version invalid_api = api_version(-2);

template<typename... RequestTypes>
consteval auto
get_flexible_request_min_versions_list(type_list<RequestTypes...> r) {
    /// An std::array where the indicies map to api_keys and values at an index
    /// map to the first flex version for a given api. If an api doesn't exist
    /// at an index -2 or \ref invalid_api will be the value at the index.
    std::array<api_version, max_api_key(r) + 1> versions;
    versions.fill(invalid_api);
    ((versions[RequestTypes::key()] = RequestTypes::min_flexible), ...);
    return versions;
}

constexpr auto g_flex_mapping = get_flexible_request_min_versions_list(
  request_types());

} // namespace

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
// NOLINTNEXTLINE(cppcoreguidelines-avoid-reference-coroutine-parameters)
parse_tags(ss::input_stream<char>& src) {
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

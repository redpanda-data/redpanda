/*
 * Copyright 2022 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "kafka/client/transport.h"

#include "kafka/client/versions.h"
#include "kafka/server/flex_versions.h"

namespace kafka::client {
void transport::write_header(
  response_writer& wr, api_key key, api_version version) {
    wr.write(int16_t(key()));
    wr.write(int16_t(version()));
    wr.write(int32_t(_correlation()));
    wr.write(std::string_view("test_client"));
    vassert(
      flex_versions::is_api_in_schema(key),
      "Attempted to send request to non-existent API: {}",
      key);
    if (flex_versions::is_flexible_request(key, version)) {
        /// Tags are unused by the client but to be protocol compliant
        /// with flex versions at least a 0 byte must be written
        wr.write_tags();
    }
    _correlation = _correlation + correlation_id(1);
}

template<typename T>
api_version request_version_lookup(const T& schema, api_key key) noexcept {
    return (key() >= 0
            && key() <= static_cast<api_key::type>(schema.size() - 1))
             ? schema[key] // NOLINT
             : unlisted_api;
}

api_version transport::negotiate_version(api_key key) const {
    const auto max_broker_version = request_version_lookup(*_max_versions, key);
    const auto max_client_version = request_version_lookup(
      g_client_mapping, key);
    const auto v = std::min(max_client_version, max_broker_version);
    if (v() < 0) {
        throw unsupported_request_exception(
          fmt::format("Unsupported request API key {} attempted", key));
    }
    return v;
}

ss::future<std::vector<api_version>> transport::get_api_versions() {
    auto response = co_await dispatch(
      api_versions_request{
        .data = api_versions_request_data{.client_software_name = "redpanda"}},
      api_version(3));
    const auto max_key = std::accumulate(
      response.data.api_keys.begin(),
      response.data.api_keys.end(),
      int16_t(0),
      [](const int16_t& acc, const api_versions_response_key& key) {
          return std::max<>(acc, key.api_key);
      });
    std::vector<api_version> api_versions(max_key + 1, unlisted_api);
    for (const auto& key : response.data.api_keys) {
        api_versions[key.api_key] = api_version(key.max_version);
    }
    co_return api_versions;
}

} // namespace kafka::client

// Copyright 2022 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "kafka/server/flex_versions.h"

#include "kafka/protocol/types.h"
#include "kafka/server/handlers/handlers.h"
namespace kafka {

/// Not every value from 0 -> max_api_key is a valid request, non-supported
/// requests will map to a value of api_key(-2)
static constexpr api_version invalid_api = api_version(-2);

template<typename... RequestTypes>
static constexpr size_t max_api_key(type_list<RequestTypes...>) {
    /// Black magic here is an overload of std::max() that takes an
    /// std::initializer_list
    return std::max({RequestTypes::api::key()...});
}

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

} // namespace kafka

// Copyright 2022 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#pragma once
#include "kafka/protocol/types.h"
#include "kafka/types.h"

namespace seastar {
template<typename T>
class input_stream;
}

namespace kafka {
class flex_versions {
public:
    /**
     * Returns true if the version provided for the given request is greater
     * than or equal to the value provided for `flexibleVersions` in the schema
     * definition for the request/response
     *
     * @param key api_key of request thats being queried
     * @param version at what version of what api is this request flex
     * @returns true if the request is flex, false if it isn't, throws
     *   in instances where the api_key is not a valid api
     */
    static bool is_flexible_request(api_key key, api_version version);

    /**
     * Returns true/false if the key provided is a valid kafka request key
     * This method never throws, useful to query if an api_key is valid
     * before calling \ref is_flexible_request
     *
     * @param key api_key of request thats being queried
     * @returns true if the api is a valid kafka request, false otherwise
     */
    static bool is_api_in_schema(api_key key) noexcept;
};

ss::future<std::pair<std::optional<tagged_fields>, size_t>>
parse_tags(ss::input_stream<char>&);

} // namespace kafka

/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_storage_clients/bucket_name_parts.h"

#include <ada.h>

namespace cloud_storage_clients {

std::expected<bucket_name_parts, std::string>
parse_bucket_name(const bucket_name& bucket) {
    const auto& bucket_str = bucket();

    auto query_pos = bucket_str.find('?');
    if (bucket_str.empty() || query_pos == 0) {
        return std::unexpected("bucket: name cannot be empty");
    }

    if (query_pos == ss::sstring::npos) {
        return bucket_name_parts{.name = plain_bucket_name{bucket_str}};
    }

    bucket_name_parts parts{
      .name = plain_bucket_name{bucket_str.substr(0, query_pos)},
    };

    auto query = std::string_view{bucket_str}.substr(query_pos + 1);
    if (query.empty()) {
        return parts;
    }

    ada::url_search_params search_params(query);
    for (const auto& [key, value] : search_params) {
        parts.params.emplace_back(ss::sstring{key}, ss::sstring{value});
    }

    return parts;
}

} // namespace cloud_storage_clients

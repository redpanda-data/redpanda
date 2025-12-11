/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_storage_clients/bucket_params.h"

namespace cloud_storage_clients {

std::expected<bucket_params, std::string>
extract_bucket_params(const bucket_name& name) {
    const auto& name_str = name();

    bucket_params params;

    const size_t search_pos = name_str.find('?');
    if (search_pos == ss::sstring::npos) {
        params.plain_name = plain_bucket_name{name_str};
        return params;
    }
    params.plain_name = plain_bucket_name{name_str.substr(0, search_pos)};
    params.upstream_opts = ada::url_search_params{
      name_str.substr(search_pos + 1)};

    return params;
}

} // namespace cloud_storage_clients

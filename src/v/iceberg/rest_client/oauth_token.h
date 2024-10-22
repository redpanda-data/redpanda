/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#pragma once

#include "base/seastarx.h"

#include <seastar/core/lowres_clock.hh>
#include <seastar/core/sstring.hh>

namespace iceberg::rest_client {
// Oauth token returned by the catalog server, in exchange for credentials
struct oauth_token {
    ss::sstring access_token;
    ss::sstring token_type;
    ss::lowres_clock::time_point expires_at;
    std::optional<ss::sstring> refresh_token;
    std::optional<ss::sstring> scope;
};

} // namespace iceberg::rest_client

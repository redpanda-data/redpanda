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

#include <seastar/core/sstring.hh>

namespace iceberg::rest_client {
// Static credentials expected to be supplied by redpanda when requesting
// an oauth token
struct credentials {
    ss::sstring client_id;
    ss::sstring client_secret;
    std::optional<ss::sstring> oauth2_server_uri;
    ss::sstring oauth2_scope;
};

}; // namespace iceberg::rest_client

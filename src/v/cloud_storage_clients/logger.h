/*
 * Copyright 2020 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#pragma once

#include "base/seastarx.h"

#include <seastar/util/log.hh>

namespace cloud_storage_clients {
inline ss::logger s3_log("s3");
inline ss::logger abs_log("abs");
inline ss::logger client_config_log("client_config");
inline ss::logger pool_log("client_pool");
} // namespace cloud_storage_clients

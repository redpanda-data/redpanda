/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#pragma once

#include "model/fundamental.h"

#include <expected>

namespace cloud_storage_clients {

struct bucket_params {
    plain_bucket_name plain_name;
};

std::expected<bucket_params, std::string>
extract_bucket_params(const bucket_name& bucket);

} // namespace cloud_storage_clients

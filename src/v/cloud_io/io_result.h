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

#include "cloud_storage_clients/client.h"

#include <cstdint>
#include <iostream>

namespace cloud_io {

enum class [[nodiscard]] download_result : int32_t {
    success,
    notfound,
    timedout,
    failed,
};

enum class [[nodiscard]] upload_result : int32_t {
    success,
    timedout,
    failed,
    cancelled,
};
std::ostream& operator<<(std::ostream& o, const download_result& r);
std::ostream& operator<<(std::ostream& o, const upload_result& r);

using list_result = cloud_storage_clients::client::list_result;

} // namespace cloud_io

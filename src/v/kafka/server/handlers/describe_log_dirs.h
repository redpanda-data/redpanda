/*
 * Copyright 2021 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */
#pragma once
#include "kafka/data/partition_proxy.h"
#include "kafka/protocol/describe_log_dirs.h"
#include "kafka/server/handlers/handler.h"

#include <seastar/core/future.hh>

namespace kafka {

using describe_log_dirs_handler
  = single_stage_handler<describe_log_dirs_api, 0, 2>;

namespace describe_log_dirs::detail {

struct log_partition_data {
    describe_log_dirs_partition local;
    std::optional<describe_log_dirs_partition> remote;
};

ss::future<log_partition_data>
describe_partition(kafka::partition_proxy& p, bool include_remote);

} // namespace describe_log_dirs::detail

} // namespace kafka

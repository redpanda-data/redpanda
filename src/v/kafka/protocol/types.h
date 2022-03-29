/*
 * Copyright 2020 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */
#pragma once
#include "kafka/protocol/fwd.h"
#include "kafka/types.h"
#include "model/metadata.h"
#include "utils/concepts-enabled.h"

namespace kafka {

static constexpr model::node_id consumer_replica_id{-1};

// clang-format off
CONCEPT(
template<typename T>
concept KafkaApi = requires (T request) {
    { T::name } -> std::convertible_to<const char*>;
    { T::key } -> std::convertible_to<const api_key&>;
};
)
// clang-format on

} // namespace kafka

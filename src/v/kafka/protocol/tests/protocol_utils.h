/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "kafka/server/handlers/handler_interface.h"
#include "kafka/server/handlers/handlers.h"

namespace kafka {

/// To adapt the generic test method below for the cases in which some generated
/// types don't adhere to the standard api for type::decode
template<typename T>
concept HasPrimitiveDecode = requires(T t, iobuf iob, kafka::api_version v) {
    { t.decode(std::move(iob), v) } -> std::same_as<void>;
};

using is_kafka_request = ss::bool_class<struct is_kafka_request_tag>;

bytes invoke_franz_harness(
  kafka::api_key key, kafka::api_version v, is_kafka_request is_request);

} // namespace kafka

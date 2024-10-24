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

#include "kafka/protocol/fetch.h"

namespace kafka::client {

fetch_request make_fetch_request(
  const model::topic_partition& tp,
  model::offset offset,
  int32_t min_bytes,
  int32_t max_bytes,
  std::chrono::milliseconds timeout);

fetch_response
make_fetch_response(const model::topic_partition& tp, std::exception_ptr ex);

} // namespace kafka::client

/*
 * Copyright 2020 Vectorized, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "kafka/requests/fetch_request.h"

namespace pandaproxy::client {

kafka::fetch_request make_fetch_request(
  const model::topic_partition& tp,
  model::offset offset,
  int32_t max_bytes,
  std::chrono::milliseconds timeout);

kafka::fetch_response
make_fetch_response(const model::topic_partition& tp, std::exception_ptr ex);

} // namespace pandaproxy::client

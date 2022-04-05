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

#include "kafka/client/fwd.h"
#include "model/fundamental.h"
#include "model/record_batch_reader.h"

namespace kafka::client {

///\brief Adapt a kafka::client to fetch from a tp as a
/// kafka::record_batch_reader.
model::record_batch_reader make_client_fetch_batch_reader(
  kafka::client::client& client,
  model::topic_partition tp,
  model::offset first,
  model::offset last);

} // namespace kafka::client

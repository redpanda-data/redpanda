/*
 * Copyright 2021 Vectorized, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "kafka/client/client.h"
#include "model/fundamental.h"
#include "model/record_batch_reader.h"

namespace pandaproxy::schema_registry {

///\brief Adapt a kafka::client to fetch from a tp as a
/// kafka::record_batch_reader.
model::record_batch_reader make_client_fetch_batch_reader(
  kafka::client::client& client,
  model::topic_partition tp,
  model::offset first,
  model::offset last);

struct consumer_batch_reader {
    model::record_batch_reader rdr;
    ss::future<model::offset> caught_up;
};

///\brief Adapt a kafka::client consumer to fetch from a tp as a
/// kafka::record_batch_reader.
ss::future<consumer_batch_reader> make_client_consumer_batch_reader(
  kafka::client::client& client, model::topic_partition tp);

} // namespace pandaproxy::schema_registry

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

#include "kafka/types.h"
#include "model/fundamental.h"

#include <absl/container/flat_hash_map.h>

namespace kafka::client {

class fetch_session {
public:
    kafka::fetch_session_id id{kafka::invalid_fetch_session_id};
    kafka::fetch_session_epoch epoch{kafka::initial_fetch_session_epoch};
    absl::flat_hash_map<
      model::topic,
      absl::flat_hash_map<model::partition_id, model::offset>>
      offsets;
};

} // namespace kafka::client

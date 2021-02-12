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
#include "model/fundamental.h"
#include "model/metadata.h"
#include "model/namespace.h"
#include "model/record_batch_reader.h"
#include "model/record_batch_types.h"
#include "raft/types.h"
#include "storage/types.h"

#include <seastar/core/future.hh>

inline static model::topic_namespace make_ts(ss::sstring&& topic) {
    return model::topic_namespace(model::kafka_namespace, model::topic(topic));
}

inline static model::topic_namespace make_ts(const model::topic& topic) {
    return model::topic_namespace(model::kafka_namespace, topic);
}

/// Produces a batch with 1 record_batch which has 1 single record within it
model::record_batch_reader single_record_record_batch_reader();

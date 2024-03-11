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

#include "reflection/adl.h"
#include "storage/record_batch_builder.h"

inline model::record_batch make_batch(model::offset offset, size_t count) {
    storage::record_batch_builder builder(
      model::record_batch_type::raft_data, offset);
    for (size_t i = 0; i < count; ++i) {
        builder.add_raw_kv(reflection::to_iobuf(i + offset), iobuf());
    }
    return std::move(builder).build();
}

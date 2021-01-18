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

#include "raft/types.h"
#include "storage/record_batch_builder.h"

inline model::record_batch make_batch(model::offset offset, size_t count) {
    storage::record_batch_builder builder(raft::data_batch_type, offset);
    for (size_t i = 0; i < count; ++i) {
        builder.add_raw_kv(reflection::to_iobuf(i + offset), iobuf());
    }
    return std::move(builder).build();
}

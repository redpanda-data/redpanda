#pragma once

#include "raft/types.h"
#include "storage/record_batch_builder.h"

inline model::record_batch make_batch(model::offset offset, size_t count) {
    storage::record_batch_builder builder(
      raft::data_batch_type, model::offset(0));
    for (size_t i = 0; i < count; ++i) {
        builder.add_raw_kv(reflection::to_iobuf(i + offset), iobuf());
    }
    return std::move(builder).build();
}

/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#include "cloud_topics/level_one/metastore/tests/state_utils.h"

#include "cloud_topics/level_one/metastore/lsm/keys.h"
#include "cloud_topics/level_one/metastore/lsm/values.h"
#include "lsm/lsm.h"

namespace cloud_topics::l1 {

state snapshot_to_state(lsm::database& db) {
    state result;
    auto snap = db.create_snapshot();
    auto iter = snap.create_iterator().get();

    for (iter.seek_to_first().get(); iter.valid(); iter.next().get()) {
        auto key = iter.key();
        auto val = iter.value();
        if (auto meta_key = metadata_row_key::decode(key)) {
            auto meta_val = serde::from_iobuf<metadata_row_value>(
              std::move(val));
            auto& p_state = result.topic_to_state[meta_key->tidp.topic_id]
                              .pid_to_state[meta_key->tidp.partition];
            p_state.start_offset = meta_val.start_offset;
            p_state.next_offset = meta_val.next_offset;
            p_state.compaction_epoch = meta_val.compaction_epoch;
        } else if (auto ext_key = extent_row_key::decode(key)) {
            auto ext_val = serde::from_iobuf<extent_row_value>(std::move(val));
            extent e{
              .base_offset = ext_key->base_offset,
              .last_offset = ext_val.last_offset,
              .max_timestamp = ext_val.max_timestamp,
              .filepos = ext_val.filepos,
              .len = ext_val.len,
              .oid = ext_val.oid,
            };
            result.topic_to_state[ext_key->tidp.topic_id]
              .pid_to_state[ext_key->tidp.partition]
              .extents.insert(e);
        } else if (auto term_key = term_row_key::decode(key)) {
            auto term_val = serde::from_iobuf<term_row_value>(std::move(val));
            term_start ts{
              .term_id = term_key->term,
              .start_offset = term_val.term_start_offset,
            };
            result.topic_to_state[term_key->tidp.topic_id]
              .pid_to_state[term_key->tidp.partition]
              .term_starts.insert(ts);
        } else if (auto comp_key = compaction_row_key::decode(key)) {
            auto comp_val = serde::from_iobuf<compaction_row_value>(
              std::move(val));
            result.topic_to_state[comp_key->tidp.topic_id]
              .pid_to_state[comp_key->tidp.partition]
              .compaction_state = std::move(comp_val.state);
        } else if (auto obj_key = object_row_key::decode(key)) {
            auto obj_val = serde::from_iobuf<object_row_value>(std::move(val));
            result.objects[obj_key->oid] = obj_val.object;
        }
    }
    return result;
}

} // namespace cloud_topics::l1

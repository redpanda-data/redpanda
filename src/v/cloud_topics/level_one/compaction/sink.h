/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#pragma once

#include "cloud_topics/level_one/common/abstract_io.h"
#include "cloud_topics/level_one/common/object.h"
#include "cloud_topics/level_one/compaction/committer.h"
#include "compaction/reducer.h"
#include "container/chunked_vector.h"
#include "model/fundamental.h"
#include "model/timestamp.h"

namespace cloud_topics::l1 {

class compaction_sink : public compaction::sliding_window_reducer::sink {
public:
    compaction_sink(
      l1::io*,
      compaction_committer*,
      model::topic_id_partition,
      object_builder::options = {});

    ss::future<ss::stop_iteration>
    operator()(model::record_batch, model::compression) final;

    ss::future<> finalize() final;

private:
    // Returns `true` if the current object represented by
    // `_active_staging_file` and `_builder` should be rolled.
    bool needs_roll() const;

    // Pushes the current object represented by `_active_staging_file` and
    // `_builder` to the `_committer`. Leaves `_active_staging_file` and
    // `_builder` as `nullptr`.
    ss::future<> commit_update_and_roll();

    // May commit the current object if `needs_roll()`. Leaves
    // `_active_staging_file` and `_builder` in a set state.
    ss::future<> maybe_roll();

    // Resets metadata (base_offset, max_offset, max_timestamp) to uninitalized
    // values. Must be called after rolling builder/active_staging_file.
    void reset_metadata();

    // Updates metadata (base_offset, max_offset, max_timestamp) with data from
    // batch. Should be called for every batch processed.
    void update_metadata(const model::record_batch&);

private:
    io* _io;
    compaction_committer* _committer;

    model::topic_id_partition _tp;
    const object_builder::options _opts;

    std::unique_ptr<staging_file> _active_staging_file{nullptr};
    // Guaranteed to have a value iff _active_staging_file.
    std::unique_ptr<object_builder> _builder{nullptr};
};

} // namespace cloud_topics::l1

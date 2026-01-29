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

#include "cloud_topics/level_one/metastore/metastore.h"
#include "model/fundamental.h"

#include <seastar/coroutine/generator.hh>

#include <expected>

namespace cloud_topics::l1 {

// Fetches extents from the `metastore` for the provided `tp` within the range
// `[min_offset, max_offset]` inclusively. Iteration (and thereby the order in
// which extents are provided) can be forwards or backwards.
// For example, for an extent state of `[[0,9],[10,19],[20,29]]` and a
// construction like `extent_metadata_reader([0,15,forwards])`, the generator
// is expected to yield the extents `[[0,9],[10,19]]`. For a construction like
// `extent_metadata_reader([0,15,backwards])`, the generator is expected to
// yield `[[10,19],[0,9]]`.
// If an error from the `metastore` is received, this error is yielded to the
// user. The `extent_metadata_generator` will be left in a valid state if the
// user wishes to continue retrying.
class extent_metadata_reader {
public:
    // The direction of iteration. Dictates which extent metadata request to the
    // `metastore` is made and the order in which extents are yielded.
    enum class iteration_direction { forwards, backwards };

    using extent_metadata_generator = ss::coroutine::experimental::generator<
      std::expected<metastore::extent_metadata, metastore::errc>>;

    // Some sane default for the max number of extents returned per RPC request
    // to the `metastore`. Can be overridden with argument to
    // `extent_metadata_reader` constructor.
    static constexpr size_t default_num_extents_per_request = 100;

    extent_metadata_reader(
      metastore*,
      model::topic_id_partition,
      kafka::offset,
      kafka::offset,
      iteration_direction,
      ss::abort_source&,
      std::optional<size_t> = std::nullopt);

    // Creates an `extent_metadata_generator`, which can be iterated over or
    // `co_await`'ed directly to retrieve extents.
    extent_metadata_generator generator();

private:
    // Implementation for `iteration_direction::forwards`.
    extent_metadata_generator forward_generator();
    // Implementation for `iteration_direction::backwards`.
    extent_metadata_generator backward_generator();

    metastore* _metastore{nullptr};

    model::topic_id_partition _tp;
    kafka::offset _min_offset;
    kafka::offset _max_offset;
    iteration_direction _iter_dir;
    ss::abort_source& _as;
    size_t _num_extents_per_request;
};

} // namespace cloud_topics::l1

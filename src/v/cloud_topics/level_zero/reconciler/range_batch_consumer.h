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

#include "absl/container/btree_map.h"
#include "bytes/iobuf.h"
#include "model/fundamental.h"
#include "model/record.h"
#include "model/timestamp.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/future.hh>

#include <optional>

namespace experimental::cloud_topics::reconciler {

/*
 * metadata about a range of batches.
 */
struct range_info {
    kafka::offset base_offset;
    kafka::offset last_offset;
    model::timestamp base_timestamp;
    model::timestamp last_timestamp;
    // 'range_info' is not aligned by term boundary so this
    // map is used to track term changes
    absl::btree_map<model::term_id, kafka::offset> terms;
};

/*
 * a materialized range of batches.
 */
struct range {
    iobuf data;
    range_info info;
};

/*
 * Consumer that builds a range from a record batch reader.
 */
class range_batch_consumer {
public:
    ss::future<ss::stop_iteration> operator()(model::record_batch);
    std::optional<range> end_of_stream();

private:
    range _range;
    std::optional<kafka::offset> _base_offset;
};

} // namespace experimental::cloud_topics::reconciler

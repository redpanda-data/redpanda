/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
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

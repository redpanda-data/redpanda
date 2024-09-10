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

#include "bytes/iobuf.h"
#include "model/fundamental.h"
#include "model/record.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/future.hh>

#include <optional>

namespace experimental::cloud_topics::reconciler {

/*
 * metadata about a range of batches.
 */
struct range_info {
    model::offset base_offset;
    model::offset last_offset;
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
    std::optional<model::offset> _base_offset;
};

} // namespace experimental::cloud_topics::reconciler

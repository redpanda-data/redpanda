/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#pragma once

#include "base/seastarx.h"
#include "cloud_topics/batcher/write_request.h"
#include "cloud_topics/errc.h"
#include "cloud_topics/types.h"
#include "container/fragmented_vector.h"
#include "model/record.h"

#include <seastar/core/circular_buffer.hh>
#include <seastar/core/weak_ptr.hh>

#include <absl/container/btree_map.h>

namespace experimental::cloud_topics::details {

/// List of placeholder batches that has to be propagated
/// to the particular write request.
template<class Clock>
struct batches_for_req {
    /// Generated placeholder batches
    ss::circular_buffer<model::record_batch> placeholders;
    /// Source write request
    ss::weak_ptr<write_request<Clock>> ref;
};

// This component aggregates a bunch of write
// requests and produces single serialized object.
template<class Clock>
class aggregator {
public:
    explicit aggregator(object_id id = object_id{uuid_t::create()});
    aggregator(const aggregator&) = delete;
    aggregator(aggregator&&) = delete;
    aggregator& operator=(const aggregator&) = delete;
    aggregator& operator=(aggregator&&) = delete;
    ~aggregator();

    /// Add content of the write request to the
    /// L0 object.
    /// If write request is destroyed before the 'prepare'
    /// call the content of the write request will not be
    /// included into L0 object. The size value returned by
    /// the 'size_bytes' call will not match the actual size
    /// of the object.
    void add(write_request<Clock>& req);

    /// Estimate L0 object size
    size_t size_bytes() const noexcept;

    /// Prepare upload byte stream
    iobuf prepare();

    object_id get_object_id() const noexcept;

    void ack();
    void ack_error(errc);

private:
    /// Generate placeholders.
    /// This method should be invoked before 'get_result'
    chunked_vector<std::unique_ptr<batches_for_req<Clock>>> get_placeholders();

    /// Produce L0 object payload.
    /// The method messes up the state so it can only
    /// be called once.
    iobuf get_stream();

    object_id _id;
    /// Source data for the aggregator
    absl::btree_map<model::ntp, write_request_list<Clock>> _staging;
    /// Prepared placeholders
    chunked_vector<std::unique_ptr<batches_for_req<Clock>>> _aggregated;
    size_t _size_bytes{0};
};

} // namespace experimental::cloud_topics::details

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

#include "absl/container/btree_map.h"
#include "base/seastarx.h"
#include "cloud_topics/errc.h"
#include "cloud_topics/level_zero/pipeline/write_request.h"
#include "cloud_topics/types.h"
#include "container/chunked_vector.h"

#include <seastar/core/weak_ptr.hh>

namespace experimental::cloud_topics::l0 {

/// List of extent_meta values that has to be propagated
/// to the particular write request.
template<class Clock>
struct extents_for_req {
    /// Generated placeholder batches
    chunked_vector<extent_meta> extents;
    /// Source write request
    ss::weak_ptr<l0::write_request<Clock>> ref;
};

// This component aggregates a bunch of write
// requests and produces single serialized object.
template<class Clock>
class aggregator {
public:
    explicit aggregator(object_id id);
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
    void add(l0::write_request<Clock>& req);

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
    chunked_vector<std::unique_ptr<extents_for_req<Clock>>> get_extents();

    /// Produce L0 object payload.
    /// The method messes up the state so it can only
    /// be called once.
    iobuf get_stream();

    object_id _id;

    /// Source data for the aggregator
    absl::btree_map<model::ntp, l0::write_request_list<Clock>> _staging;
    /// Prepared placeholders
    chunked_vector<std::unique_ptr<extents_for_req<Clock>>> _aggregated;
    size_t _size_bytes{0};
};

} // namespace experimental::cloud_topics::l0

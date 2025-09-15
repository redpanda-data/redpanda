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

#include "container/chunked_vector.h"
#include "model/fundamental.h"
#include "model/record.h"
#include "model/timeout_clock.h"
#include "raft/replicate.h"
#include "ssx/semaphore.h"

namespace cluster_link::replication {

/**
 * Interface to which the data from source is replicated to.
 */
class data_sink {
public:
    virtual ~data_sink() = default;

    virtual ss::future<> start() = 0;
    virtual ss::future<> stop() noexcept = 0;

    virtual kafka::offset last_replicated_offset() const = 0;

    virtual raft::replicate_stages replicate(
      chunked_vector<::model::record_batch> batches,
      ::model::timeout_clock::duration timeout,
      ss::abort_source& as)
      = 0;

    // Notifies the sink of any terminal failure that can
    // result in replicator not being able to start/progress.
    virtual void notify_replicator_failure(::model::term_id) = 0;
};

class data_sink_factory {
public:
    virtual ~data_sink_factory() = default;
    virtual std::unique_ptr<data_sink> make_sink(const ::model::ntp&) = 0;
};

/**
 * Interface to fetch the data from.
 */
class data_source {
public:
    virtual ~data_source() = default;

    virtual ss::future<> start() = 0;
    virtual ss::future<> stop() noexcept = 0;

    /**
     * Reset the data source to its initial state.
     * fetching from the given offset.
     */
    virtual ss::future<> reset(kafka::offset) = 0;

    struct data {
        chunked_vector<::model::record_batch> batches;
        ssx::semaphore_units units;
    };

    /**
     * Fetches some data, if any.
     */
    virtual ss::future<data> fetch_next(ss::abort_source&) = 0;
};

class data_source_factory {
public:
    virtual ss::future<> start() = 0;
    virtual ss::future<> stop() noexcept = 0;
    virtual ~data_source_factory() = default;
    virtual std::unique_ptr<data_source> make_source(const ::model::ntp&) = 0;
};

} // namespace cluster_link::replication

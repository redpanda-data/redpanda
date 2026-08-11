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

#include "cluster_link/replication/deps.h"
#include "cluster_link/replication/types.h"

#include <seastar/core/gate.hh>

namespace cluster_link::replication::tests {

class test_config_provider : public link_configuration_provider {
public:
    ss::future<kafka::offset>
    start_offset(const ::model::ntp&, ss::abort_source&) override;
};

class accounting_sink : public data_sink {
public:
    ss::future<> start() override;
    ss::future<> stop() noexcept override;
    ss::future<> reset() override;
    kafka::offset last_replicated_offset() const override;
    raft::replicate_stages replicate(
      chunked_vector<::model::record_batch> batches,
      ::model::timeout_clock::duration timeout,
      ss::abort_source& as) override;
    void notify_replicator_failure(::model::term_id) override;
    kafka::offset high_watermark() const final;
    bool can_prefix_truncate() const final;
    ss::future<kafka::error_code> prefix_truncate(
      kafka::offset truncation_offset,
      ss::lowres_clock::time_point deadline) final;
    kafka::offset start_offset() final;
    ss::future<> maybe_sync_pid() final { return ss::now(); }

private:
    size_t _records_consumed;
    kafka::offset _start_offset{0};
    kafka::offset _last_offset{};
    ss::gate _gate;
};

class random_data_source : public data_source {
public:
    ss::future<> start(kafka::offset) noexcept override;
    ss::future<> stop() noexcept override;
    ss::future<> reset(kafka::offset) override;
    ss::future<fetch_data> fetch_next(ss::abort_source&) override;
    std::optional<source_partition_offsets_report> get_offsets() final;

private:
    kafka::offset _next{};
    ss::gate _gate;
};

class random_data_source_factory : public data_source_factory {
public:
    ss::future<> start() override { return ss::now(); }
    ss::future<> stop() noexcept override { return ss::now(); }
    std::unique_ptr<data_source> make_source(const ::model::ntp&) override;
};

class accounting_sink_factory : public data_sink_factory {
public:
    std::unique_ptr<data_sink> make_sink(const ::model::ntp&) override;
};
} // namespace cluster_link::replication::tests

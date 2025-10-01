/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cluster/fwd.h"
#include "cluster_link/replication/deps.h"
#include "kafka/protocol/list_offset.h"

#include <seastar/core/gate.hh>
#include <seastar/core/sharded.hh>

namespace kafka {
class write_at_offset_stm;
};

namespace cluster_link::replication {

class mux_remote_consumer;
/*
 * Source backed by partition data on a remote cluster.
 */
class remote_partition_source : public data_source {
public:
    explicit remote_partition_source(
      ::model::topic_partition tp,
      mux_remote_consumer& consumer,
      ::model::timestamp starting_offset)
      : _tp(std::move(tp))
      , _consumer(consumer)
      , _starting_offset(starting_offset) {}
    ss::future<> start(kafka::offset) override;
    ss::future<> stop() noexcept override;
    ss::future<> reset(kafka::offset) override;
    ss::future<data_source::data> fetch_next(ss::abort_source&) override;
    kafka::offset last_seen_log_start_offset() final;

private:
    ss::future<kafka::offset> fetch_starting_offset();
    ss::future<kafka::offset> do_fetch_starting_offset();
    ss::future<kafka::list_offset_partition_response> do_list_offset();
    ss::future<std::optional<std::tuple<::model::node_id, kafka::leader_epoch>>>
    get_leader_and_epoch_for_ntp();

    std::optional<std::tuple<::model::node_id, kafka::leader_epoch>>
    do_get_leader_and_epoch_for_ntp();

    ss::future<std::optional<kafka::api_version>> get_list_offset_api_version();

private:
    ::model::topic_partition _tp;
    mux_remote_consumer& _consumer;
    ::model::timestamp _starting_offset;
    ss::gate _gate;
    ss::abort_source _as;
};

class remote_data_source_factory : public data_source_factory {
public:
    explicit remote_data_source_factory(std::unique_ptr<mux_remote_consumer>);
    ~remote_data_source_factory() override;
    ss::future<> start() override;
    ss::future<> stop() noexcept override;
    std::unique_ptr<data_source>
    make_source(const ::model::ntp&, ::model::timestamp) override;

private:
    std::unique_ptr<mux_remote_consumer> _consumer;
};

/*
 * Sink for writing partition data to the partition leader on the local shard.
 */
class local_partition_sink : public data_sink {
public:
    explicit local_partition_sink(ss::lw_shared_ptr<cluster::partition>);
    ss::future<> start() override;
    ss::future<> stop() noexcept override;
    kafka::offset last_replicated_offset() const override;
    raft::replicate_stages replicate(
      chunked_vector<::model::record_batch> batches,
      ::model::timeout_clock::duration timeout,
      ss::abort_source& as) override;
    void notify_replicator_failure(::model::term_id) override;
    ss::future<> maybe_trim_prefix(kafka::offset) final;

private:
    ss::gate _gate;
    ss::lw_shared_ptr<cluster::partition> _partition;
    ss::shared_ptr<kafka::write_at_offset_stm> _stm;
    // set in start();
    std::optional<kafka::offset> _last_replicated_offset;
};

class local_partition_data_sink_factory : public data_sink_factory {
public:
    explicit local_partition_data_sink_factory(
      ss::sharded<cluster::partition_manager>& pm)
      : _partition_manager(pm) {}
    std::unique_ptr<data_sink> make_sink(const ::model::ntp&) override;

private:
    ss::sharded<cluster::partition_manager>& _partition_manager;
};

} // namespace cluster_link::replication

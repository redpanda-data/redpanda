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

#include "cluster_link/replication/partition_data_queue.h"
#include "container/chunked_hash_map.h"
#include "kafka/client/direct_consumer/direct_consumer.h"

#include <expected>
#include <memory>

class MuxConsumerFixture;

namespace cluster_link::replication {

/**
 * @brief Multiplexed remote consumer for managing multiple partition consumers
 *
 * This class provides a multiplexed consumer that can manage multiple
 * partitions across different topics using a single underlying Kafka consumer.
 * It handles fetching data from remote partitions and distributing it to
 * individual partition data queues. This is done so that an individual slow
 * partition does not block the fetching of data from remaining partitions.
 *
 * The consumer operates with a fetch loop that retrieves data from all assigned
 * partitions and buffers it according to the configured limits. Partitions can
 * be dynamically added or removed during runtime depending on if their queue is
 * full.
 */
class mux_remote_consumer {
public:
    enum class errc : int8_t {
        partition_not_found = 1,
        partition_already_exists = 2,
    };
    using result = std::expected<void, errc>;

    explicit mux_remote_consumer(
      std::unique_ptr<kafka::client::direct_consumer> consumer,
      size_t partition_max_buffered,
      std::chrono::milliseconds fetch_max_wait)
      : _consumer(std::move(consumer))
      , _partition_max_buffered(partition_max_buffered)
      , _fetch_max_wait(fetch_max_wait) {}

    ss::future<> start();
    ss::future<> stop() noexcept;

    result add(const ::model::topic_partition&, kafka::offset);
    ss::future<result> remove(const ::model::topic_partition&);

    /**
     * Reset the state of a partition to the provided offset.
     * Aborts any inflight fetch with abort_requested_exception.
     * Any subsequent fetch will start from the new offset.
     *
     * Also used to set the seed offset of fetches after add().
     */
    result reset(const ::model::topic_partition& tp, kafka::offset);

    /**
     * Fetch data for the specified partition. Pre-reqs for a fetch
     * are add() and reset() (atleast once). add() adds the partition to the
     * consumer and reset() sets the initial offset for fetching.
     */
    ss::future<std::expected<partition_data_queue::fetch_data, errc>>
    fetch(const ::model::topic_partition&, ss::abort_source&);

private:
    bool can_ignore_partition_data(const ::model::topic_partition&);
    ss::future<> assign_pending_partitions();
    ss::future<> fetch_loop();
    ss::future<>
      process_fetched_data(chunked_vector<kafka::client::fetched_topic_data>);
    // A ptr to allow std::move() without closing the queue gate.
    chunked_hash_map<
      ::model::topic_partition,
      std::unique_ptr<partition_data_queue>>
      _partitions;
    std::unique_ptr<kafka::client::direct_consumer> _consumer;
    /// Maps topics to their pending partition assignments that are waiting to
    /// be processed. Each topic is associated with a set of partition IDs that
    /// need to be assigned to this consumer but have not yet been fully
    /// processed.
    chunked_hash_map<::model::topic, absl::btree_set<::model::partition_id>>
      _pending_assignment;
    size_t _partition_max_buffered;
    std::chrono::milliseconds _fetch_max_wait;
    ss::gate _gate;
    ss::condition_variable _cv;
    friend class ::MuxConsumerFixture;
};

} // namespace cluster_link::replication

template<>
struct fmt::formatter<cluster_link::replication::mux_remote_consumer::errc>
  : fmt::formatter<std::string_view> {
    auto format(
      const cluster_link::replication::mux_remote_consumer::errc& e,
      fmt::format_context& ctx) const -> decltype(ctx.out());
};

// To keep google tests happy
std::ostream& operator<<(
  std::ostream& os,
  const cluster_link::replication::mux_remote_consumer::errc&);

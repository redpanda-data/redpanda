/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */
#pragma once
#include "base/seastarx.h"
#include "container/chunked_hash_map.h"
#include "kafka/client/cluster.h"
#include "kafka/client/direct_consumer/api_types.h"

namespace kafka {
struct metadata_response_data;
namespace client {

class fetcher;
class data_queue;
/**
 * Simple direct Kafka consumer. (named after franz-go direct consumer which
 * does not use consumer groups) It allows user to subscribe to a list of topic
 * partitions. The consumer maintains broker fetch session, it reacts for the
 * leadership changes and metadata updates. Fetch responses from all the brokers
 * are exposed through `fetch_next()`. The fetch_next() returns whenever any
 * ready to be consumer are available.
 *
 * Implementation details:
 * NOTE:
 * Fetcher and data queue classes should not be used directly, they are
 * implementation details of the direct consumer, they were placed into separate
 * files for readability.
 *
 * Direct consumer uses fetcher class to fetch data from the brokers. The
 * fetcher is responsible for fetching data from a single broker. It maintains a
 * list of partitions and corresponding fetch offsets. The fetcher loop
 * constantly queries the broker for data and updates the fetch offsets based on
 * the received data. Fetcher is also responsible for fetching offsets to apply
 * the reset policy. Fetcher puts the fetched data into the parent consumer's
 * data queue. The queue is then accessed through the `fetch_next()`
 * method.
 *
 * The direct consumer maintains a list of partition subscriptions. It does not
 * use the consumer groups to control the list of partitions to fetch from. When
 * the subscription list is changed the consumer updates the fetchers state.
 *
 */
class direct_consumer {
public:
    struct configuration {
        int32_t min_bytes{1};
        int32_t max_fetch_size{512_KiB};
        int32_t partition_max_bytes{128_KiB};
        offset_reset_policy reset_policy = offset_reset_policy::earliest;
        std::chrono::milliseconds max_wait_time{100};
        model::isolation_level isolation_level
          = model::isolation_level::read_uncommitted;
        // queue settings
        size_t max_buffered_bytes{10_MiB};
        size_t max_buffered_elements{10};
        // fetch sessions enabled by default
        fetch_sessions_enabled with_sessions{fetch_sessions_enabled::yes};
        friend std::ostream& operator<<(std::ostream&, const configuration&);
    };

    direct_consumer(cluster& cluster, configuration cfg);

    ~direct_consumer();
    /**
     * Starts the consumer, this method will start the fetchers and
     * initialize the data queue.
     * It will also register the metadata update callback.
     * The method will return when the consumer is ready to fetch data.
     *
     * Not started consumer can still accept assignments, but the fetchers will
     * only start after this method is called.
     */
    ss::future<> start();
    /**
     * Stops consumer
     */
    ss::future<> stop();

    /**
     * Returns all data available to fetch. If the timeout passed to this method
     * is reached and there were no data to consume it will return an empty
     * vector.
     * The method returns an error whenever the consumer receives a
     * non-retriable error from any of the brokers.
     */
    ss::future<fetches> fetch_next(std::chrono::milliseconds timeout);

    /**
     * Assign partitions to be fetched from, if partition is already being
     * fetched from and is included in the vector its fetch offset will be
     * updated
     */
    ss::future<> assign_partitions(chunked_vector<topic_assignment>);

    /**
     * Removes listed topics from assignment.
     *
     * NOTE: if the topic is not present in the current assignment it will be
     * ignored.
     */
    ss::future<> unassign_topics(chunked_vector<model::topic> topics);

    /**
     * Removes listed topic partitions from assignment.
     *
     * NOTE: if the topic is not present in the current assignment it will be
     * ignored.
     */
    ss::future<> unassign_partitions(
      chunked_vector<model::topic_partition> topic_partitions);

    /**
     *  Updates consumer configuration, the configuration will be update when
     * for the next fetch request.
     */
    void update_configuration(configuration cfg);

private:
    struct subscription {
        std::optional<model::node_id> current_fetcher;
        std::optional<kafka::offset> fetch_offset;
    };
    friend class fetcher;
    void on_metadata_update(const metadata_update&);

    ss::future<> handle_metadata_update();
    ss::future<> update_fetchers(
      mutex::units lock_holder,
      topic_partition_map<subscription> removals = {});

    fetcher& get_fetcher(model::node_id id);

    cluster* _cluster;

    offset_reset_policy _reset_policy
      = offset_reset_policy::earliest; // default to earliest

    configuration _config;

    // serialize updates to _subscriptions
    mutex _subscriptions_lock{"direct_consumer::_subscriptions_lock"};
    topic_partition_map<subscription> _subscriptions;
    chunked_hash_map<model::node_id, std::unique_ptr<fetcher>> _broker_fetchers;
    std::unique_ptr<data_queue> _fetched_data_queue;
    ss::condition_variable _data_available;

    cluster::callback_id _metadata_callback_id;
    bool _started = false;
    ss::gate _gate;

public:
    // For testing
    const topic_partition_map<subscription>& subscriptions() const {
        return _subscriptions;
    }
};
} // namespace client
} // namespace kafka

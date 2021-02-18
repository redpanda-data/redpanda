/*
 * Copyright 2020 Vectorized, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "cluster/types.h"
#include "model/metadata.h"
#include "model/timestamp.h"
#include "seastarx.h"
#include "utils/expiring_promise.h"

#include <seastar/core/future.hh>
#include <seastar/core/sharded.hh>

#include <absl/container/flat_hash_map.h>

namespace cluster {

/// Metadata cache provides all informationrequired to fill Kafka metadata
/// response. metadata dissemination service. MetadataCache is the facade over
/// cluster state distributed in separate components. The metadata cache
/// core-affinity is independent from the actual state location as the Metadata
/// cache facade, for simplicity, is instantiated on every core.
/// MetadaCache itself does not hold any state
///```plain
///
///   Kafka API                  Kafka Proxy
///       +                           +
///       |                           |
///       |                           |
///       |                           |
///       |                           |
/// +-----v---------------------------v--------+
/// |                                          |
/// |         Metadata Cache (facade)          |
/// |                                          |
/// +----+-------------+----------------+------+
///      |             |                |
/// +----v----+   +----v-----+    +-----v------+
/// | Members |   |  Topics  |    |  Leaders   |
/// +---------+   +----------+    +------------+
class metadata_cache {
public:
    metadata_cache(
      ss::sharded<topic_table>&,
      ss::sharded<members_table>&,
      ss::sharded<partition_leaders_table>&);

    ss::future<> stop() { return ss::now(); }

    /// Returns list of all topics that exists in the cluster.
    std::vector<model::topic_namespace> all_topics() const;

    ///\brief Returns metadata of single topic.
    ///
    /// If topic does not exists it returns an empty optional
    std::optional<model::topic_metadata>
      get_topic_metadata(model::topic_namespace_view) const;

    ///\brief Returns configuration of single topic.
    ///
    /// If topic does not exists it returns an empty optional
    std::optional<topic_configuration>
      get_topic_cfg(model::topic_namespace_view) const;

    ///\brief Returns topics timestamp type
    ///
    /// If topic does not exists it returns an empty optional
    std::optional<model::timestamp_type>
      get_topic_timestamp_type(model::topic_namespace_view) const;

    /// Returns metadata of all topics.
    std::vector<model::topic_metadata> all_topics_metadata() const;

    /// Returns all brokers, returns copy as the content of broker can change
    std::vector<broker_ptr> all_brokers() const;

    /// Returns all broker ids
    std::vector<model::node_id> all_broker_ids() const;

    /// Returns single broker if exists in cache,returns copy as the content of
    /// broker can change
    std::optional<broker_ptr> get_broker(model::node_id) const;

    bool contains(model::topic_namespace_view, model::partition_id) const;

    /// Returns metadata of all topics in cache internal format
    // const cache_t& all_metadata() const { return _cache; }

    /**
     * Return the leader of a partition with a timeout.
     *
     * If the partition leader is set then the leader's node id is returned as a
     * ready future. Otherwise, wait up to the specified timeout for a leader to
     * be elected.
     */
    ss::future<model::node_id> get_leader(
      const model::ntp&,
      ss::lowres_clock::time_point,
      std::optional<std::reference_wrapper<ss::abort_source>> = std::nullopt);

    /// If present returns a leader of raft0 group
    std::optional<model::node_id> get_controller_leader_id();

private:
    ss::sharded<topic_table>& _topics_state;
    ss::sharded<members_table>& _members_table;
    ss::sharded<partition_leaders_table>& _leaders;
};
} // namespace cluster

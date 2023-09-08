/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */
#pragma once

#include "cluster/fwd.h"
#include "cluster/types.h"
#include "kafka/server/partition_proxy.h"
#include "model/fundamental.h"
#include "model/ktp.h"
#include "model/metadata.h"

#include <seastar/util/noncopyable_function.hh>

/**
 * These are wrapper types for types defined outside this module, useful for
 * testing without spinning up a full Redpanda cluster.
 */

namespace transform::rpc {

/**
 * A cache for which node owns a given partition leader.
 */
class partition_leader_cache {
public:
    partition_leader_cache() = default;
    partition_leader_cache(const partition_leader_cache&) = delete;
    partition_leader_cache(partition_leader_cache&&) = delete;
    partition_leader_cache& operator=(const partition_leader_cache&) = delete;
    partition_leader_cache& operator=(partition_leader_cache&&) = delete;
    virtual ~partition_leader_cache() = default;

    static std::unique_ptr<partition_leader_cache>
    make_default(ss::sharded<cluster::partition_leaders_table>*);

    /**
     * Lookup which node owns a given ntp's leader.
     */
    std::optional<model::node_id> get_leader_node(const model::ntp& ntp) const;

    virtual std::optional<model::node_id>
      get_leader_node(model::topic_namespace_view, model::partition_id) const
      = 0;
};

/**
 * A cache for topic/partition metadata.
 */
class topic_metadata_cache {
public:
    topic_metadata_cache() = default;
    topic_metadata_cache(const topic_metadata_cache&) = delete;
    topic_metadata_cache(topic_metadata_cache&&) = delete;
    topic_metadata_cache& operator=(const topic_metadata_cache&) = delete;
    topic_metadata_cache& operator=(topic_metadata_cache&&) = delete;
    virtual ~topic_metadata_cache() = default;

    static std::unique_ptr<topic_metadata_cache>
    make_default(ss::sharded<cluster::metadata_cache>*);

    /**
     * Lookup the topic configuration.
     */
    virtual std::optional<cluster::topic_configuration>
      find_topic_cfg(model::topic_namespace_view) const = 0;

    /**
     * The default max batch bytes size.
     */
    virtual uint32_t get_default_batch_max_bytes() const = 0;
};

/**
 * Handles routing for shard local partitions.
 */
class partition_manager {
public:
    partition_manager() = default;
    partition_manager(const partition_manager&) = delete;
    partition_manager(partition_manager&&) = delete;
    partition_manager& operator=(const partition_manager&) = delete;
    partition_manager& operator=(partition_manager&&) = delete;
    virtual ~partition_manager() = default;

    static std::unique_ptr<partition_manager> make_default(
      ss::sharded<cluster::shard_table>*,
      ss::sharded<cluster::partition_manager>*);

    /**
     * Lookup which shard owns a particular ntp.
     */
    virtual std::optional<ss::shard_id> shard_owner(const model::ktp& ktp);
    virtual std::optional<ss::shard_id> shard_owner(const model::ntp&) = 0;

    /**
     * Invoke a function on the ntp's leader shard.
     *
     * Will return cluster::errc::not_leader if the shard_id is incorrect for
     * that ntp.
     */
    virtual ss::future<cluster::errc> invoke_on_shard(
      ss::shard_id shard_id,
      const model::ktp& ktp,
      ss::noncopyable_function<
        ss::future<cluster::errc>(kafka::partition_proxy*)> fn);

    virtual ss::future<cluster::errc> invoke_on_shard(
      ss::shard_id,
      const model::ntp&,
      ss::noncopyable_function<
        ss::future<cluster::errc>(kafka::partition_proxy*)>)
      = 0;
};

}; // namespace transform::rpc

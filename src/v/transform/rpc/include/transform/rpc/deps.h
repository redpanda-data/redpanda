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
#include "model/transform.h"
#include "transform/rpc/serde.h"

#include <seastar/util/noncopyable_function.hh>

/**
 * These are wrapper types for types defined outside this module, useful for
 * testing without spinning up a full Redpanda cluster.
 */

namespace transform::rpc {

/**
 * Able to report on the state of all transforms for this node.
 */
class reporter {
public:
    reporter() = default;
    reporter(const reporter&) = delete;
    reporter& operator=(const reporter&) = delete;
    reporter(reporter&&) = delete;
    reporter& operator=(reporter&&) = delete;
    virtual ~reporter() = default;

    virtual ss::future<model::cluster_transform_report> compute_report() = 0;
};

/**
 * A cache for all the nodes that exist in the cluster.
 */
class cluster_members_cache {
public:
    cluster_members_cache() = default;
    cluster_members_cache(const cluster_members_cache&) = delete;
    cluster_members_cache& operator=(const cluster_members_cache&) = delete;
    cluster_members_cache(cluster_members_cache&&) = delete;
    cluster_members_cache& operator=(cluster_members_cache&&) = delete;
    virtual ~cluster_members_cache() = default;

    static std::unique_ptr<cluster_members_cache>
    make_default(ss::sharded<cluster::members_table>*);

    /**
     * A list of all the nodes in the cluster, including the local node.
     */
    virtual std::vector<model::node_id> all_cluster_members() = 0;
};

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
 * A component that can create topics.
 */
class topic_creator {
public:
    topic_creator() = default;
    topic_creator(const topic_creator&) = default;
    topic_creator(topic_creator&&) = delete;
    topic_creator& operator=(const topic_creator&) = default;
    topic_creator& operator=(topic_creator&&) = delete;
    virtual ~topic_creator() = default;

    static std::unique_ptr<topic_creator> make_default(cluster::controller*);

    /**
     * Create a topic.
     */
    virtual ss::future<cluster::errc> create_topic(
      model::topic_namespace_view,
      int32_t partition_count,
      cluster::topic_properties)
      = 0;

    /**
     * Update a topic.
     */
    virtual ss::future<cluster::errc>
      update_topic(cluster::topic_properties_update) = 0;
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
      ss::sharded<cluster::partition_manager>*,
      ss::smp_service_group smp_group);

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
    virtual ss::future<result<model::offset, cluster::errc>> invoke_on_shard(
      ss::shard_id shard_id,
      const model::ktp& ktp,
      ss::noncopyable_function<ss::future<result<model::offset, cluster::errc>>(
        kafka::partition_proxy*)> fn)
      = 0;
    virtual ss::future<result<model::offset, cluster::errc>> invoke_on_shard(
      ss::shard_id,
      const model::ntp&,
      ss::noncopyable_function<ss::future<result<model::offset, cluster::errc>>(
        kafka::partition_proxy*)>)
      = 0;

    virtual ss::future<result<model::wasm_binary_iobuf, cluster::errc>>
    invoke_on_shard(
      ss::shard_id shard_id,
      const model::ktp& ktp,
      ss::noncopyable_function<
        ss::future<result<model::wasm_binary_iobuf, cluster::errc>>(
          kafka::partition_proxy*)>)
      = 0;
    virtual ss::future<result<model::wasm_binary_iobuf, cluster::errc>>
    invoke_on_shard(
      ss::shard_id,
      const model::ntp&,
      ss::noncopyable_function<
        ss::future<result<model::wasm_binary_iobuf, cluster::errc>>(
          kafka::partition_proxy*)>)
      = 0;

    virtual ss::future<find_coordinator_response>
    invoke_on_shard(ss::shard_id, const model::ntp&, find_coordinator_request)
      = 0;

    virtual ss::future<offset_commit_response>
    invoke_on_shard(ss::shard_id, const model::ntp&, offset_commit_request) = 0;

    virtual ss::future<offset_fetch_response>
    invoke_on_shard(ss::shard_id, const model::ntp&, offset_fetch_request) = 0;

    virtual ss::future<result<model::transform_offsets_map, cluster::errc>>
    list_committed_offsets_on_shard(ss::shard_id, const model::ntp&) = 0;

    virtual ss::future<cluster::errc> delete_committed_offsets_on_shard(
      ss::shard_id, const model::ntp&, absl::btree_set<model::transform_id>)
      = 0;
};

}; // namespace transform::rpc

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
#include "kafka/data/partition_proxy.h"
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

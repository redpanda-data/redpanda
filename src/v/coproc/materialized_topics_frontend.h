/*
 * Copyright 2021 Vectorized, Inc.
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
#include "coproc/exception.h"

#include <seastar/core/future.hh>
#include <seastar/core/sharded.hh>
#include <seastar/core/shared_future.hh>
#include <seastar/core/shared_ptr.hh>

#include <absl/container/node_hash_map.h>

namespace coproc {

/// \brief Thrown when the cluster subsystem fails to replicate the materialized
/// topic.
class materialized_topic_replication_exception final : public exception {
    using exception::exception;
};

/// This class is used to debounce requests to make materialzed topics. A single
/// coprocessor will (most likely) make multiple requests to create the same
/// materialized topic across shards as long at it has an input topic with more
/// then 1 partition. Fibers serving these partitions can use this class to
/// reconcile and debounce duplicate requests to make a materialized topic.
class materialized_topics_frontend {
public:
    /// \brief Mapping between requested topic_namespace to create and shared
    /// promise that is associated with an outstanding request
    using underlying_t = absl::node_hash_map<
      model::topic_namespace,
      ss::lw_shared_ptr<ss::shared_promise<>>>;

    /// \brief Class constructor
    /// Depends on topics_frontend so 'create_non_replicable_topics' can be used
    explicit materialized_topics_frontend(
      ss::sharded<cluster::topics_frontend>&) noexcept;

    /// \brief Creates desired materialized topics by disseminating the command
    /// across the cluster. If a command is already in progress for a given
    /// topic_namespace, a future will be returned which resolves when the
    /// command completes.
    ss::future<> create_materialized_topics(
      std::vector<cluster::non_replicable_topic>,
      model::timeout_clock::time_point timeout);

private:
    void topic_creation_resolved(std::vector<cluster::topic_result>);

    /// Reference to underlying topic creation mechanism
    ss::sharded<cluster::topics_frontend>& _topics_frontend;

    /// If a key already exists for a requested topic_namespace, a future from
    /// the associate shared_promise will be returned. When the original request
    /// resolves the shared promise will be set.
    underlying_t _topics;
};

/// \brief materializd_topics_frontend is a sharded service that only exists on
/// shard 0. Use this as its 'home_shard' when calling 'invoke_on'
static constexpr ss::shard_id materialized_topics_frontend_shard = 0;

} // namespace coproc

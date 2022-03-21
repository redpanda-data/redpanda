/*
 * Copyright 2021 Redpanda Data, Inc.
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

#include <vector>

namespace cluster {

/// \brief Thrown when the cluster subsystem fails to disseminate the
/// materialized topic.
class non_replicable_topic_creation_exception final : public std::exception {
public:
    explicit non_replicable_topic_creation_exception(ss::sstring) noexcept;

    const char* what() const noexcept override { return _msg.c_str(); }

public:
    ss::sstring _msg;
};

/// This class is used to debounce requests to make materialzed topics. A single
/// coprocessor will (most likely) make multiple requests to create the same
/// materialized topic across shards as long at it has an input topic with more
/// then 1 partition. Fibers serving these partitions can use this class to
/// reconcile and debounce duplicate requests to make a materialized topic.
class non_replicable_topics_frontend {
public:
    /// \brief Mapping between requested topic_namespace to create and shared
    /// promise that is associated with an outstanding request
    using underlying_t
      = absl::node_hash_map<model::topic_namespace, std::vector<ss::promise<>>>;

    /// \brief Class constructor
    /// Depends on topics_frontend so 'create_non_replicable_topics' can be used
    explicit non_replicable_topics_frontend(
      ss::sharded<cluster::topics_frontend>&) noexcept;

    /// \brief Creates desired materialized topics by disseminating the command
    /// across the cluster. If a command is already in progress for a given
    /// topic_namespace, a future will be returned which resolves when the
    /// command completes.
    ss::future<> create_non_replicable_topics(
      std::vector<cluster::non_replicable_topic>,
      model::timeout_clock::duration timeout);

private:
    void topic_creation_resolved(const std::vector<cluster::topic_result>&);
    void topic_creation_exception(
      const std::vector<cluster::non_replicable_topic>&, const std::exception&);

    /// Reference to underlying topic creation mechanism
    ss::sharded<cluster::topics_frontend>& _topics_frontend;

    /// If a key already exists for a requested topic_namespace, a future from
    /// the associate shared_promise will be returned. When the original request
    /// resolves the shared promise will be set.
    underlying_t _topics;
};

/// \brief non_replicable_topics_frontend is a sharded service that only exists
/// on shard 0. Use this as its 'home_shard' when calling 'invoke_on'
static constexpr ss::shard_id non_replicable_topics_frontend_shard = 0;

} // namespace cluster

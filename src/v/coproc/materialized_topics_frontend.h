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

#include <seastar/core/future.hh>
#include <seastar/core/sharded.hh>
#include <seastar/core/shared_future.hh>
#include <seastar/core/shared_ptr.hh>

#include <absl/container/node_hash_map.h>

namespace coproc {

class materialized_topic_replication_exception final : public std::exception {
public:
    explicit materialized_topic_replication_exception(ss::sstring msg) noexcept
      : _msg(std::move(msg)) {}

    const char* what() const noexcept final { return _msg.c_str(); }

private:
    ss::sstring _msg;
};

class materialized_topics_frontend {
public:
    using underlying_t = absl::node_hash_map<
      model::topic_namespace,
      ss::lw_shared_ptr<ss::shared_promise<>>>;

    explicit materialized_topics_frontend(
      ss::sharded<cluster::topics_frontend>&) noexcept;

    ss::future<> create_materialized_topics(
      std::vector<cluster::topic_configuration> topics,
      model::timeout_clock::time_point timeout);

private:
    ss::future<> topic_creation_resolved(std::vector<cluster::topic_result>);

    ss::sharded<cluster::topics_frontend>& _topics_frontend;
    underlying_t _topics;
};

} // namespace coproc

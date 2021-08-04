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

#include "coproc/materialized_topics_frontend.h"

#include "cluster/metadata_cache.h"
#include "cluster/topics_frontend.h"
#include "coproc/logger.h"
#include "vlog.h"
namespace coproc {

materialized_topics_frontend::materialized_topics_frontend(
  ss::sharded<cluster::topics_frontend>& topics_frontend) noexcept
  : _topics_frontend(topics_frontend) {}

ss::future<> materialized_topics_frontend::topic_creation_resolved(
  std::vector<cluster::topic_result> results) {
    for (const auto& result : results) {
        auto found = _topics.find(result.tp_ns);
        vassert(
          found != _topics.end(), "Missing promise with associated event");
        if (result.ec == cluster::errc::success) {
            vlog(
              coproclog.info, "Materialized topic created: {}", result.tp_ns);
            found->second->set_value();
        } else if (result.ec == cluster::errc::topic_already_exists) {
            vlog(
              coproclog.info,
              "Materialzed log has come into existance via "
              "another node: {}",
              result.tp_ns);
            found->second->set_value();
        } else {
            found->second->set_exception(
              materialized_topic_replication_exception(fmt::format(
                "Failed to replicate materialized topic: {}, error: {}",
                result.tp_ns,
                result.ec)));
        }
        _topics.erase(found);
    }
    return ss::now();
}

ss::future<> materialized_topics_frontend::create_materialized_topics(
  std::vector<cluster::topic_configuration> topics,
  model::timeout_clock::time_point timeout) {
    std::vector<ss::future<>> all;
    std::vector<cluster::topic_configuration> todos;
    for (auto& topic : topics) {
        auto [itr, success] = _topics.insert(
          {topic.tp_ns, ss::make_lw_shared<ss::shared_promise<>>()});
        if (!success) {
            all.emplace_back(itr->second->get_shared_future());
        } else {
            todos.emplace_back(std::move(topic));
        }
    }
    if (!todos.empty()) {
        auto f = _topics_frontend.local()
                   .create_topics(std::move(todos), timeout)
                   .then([this](std::vector<cluster::topic_result> result) {
                       return topic_creation_resolved(std::move(result));
                   });
        all.emplace_back(std::move(f));
    }
    return ss::when_all_succeed(all.begin(), all.end());
}
} // namespace coproc

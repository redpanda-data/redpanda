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

#include "cluster/non_replicable_topics_frontend.h"

#include "cluster/logger.h"
#include "cluster/metadata_cache.h"
#include "cluster/topics_frontend.h"
#include "vlog.h"

#include <seastar/core/coroutine.hh>
namespace cluster {

non_replicable_topic_creation_exception::
  non_replicable_topic_creation_exception(ss::sstring msg) noexcept
  : _msg(std::move(msg)) {}

non_replicable_topics_frontend::non_replicable_topics_frontend(
  ss::sharded<cluster::topics_frontend>& topics_frontend) noexcept
  : _topics_frontend(topics_frontend) {}

void non_replicable_topics_frontend::topic_creation_resolved(
  const std::vector<cluster::topic_result>& results) {
    for (const auto& result : results) {
        auto found = _topics.find(result.tp_ns);
        vassert(
          found != _topics.end(),
          "Missing promise with associated event: {}",
          result.tp_ns);
        for (auto& p : found->second) {
            p.set_value(result);
        }
        _topics.erase(found);
    }
}

void non_replicable_topics_frontend::topic_creation_exception(
  const std::vector<cluster::non_replicable_topic>& topics,
  const std::exception& ex) {
    for (const auto& topic : topics) {
        auto found = _topics.find(topic.name);
        if (found != _topics.end()) {
            for (auto& p : found->second) {
                p.set_exception(
                  non_replicable_topic_creation_exception(fmt::format(
                    "create_non_replicable_topics threw: {}, when attempting "
                    "to "
                    "create non_replicable_topic: {}",
                    ex.what(),
                    topic)));
            }
        } else {
            vlog(
              clusterlog.error,
              "Exception thrown for non_replicable_topic creation that doesn't "
              "exist in requests cache: {}",
              topic);
        }
        _topics.erase(found);
    }
}

ss::future<std::vector<cluster::topic_result>>
non_replicable_topics_frontend::create_non_replicable_topics(
  std::vector<cluster::non_replicable_topic> topics,
  model::timeout_clock::time_point timeout) {
    std::vector<ss::future<cluster::topic_result>> all;
    std::vector<cluster::non_replicable_topic> todos;
    for (auto& topic : topics) {
        auto [itr, success] = _topics.try_emplace(
          topic.name, std::vector<ss::promise<cluster::topic_result>>());
        if (!success) {
            itr->second.push_back(ss::promise<cluster::topic_result>());
            all.emplace_back(itr->second.back().get_future());
        } else {
            todos.emplace_back(std::move(topic));
        }
    }
    if (!todos.empty()) {
        co_await _topics_frontend.local()
          .create_non_replicable_topics(todos, timeout)
          .then([this](const std::vector<cluster::topic_result>& result) {
              topic_creation_resolved(result);
          })
          .handle_exception_type(
            [this, todos = std::move(todos)](const std::exception& ex) {
                topic_creation_exception(todos, ex);
            });
    }
    co_return co_await ss::when_all_succeed(all.begin(), all.end());
}
} // namespace cluster

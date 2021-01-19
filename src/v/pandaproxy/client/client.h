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

#include "kafka/client.h"
#include "kafka/types.h"
#include "pandaproxy/client/assignment_plans.h"
#include "pandaproxy/client/broker.h"
#include "pandaproxy/client/brokers.h"
#include "pandaproxy/client/configuration.h"
#include "pandaproxy/client/consumer.h"
#include "pandaproxy/client/fetcher.h"
#include "pandaproxy/client/producer.h"
#include "pandaproxy/client/retry_with_mitigation.h"
#include "utils/retry.h"
#include "utils/unresolved_address.h"

#include <seastar/core/condition-variable.hh>
#include <seastar/core/semaphore.hh>

#include <absl/container/flat_hash_map.h>
#include <absl/container/flat_hash_set.h>

namespace pandaproxy::client {

/// \brief wait or start a function
///
/// Start the function and wait for it to finish, or, if an instance of the
/// function is already running, wait for that one to finish.
class wait_or_start {
public:
    // Prevent accidentally calling the protected func.
    struct tag {};
    using func = ss::noncopyable_function<ss::future<>(tag)>;

    explicit wait_or_start(func func)
      : _func{std::move(func)} {}

    ss::future<> operator()() {
        if (_lock.try_wait()) {
            return _func(tag{}).finally(
              [this]() { _lock.signal(_lock.waiters() + 1); });
        }
        return _lock.wait();
    }

private:
    func _func;
    ss::semaphore _lock{1};
};

class client {
public:
    explicit client(std::vector<unresolved_address> broker_addrs);

    /// \brief Connect to all brokers.
    ss::future<> connect();
    /// \brief Disconnect from all brokers.
    ss::future<> stop();

    /// \brief Invoke func, on failure, mitigate error and retry.
    template<typename Func>
    std::invoke_result_t<Func> gated_retry_with_mitigation(Func func) {
        return ss::with_gate(_gate, [this, func{std::move(func)}]() {
            return retry_with_mitigation(
              shard_local_cfg().retries(),
              shard_local_cfg().retry_base_backoff(),
              [this, func{std::move(func)}]() {
                  _gate.check();
                  return func();
              },
              [this](std::exception_ptr ex) { return mitigate_error(ex); });
        });
    }

    /// \brief Dispatch a request to any broker.
    template<typename Func>
    CONCEPT(requires requires {
        typename std::invoke_result_t<Func>::api_type::response_type;
    })
    ss::future<typename std::invoke_result_t<
      Func>::api_type::response_type> dispatch(Func func) {
        return gated_retry_with_mitigation([this, func{std::move(func)}]() {
            return _brokers.any().then(
              [func{std::move(func)}](shared_broker_t broker) mutable {
                  return broker->dispatch(func());
              });
        });
    }

    ss::future<kafka::produce_response::partition> produce_record_batch(
      model::topic_partition tp, model::record_batch&& batch);

    ss::future<kafka::fetch_response> fetch_partition(
      model::topic_partition tp,
      model::offset offset,
      int32_t max_bytes,
      std::chrono::milliseconds timeout);

    ss::future<kafka::member_id> create_consumer(const kafka::group_id& g_id);

    ss::future<>
    remove_consumer(const kafka::group_id& g_id, const kafka::member_id& m_id);

    ss::future<> subscribe_consumer(
      const kafka::group_id& group_id,
      const kafka::member_id& member_id,
      std::vector<model::topic> topics);

    ss::future<std::vector<model::topic>>
    consumer_topics(const kafka::group_id& g_id, const kafka::member_id& m_id);

    ss::future<assignment> consumer_assignment(
      const kafka::group_id& g_id, const kafka::member_id& m_id);

    ss::future<kafka::offset_fetch_response> consumer_offset_fetch(
      const kafka::group_id& g_id,
      const kafka::member_id& m_id,
      std::vector<kafka::offset_fetch_request_topic> topics);

    ss::future<kafka::offset_commit_response> consumer_offset_commit(
      const kafka::group_id& g_id,
      const kafka::member_id& m_id,
      std::vector<kafka::offset_commit_request_topic> topics);

private:
    /// \brief Connect and update metdata.
    ss::future<> do_connect(unresolved_address addr);

    /// \brief Update metadata
    ///
    /// If an existing update is in progress, the future returned will be
    /// satisfied by the outstanding request.
    ///
    /// Uses round-robin load-balancing strategy.
    ss::future<> update_metadata(wait_or_start::tag);

    /// \brief Handle errors by performing an action that may fix the cause of
    /// the error
    ss::future<> mitigate_error(std::exception_ptr ex);

    ss::future<shared_consumer_t>
    get_consumer(const kafka::group_id& g_id, const kafka::member_id& m_id);

    /// \brief Seeds are used when no brokers are connected.
    std::vector<unresolved_address> _seeds;
    /// \brief Broker lookup from topic_partition.
    brokers _brokers;
    /// \brief Update metadata, or wait for an existing one.
    wait_or_start _wait_or_start_update_metadata;
    /// \brief Batching producer.
    producer _producer;
    /// \brief Consumers
    absl::flat_hash_set<
      shared_consumer_t,
      detail::consumer_hash,
      detail::consumer_eq>
      _consumers;
    /// \brief Wait for retries.
    ss::gate _gate;
};

} // namespace pandaproxy::client

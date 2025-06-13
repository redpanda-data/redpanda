/*
 * Copyright 2020 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "kafka/client/broker.h"
#include "kafka/client/brokers.h"
#include "kafka/client/configuration.h"
#include "kafka/client/exceptions.h"
#include "kafka/protocol/find_coordinator.h"
#include "kafka/protocol/offset_commit.h"
#include "utils/retry.h"

namespace kafka::client {

/// \brief Perform an action with retry on failure.
///
/// If the action returns an error, it is retried with a backoff.
/// There is an attempt to mitigate the error after the backoff and prior
/// to the retry.
///
/// \param func is copied for each iteration
/// \param errFunc is copied, but held by reference for each iteration
template<
  typename Func,
  typename ErrFunc,
  typename Futurator = ss::futurize<std::invoke_result_t<Func>>>
requires std::regular_invocable<Func>
auto retry_with_mitigation(
  int32_t retries,
  std::chrono::milliseconds retry_base_backoff,
  Func func,
  ErrFunc errFunc,
  std::optional<std::reference_wrapper<ss::abort_source>> as = std::nullopt) {
    using namespace std::chrono_literals;
    return ss::do_with(
      std::move(func),
      std::move(errFunc),
      std::exception_ptr(),
      [retries, retry_base_backoff, as](
        const Func& func, ErrFunc& errFunc, std::exception_ptr& eptr) {
          return retry_with_backoff(
            retries,
            [&func, &errFunc, &eptr]() {
                auto fut = ss::now();
                if (eptr) {
                    fut = errFunc(eptr).handle_exception(
                      [](const std::exception_ptr&) {
                          // ignore failed mitigation
                      });
                }

                static_assert(
                  std::is_invocable_v<const Func&>,
                  "Func must have a const operator(), i.e., it must not be a "
                  "mutable lambda or functor with non-const call operator. "
                  "This is to prevent problematic usage patterns where state "
                  "is moved-from on retries. For example, the following is "
                  "problematic:\n"
                  "\tauto bad_func = [movable_state]() mutable {\n"
                  "\t    auto _ = consume(std::move(movable_state));\n"
                  "\t};");

                return fut.then([&func] { return func(); })
                  .handle_exception([&eptr](std::exception_ptr ex) mutable {
                      eptr = ex;
                      return Futurator::make_exception_future(eptr);
                  });
            },
            retry_base_backoff,
            as);
      });
}

/// \brief Invoke func, on failure. Invoke errFunc on error and retry.
template<typename Func, typename ErrFunc>
std::invoke_result_t<Func> gated_retry_with_mitigation_impl(
  ss::gate& retry_gate,
  int32_t retries,
  std::chrono::milliseconds retry_base_backoff,
  Func func,
  ErrFunc errFunc,
  std::optional<std::reference_wrapper<ss::abort_source>> as = std::nullopt) {
    return ss::try_with_gate(
      retry_gate,
      [retries,
       retry_base_backoff,
       &retry_gate,
       func{std::move(func)},
       errFunc{std::move(errFunc)},
       as]() mutable {
          return retry_with_mitigation(
            retries,
            retry_base_backoff,
            [&retry_gate, func{std::move(func)}]() {
                retry_gate.check();
                return func();
            },
            errFunc,
            as);
      });
}

/// \brief Execute a find coordinator request with retry and error mitigation.
template<typename ErrFunc>
ss::future<shared_broker_t> find_coordinator_with_retry_and_mitigation(
  ss::gate& retry_gate,
  const configuration& client_config,
  brokers& cluster_brokers,
  const group_id& group_id,
  member_id name,
  ErrFunc errFunc) {
    return gated_retry_with_mitigation_impl(
             retry_gate,
             client_config.retries(),
             client_config.retry_base_backoff(),
             [group_id, name, &cluster_brokers]() {
                 return cluster_brokers.any()
                   .then([group_id](shared_broker_t broker) {
                       return broker->dispatch(
                         find_coordinator_request(group_id));
                   })
                   .then([group_id, name](find_coordinator_response res) {
                       if (res.data.error_code != error_code::none) {
                           return ss::make_exception_future<
                             find_coordinator_response>(consumer_error(
                             group_id, name, res.data.error_code));
                       };
                       return ss::make_ready_future<find_coordinator_response>(
                         std::move(res));
                   });
             },
             errFunc)
      .then([&client_config](find_coordinator_response res) {
          return make_broker(
            res.data.node_id,
            net::unresolved_address(res.data.host, res.data.port),
            client_config);
      });
}

inline chunked_vector<kafka::offset_commit_request_topic>
make_copy(const chunked_vector<kafka::offset_commit_request_topic>& topics) {
    static_assert(
      reflection::arity<kafka::offset_commit_request_topic>() == 3,
      "kafka::offset_commit_request_topic must have 3 fields, otherwise this "
      "function must be updated");

    chunked_vector<kafka::offset_commit_request_topic> res;
    res.reserve(topics.size());
    for (const auto& topic : topics) {
        res.push_back({
          .name = topic.name,
          .partitions = topic.partitions.copy(),
          .unknown_tags = topic.unknown_tags,
        });
    }
    return res;
}

} // namespace kafka::client

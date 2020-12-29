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
#include "cluster/controller_service.h"
#include "cluster/errc.h"
#include "cluster/types.h"
#include "config/configuration.h"
#include "config/tls_config.h"
#include "outcome_future_utils.h"
#include "rpc/connection_cache.h"

#include <seastar/core/sharded.hh>

#include <utility>

namespace cluster {

class metadata_cache;
/// This method calculates the machine nodes that were updated/added
/// and removed
patch<broker_ptr> calculate_changed_brokers(
  std::vector<broker_ptr> new_list, std::vector<broker_ptr> old_list);

/// Creates the same topic_result for all requests
// clang-format off
template<typename T>
CONCEPT(requires requires(const T& req) {
    { req.tp_ns } -> std::convertible_to<const model::topic_namespace&>;
})
// clang-format on
std::vector<topic_result> create_topic_results(
  const std::vector<T>& requests, errc error_code) {
    std::vector<topic_result> results;
    results.reserve(requests.size());
    std::transform(
      std::cbegin(requests),
      std::cend(requests),
      std::back_inserter(results),
      [error_code](const T& r) { return topic_result(r.tp_ns, error_code); });
    return results;
}

std::vector<topic_result> create_topic_results(
  const std::vector<model::topic_namespace>& topics, errc error_code);

ss::future<> update_broker_client(
  model::node_id,
  ss::sharded<rpc::connection_cache>&,
  model::node_id node,
  unresolved_address addr,
  config::tls_config);

ss::future<> remove_broker_client(
  model::node_id, ss::sharded<rpc::connection_cache>&, model::node_id);

// clang-format off
template<typename Proto, typename Func>
CONCEPT(requires requires(Func&& f, Proto c) {
        f(c);
})
// clang-format on
auto with_client(
  model::node_id self,
  ss::sharded<rpc::connection_cache>& cache,
  model::node_id id,
  unresolved_address addr,
  config::tls_config tls_config,
  Func&& f) {
    return update_broker_client(
             self, cache, id, std::move(addr), std::move(tls_config))
      .then([id, self, &cache, f = std::forward<Func>(f)]() mutable {
          return cache.local().with_node_client<Proto, Func>(
            self, ss::this_shard_id(), id, std::forward<Func>(f));
      });
}

/// Creates current broker instance using its configuration.
model::broker make_self_broker(const config::configuration& cfg);

/// \brief Log reload credential event
/// The function is supposed to be invoked from the callback passed to
/// 'build_reloadable_*_credentials' methods.
///
/// \param log is a ss::logger instance that should be used
/// \param system_name is a name of the subsystem that uses credentials
/// \param updated is a set of updated credential names
/// \param eptr is an exception ptr in case of error
void log_certificate_reload_event(
  ss::logger& log,
  const char* system_name,
  const std::unordered_set<ss::sstring>& updated,
  const std::exception_ptr& eptr);

} // namespace cluster

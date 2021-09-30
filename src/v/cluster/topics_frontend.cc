// Copyright 2020 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cluster/topics_frontend.h"

#include "cluster/cluster_utils.h"
#include "cluster/commands.h"
#include "cluster/controller_stm.h"
#include "cluster/errc.h"
#include "cluster/logger.h"
#include "cluster/partition_leaders_table.h"
#include "cluster/scheduling/partition_allocator.h"
#include "cluster/types.h"
#include "model/errc.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "model/namespace.h"
#include "model/validation.h"
#include "raft/errc.h"
#include "raft/types.h"
#include "random/generators.h"
#include "rpc/errc.h"
#include "rpc/types.h"
#include "ssx/future-util.h"

#include <seastar/core/coroutine.hh>

#include <algorithm>
#include <iterator>
#include <regex>

namespace cluster {

topics_frontend::topics_frontend(
  model::node_id self,
  ss::sharded<controller_stm>& s,
  ss::sharded<rpc::connection_cache>& con,
  ss::sharded<partition_allocator>& pal,
  ss::sharded<partition_leaders_table>& l,
  ss::sharded<topic_table>& topics,
  ss::sharded<ss::abort_source>& as)
  : _self(self)
  , _stm(s)
  , _allocator(pal)
  , _connections(con)
  , _leaders(l)
  , _topics(topics)
  , _as(as) {}

static bool
needs_linearizable_barrier(const std::vector<topic_result>& results) {
    return std::any_of(
      results.cbegin(), results.cend(), [](const topic_result& r) {
          return r.ec == errc::success;
      });
}

ss::future<std::vector<topic_result>> topics_frontend::create_topics(
  std::vector<topic_configuration> topics,
  model::timeout_clock::time_point timeout) {
    vlog(clusterlog.trace, "Create topics {}", topics);
    // make sure that STM is up to date (i.e. we have the most recent state
    // available) before allocating topics
    return _stm
      .invoke_on(
        controller_stm_shard,
        [timeout](controller_stm& stm) {
            return stm.quorum_write_empty_batch(timeout);
        })
      .then([this, topics = std::move(topics), timeout](
              result<raft::replicate_result> result) mutable {
          if (!result) {
              return ss::make_ready_future<std::vector<topic_result>>(
                create_topic_results(topics, errc::not_leader_controller));
          }
          std::vector<ss::future<topic_result>> futures;
          futures.reserve(topics.size());

          std::transform(
            std::begin(topics),
            std::end(topics),
            std::back_inserter(futures),
            [this, timeout](topic_configuration& t_cfg) {
                return do_create_topic(std::move(t_cfg), timeout);
            });

          return ss::when_all_succeed(futures.begin(), futures.end());
      })
      .then([this, timeout](std::vector<topic_result> results) {
          if (needs_linearizable_barrier(results)) {
              return stm_linearizable_barrier(timeout).then(
                [results = std::move(results)](result<model::offset>) mutable {
                    return results;
                });
          }
          return ss::make_ready_future<std::vector<topic_result>>(
            std::move(results));
      });
}

cluster::errc map_errc(std::error_code ec) {
    if (ec == errc::success) {
        return errc::success;
    }
    // error comming from raft
    if (ec.category() == raft::error_category()) {
        switch (static_cast<raft::errc>(ec.value())) {
        case raft::errc::timeout:
            return errc::timeout;
        case raft::errc::not_leader:
            return errc::not_leader_controller;
        default:
            return errc::replication_error;
        }
    }

    // error comming from raft
    if (ec.category() == rpc::error_category()) {
        switch (static_cast<rpc::errc>(ec.value())) {
        case rpc::errc::client_request_timeout:
            return errc::timeout;
        default:
            return errc::replication_error;
        }
    }
    // cluster errors, just forward
    if (ec.category() == cluster::error_category()) {
        return static_cast<errc>(ec.value());
    }

    return errc::replication_error;
}

ss::future<std::vector<topic_result>> topics_frontend::update_topic_properties(
  std::vector<topic_properties_update> updates,
  model::timeout_clock::time_point timeout) {
    auto cluster_leader = _leaders.local().get_leader(model::controller_ntp);

    // no leader available
    if (!cluster_leader) {
        co_return create_topic_results(updates, errc::no_leader_controller);
    }

    // current node is a leader, just replicate
    if (cluster_leader == _self) {
        // replicate empty batch to make sure leader local state is up to date.
        auto result = co_await _stm.invoke_on(
          controller_stm_shard, [timeout](controller_stm& stm) {
              return stm.quorum_write_empty_batch(timeout);
          });
        if (!result) {
            co_return create_topic_results(updates, map_errc(result.error()));
        }

        auto results = co_await ssx::parallel_transform(
          std::move(updates), [this, timeout](topic_properties_update update) {
              return do_update_topic_properties(std::move(update), timeout);
          });

        // we are not really interested in the result comming from the
        // linearizable barrier, results comming from the previous steps will be
        // propagated to clients, this is just an optimization, this doesn't
        // affect correctness of the protocol
        if (needs_linearizable_barrier(results)) {
            co_await stm_linearizable_barrier(timeout).discard_result();
        }

        co_return results;
    }

    co_return co_await _connections.local()
      .with_node_client<controller_client_protocol>(
        _self,
        ss::this_shard_id(),
        *cluster_leader,
        timeout,
        [updates, timeout](controller_client_protocol client) mutable {
            return client
              .update_topic_properties(
                update_topic_properties_request{.updates = std::move(updates)},
                rpc::client_opts(timeout))
              .then(&rpc::get_ctx_data<update_topic_properties_reply>);
        })
      .then([updates](result<update_topic_properties_reply> r) {
          if (r.has_error()) {
              return create_topic_results(updates, map_errc(r.error()));
          }
          return std::move(r.value().results);
      });
}

ss::future<topic_result> topics_frontend::do_update_topic_properties(
  topic_properties_update update, model::timeout_clock::time_point timeout) {
    update_topic_properties_cmd cmd(update.tp_ns, update.properties);
    try {
        auto ec = co_await replicate_and_wait(
          _stm, _as, std::move(cmd), timeout);
        co_return topic_result(std::move(update.tp_ns), map_errc(ec));
    } catch (...) {
        vlog(
          clusterlog.warn,
          "unable to update {} configuration properties - {}",
          update.tp_ns,
          std::current_exception());

        co_return topic_result(
          std::move(update.tp_ns), errc::replication_error);
    }
}

topic_result
make_error_result(const model::topic_namespace& tp_ns, std::error_code ec) {
    if (ec.category() == cluster::error_category()) {
        return topic_result(tp_ns, cluster::errc(ec.value()));
    }

    return topic_result(tp_ns, errc::topic_operation_error);
}

allocation_request make_allocation_request(const topic_configuration& cfg) {
    allocation_request req;
    req.partitions.reserve(cfg.partition_count);
    for (auto p = 0; p < cfg.partition_count; ++p) {
        req.partitions.emplace_back(
          model::partition_id(p), cfg.replication_factor);
    }
    return req;
}

ss::future<topic_result> topics_frontend::do_create_topic(
  topic_configuration t_cfg, model::timeout_clock::time_point timeout) {
    if (!validate_topic_name(t_cfg.tp_ns)) {
        return ss::make_ready_future<topic_result>(
          topic_result(t_cfg.tp_ns, errc::invalid_topic_name));
    }
    return _allocator
      .invoke_on(
        partition_allocator::shard,
        [t_cfg](partition_allocator& al) {
            return al.allocate(make_allocation_request(t_cfg));
        })
      .then([this, t_cfg = std::move(t_cfg), timeout](
              result<allocation_units> units) mutable {
          // no assignments, error
          if (!units) {
              return ss::make_ready_future<topic_result>(
                make_error_result(t_cfg.tp_ns, units.error()));
          }

          return replicate_create_topic(
            std::move(t_cfg), std::move(units.value()), timeout);
      });
}

ss::future<topic_result> topics_frontend::replicate_create_topic(
  topic_configuration cfg,
  allocation_units units,
  model::timeout_clock::time_point timeout) {
    auto tp_ns = cfg.tp_ns;
    create_topic_cmd cmd(
      tp_ns,
      topic_configuration_assignment(std::move(cfg), units.get_assignments()));

    std::vector<ntp_leader> leaders;
    leaders.reserve(cmd.value.assignments.size());
    for (auto& p_as : cmd.value.assignments) {
        std::shuffle(
          p_as.replicas.begin(),
          p_as.replicas.end(),
          random_generators::internal::gen);
        // guesstimate leaders
        leaders.emplace_back(
          model::ntp(tp_ns.ns, tp_ns.tp, p_as.id),
          p_as.replicas.begin()->node_id);
    }

    return replicate_and_wait(_stm, _as, std::move(cmd), timeout)
      .then_wrapped(
        [this,
         tp_ns = std::move(tp_ns),
         units = std::move(units),
         leaders = std::move(leaders)](ss::future<std::error_code> f) mutable {
            try {
                auto error_code = f.get0();
                auto ret_f = ss::now();
                if (!error_code) {
                    ret_f = update_leaders_with_estimates(std::move(leaders));
                }

                return ret_f.then([tp_ns = std::move(tp_ns),
                                   error_code]() mutable {
                    return topic_result(std::move(tp_ns), map_errc(error_code));
                });

            } catch (...) {
                vlog(
                  clusterlog.warn,
                  "Unable to create topic - {}",
                  std::current_exception());
                return ss::make_ready_future<topic_result>(
                  topic_result(std::move(tp_ns), errc::replication_error));
            }
        });
}

ss::future<> topics_frontend::update_leaders_with_estimates(
  std::vector<ntp_leader> leaders) {
    return ss::do_with(
      std::move(leaders), [this](std::vector<ntp_leader>& leaders) {
          return ss::parallel_for_each(leaders, [this](ntp_leader& leader) {
              return _leaders.invoke_on_all(
                [leader](partition_leaders_table& l) {
                    return l.update_partition_leader(
                      leader.first, model::term_id(1), leader.second);
                });
          });
      });
}

ss::future<std::vector<topic_result>> topics_frontend::delete_topics(
  std::vector<model::topic_namespace> topics,
  model::timeout_clock::time_point timeout) {
    std::vector<ss::future<topic_result>> futures;
    futures.reserve(topics.size());

    std::transform(
      std::begin(topics),
      std::end(topics),
      std::back_inserter(futures),
      [this, timeout](model::topic_namespace& tp_ns) {
          return do_delete_topic(std::move(tp_ns), timeout);
      });

    return ss::when_all_succeed(futures.begin(), futures.end())
      .then([this, timeout](std::vector<topic_result> results) {
          if (needs_linearizable_barrier(results)) {
              return stm_linearizable_barrier(timeout).then(
                [results = std::move(results)](result<model::offset>) mutable {
                    return results;
                });
          }
          return ss::make_ready_future<std::vector<topic_result>>(
            std::move(results));
      });
}

ss::future<topic_result> topics_frontend::do_delete_topic(
  model::topic_namespace tp_ns, model::timeout_clock::time_point timeout) {
    delete_topic_cmd cmd(tp_ns, tp_ns);

    return replicate_and_wait(_stm, _as, std::move(cmd), timeout)
      .then_wrapped(
        [tp_ns = std::move(tp_ns)](ss::future<std::error_code> f) mutable {
            try {
                auto ec = f.get0();
                if (ec != errc::success) {
                    return topic_result(std::move(tp_ns), map_errc(ec));
                }
                return topic_result(std::move(tp_ns), errc::success);
            } catch (...) {
                vlog(
                  clusterlog.warn,
                  "Unable to delete topic - {}",
                  std::current_exception());
                return topic_result(std::move(tp_ns), errc::replication_error);
            }
        });
}

ss::future<std::vector<topic_result>> topics_frontend::autocreate_topics(
  std::vector<topic_configuration> topics,
  model::timeout_clock::duration timeout) {
    vlog(clusterlog.trace, "Auto create topics {}", topics);

    auto leader = _leaders.local().get_leader(model::controller_ntp);

    // no leader available
    if (!leader) {
        return ss::make_ready_future<std::vector<topic_result>>(
          create_topic_results(topics, errc::no_leader_controller));
    }
    // current node is a leader controller
    if (leader == _self) {
        return create_topics(
          std::move(topics), model::timeout_clock::now() + timeout);
    }
    // dispatch to leader
    return dispatch_create_to_leader(
      leader.value(), std::move(topics), timeout);
}

ss::future<std::vector<topic_result>>
topics_frontend::dispatch_create_to_leader(
  model::node_id leader,
  std::vector<topic_configuration> topics,
  model::timeout_clock::duration timeout) {
    vlog(clusterlog.trace, "Dispatching create topics to {}", leader);
    return _connections.local()
      .with_node_client<cluster::controller_client_protocol>(
        _self,
        ss::this_shard_id(),
        leader,
        timeout,
        [topics, timeout](controller_client_protocol cp) mutable {
            return cp.create_topics(
              create_topics_request{std::move(topics), timeout},
              rpc::client_opts(model::timeout_clock::now() + timeout));
        })
      .then(&rpc::get_ctx_data<create_topics_reply>)
      .then(
        [topics = std::move(topics)](result<create_topics_reply> r) mutable {
            if (r.has_error()) {
                return create_topic_results(topics, map_errc(r.error()));
            }
            return std::move(r.value().results);
        });
}

bool topics_frontend::validate_topic_name(const model::topic_namespace& topic) {
    if (topic.ns == model::kafka_namespace) {
        const auto errc = model::validate_kafka_topic_name(topic.tp);
        if (static_cast<model::errc>(errc.value()) != model::errc::success) {
            vlog(clusterlog.info, "{} {}", errc.message(), topic.tp());
            return false;
        }
    }
    return true;
}

ss::future<std::error_code> topics_frontend::move_partition_replicas(
  model::ntp ntp,
  std::vector<model::broker_shard> new_replica_set,
  model::timeout_clock::time_point tout) {
    move_partition_replicas_cmd cmd(std::move(ntp), std::move(new_replica_set));

    return replicate_and_wait(_stm, _as, std::move(cmd), tout);
}

ss::future<std::error_code> topics_frontend::finish_moving_partition_replicas(
  model::ntp ntp,
  std::vector<model::broker_shard> new_replica_set,
  model::timeout_clock::time_point tout) {
    auto leader = _leaders.local().get_leader(model::controller_ntp);

    // no leader available
    if (!leader) {
        return ss::make_ready_future<std::error_code>(
          errc::no_leader_controller);
    }
    // current node is a leader, just replicate
    if (leader == _self) {
        finish_moving_partition_replicas_cmd cmd(
          std::move(ntp), std::move(new_replica_set));

        return replicate_and_wait(_stm, _as, std::move(cmd), tout);
    }

    return _connections.local()
      .with_node_client<controller_client_protocol>(
        _self,
        ss::this_shard_id(),
        *leader,
        tout,
        [ntp = std::move(ntp), replicas = std::move(new_replica_set), tout](
          controller_client_protocol client) mutable {
            return client
              .finish_partition_update(
                finish_partition_update_request{
                  .ntp = std::move(ntp),
                  .new_replica_set = std::move(replicas)},
                rpc::client_opts(tout))
              .then(&rpc::get_ctx_data<finish_partition_update_reply>);
        })
      .then([](result<finish_partition_update_reply> r) {
          return r.has_error() ? r.error() : r.value().result;
      });
}

ss::future<result<model::offset>> topics_frontend::stm_linearizable_barrier(
  model::timeout_clock::time_point timeout) {
    return _stm.invoke_on(controller_stm_shard, [timeout](controller_stm& stm) {
        return stm.instert_linerizable_barrier(timeout);
    });
}

ss::future<std::vector<topic_result>> topics_frontend::create_partitions(
  std::vector<create_partititions_configuration> partitions,
  model::timeout_clock::time_point timeout) {
    auto r = co_await stm_linearizable_barrier(timeout);
    if (!r) {
        std::vector<topic_result> results;
        results.reserve(partitions.size());
        std::transform(
          partitions.begin(),
          partitions.end(),
          std::back_inserter(results),
          [err = r.error()](const create_partititions_configuration& cfg) {
              return make_error_result(cfg.tp_ns, err);
          });
        co_return results;
    }

    auto result = co_await ssx::parallel_transform(
      partitions.begin(),
      partitions.end(),
      [this, timeout](create_partititions_configuration cfg) {
          return do_create_partition(std::move(cfg), timeout);
      });

    co_return result;
}

allocation_request make_allocation_request(
  int16_t replication_factor,
  const int32_t current_partitions_count,
  const create_partititions_configuration& cfg) {
    const auto new_partitions_cnt = cfg.new_total_partition_count
                                    - current_partitions_count;
    allocation_request req;
    req.partitions.reserve(new_partitions_cnt);
    for (auto p = 0; p < new_partitions_cnt; ++p) {
        req.partitions.emplace_back(model::partition_id(p), replication_factor);
    }
    return req;
}

ss::future<topic_result> topics_frontend::do_create_partition(
  create_partititions_configuration p_cfg,
  model::timeout_clock::time_point timeout) {
    auto tp_cfg = _topics.local().get_topic_cfg(p_cfg.tp_ns);
    if (!tp_cfg) {
        co_return make_error_result(p_cfg.tp_ns, errc::topic_not_exists);
    }
    // we only support increasing number of partitions
    if (p_cfg.new_total_partition_count <= tp_cfg->partition_count) {
        co_return make_error_result(
          p_cfg.tp_ns, errc::topic_invalid_partitions);
    }

    auto units = co_await _allocator.invoke_on(
      partition_allocator::shard,
      [p_cfg,
       current = tp_cfg->partition_count,
       rf = tp_cfg->replication_factor](partition_allocator& al) {
          return al.allocate(make_allocation_request(rf, current, p_cfg));
      });

    // no assignments, error
    if (!units) {
        co_return make_error_result(p_cfg.tp_ns, units.error());
    }

    auto tp_ns = p_cfg.tp_ns;
    create_partititions_configuration_assignment payload(
      std::move(p_cfg), units.value().get_assignments());
    create_partition_cmd cmd = create_partition_cmd(tp_ns, std::move(payload));

    try {
        auto ec = co_await replicate_and_wait(
          _stm, _as, std::move(cmd), timeout);
        co_return topic_result(tp_ns, map_errc(ec));
    } catch (...) {
        vlog(
          clusterlog.warn,
          "Unable to create topic {} partitions - {}",
          tp_ns,
          std::current_exception());
        co_return topic_result(std::move(tp_ns), errc::replication_error);
    }
}

} // namespace cluster

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
#include "cluster/partition_allocator.h"
#include "cluster/types.h"
#include "model/errc.h"
#include "model/metadata.h"
#include "model/namespace.h"
#include "model/validation.h"
#include "raft/errc.h"
#include "raft/types.h"
#include "random/generators.h"
#include "rpc/errc.h"
#include "rpc/types.h"

#include <regex>

namespace cluster {

topics_frontend::topics_frontend(
  model::node_id self,
  ss::sharded<controller_stm>& s,
  ss::sharded<rpc::connection_cache>& con,
  ss::sharded<partition_allocator>& pal,
  ss::sharded<partition_leaders_table>& l,
  ss::sharded<ss::abort_source>& as)
  : _self(self)
  , _stm(s)
  , _allocator(pal)
  , _connections(con)
  , _leaders(l)
  , _as(as) {}

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

template<typename Cmd>
ss::future<std::error_code> topics_frontend::replicate_and_wait(
  Cmd&& cmd, model::timeout_clock::time_point timeout) {
    return _stm.invoke_on(
      controller_stm_shard,
      [cmd = std::forward<Cmd>(cmd), &as = _as, timeout](
        controller_stm& stm) mutable {
          return serialize_cmd(std::forward<Cmd>(cmd))
            .then([&stm, timeout, &as](model::record_batch b) {
                return stm.replicate_and_wait(
                  std::move(b), timeout, as.local());
            });
      });
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
        [t_cfg](partition_allocator& al) { return al.allocate(t_cfg); })
      .then(
        [this, t_cfg = std::move(t_cfg), timeout](
          std::optional<partition_allocator::allocation_units> units) mutable {
            // no assignments, error
            if (!units) {
                return ss::make_ready_future<topic_result>(
                  topic_result(t_cfg.tp_ns, errc::topic_invalid_partitions));
            }

            return replicate_create_topic(
              std::move(t_cfg), std::move(*units), timeout);
        });
}

ss::future<topic_result> topics_frontend::replicate_create_topic(
  topic_configuration cfg,
  partition_allocator::allocation_units units,
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

    return replicate_and_wait(std::move(cmd), timeout)
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

    return ss::when_all_succeed(futures.begin(), futures.end());
}

ss::future<topic_result> topics_frontend::do_delete_topic(
  model::topic_namespace tp_ns, model::timeout_clock::time_point timeout) {
    delete_topic_cmd cmd(tp_ns, tp_ns);

    return replicate_and_wait(std::move(cmd), timeout)
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

    return replicate_and_wait(std::move(cmd), tout);
}

} // namespace cluster

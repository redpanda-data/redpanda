// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cluster/topics_frontend.h"

#include "cloud_storage/remote.h"
#include "cloud_storage_clients/configuration.h"
#include "cluster/cluster_utils.h"
#include "cluster/commands.h"
#include "cluster/controller_service.h"
#include "cluster/controller_stm.h"
#include "cluster/errc.h"
#include "cluster/health_monitor_frontend.h"
#include "cluster/health_monitor_types.h"
#include "cluster/logger.h"
#include "cluster/members_table.h"
#include "cluster/partition_leaders_table.h"
#include "cluster/partition_manager.h"
#include "cluster/scheduling/constraints.h"
#include "cluster/scheduling/partition_allocator.h"
#include "cluster/shard_table.h"
#include "cluster/types.h"
#include "config/configuration.h"
#include "model/errc.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "model/namespace.h"
#include "model/validation.h"
#include "raft/consensus_client_protocol.h"
#include "raft/errc.h"
#include "raft/types.h"
#include "random/generators.h"
#include "rpc/errc.h"
#include "rpc/types.h"
#include "ssx/future-util.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/future.hh>
#include <seastar/core/loop.hh>
#include <seastar/core/sharded.hh>

#include <algorithm>
#include <iterator>
#include <memory>
#include <regex>
#include <sstream>
#include <system_error>

namespace cluster {

topics_frontend::topics_frontend(
  model::node_id self,
  ss::sharded<controller_stm>& s,
  ss::sharded<rpc::connection_cache>& con,
  ss::sharded<partition_allocator>& pal,
  ss::sharded<partition_leaders_table>& l,
  ss::sharded<topic_table>& topics,
  ss::sharded<health_monitor_frontend>& hm_frontend,
  ss::sharded<ss::abort_source>& as,
  ss::sharded<cloud_storage::remote>& cloud_storage_api,
  ss::sharded<features::feature_table>& features,
  ss::sharded<cluster::members_table>& members_table,
  ss::sharded<partition_manager>& pm,
  ss::sharded<shard_table>& shard_table,
  config::binding<unsigned> hard_max_disk_usage_ratio)
  : _self(self)
  , _stm(s)
  , _allocator(pal)
  , _connections(con)
  , _leaders(l)
  , _topics(topics)
  , _hm_frontend(hm_frontend)
  , _as(as)
  , _cloud_storage_api(cloud_storage_api)
  , _features(features)
  , _members_table(members_table)
  , _pm(pm)
  , _shard_table(shard_table)
  , _hard_max_disk_usage_ratio(hard_max_disk_usage_ratio) {}

static bool
needs_linearizable_barrier(const std::vector<topic_result>& results) {
    return std::any_of(
      results.cbegin(), results.cend(), [](const topic_result& r) {
          return r.ec == errc::success;
      });
}

ss::future<std::vector<topic_result>> topics_frontend::create_topics(
  std::vector<custom_assignable_topic_configuration> topics,
  model::timeout_clock::time_point timeout) {
    vlog(clusterlog.info, "Create topics {}", topics);
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
            [this, timeout](custom_assignable_topic_configuration& t_cfg) {
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

    if (!_features.local().is_active(features::feature::cloud_retention)) {
        // The ADL encoding for cluster::incremental_topic_updates has evolved
        // in v22.3. ADL is not forwards compatible, so we need to safe-guard
        // against sending a message from the future to older nodes.

        vlog(
          clusterlog.info,
          "Refusing to update topics as not all cluster nodes are running "
          "v22.3");
        co_return create_topic_results(updates, errc::feature_disabled);
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

        // we are not really interested in the result coming from the
        // linearizable barrier, results coming from the previous steps will be
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

ss::future<std::error_code> topics_frontend::do_update_replication_factor(
  topic_properties_update& update, model::timeout_clock::time_point timeout) {
    switch (update.custom_properties.replication_factor.op) {
    case incremental_update_operation::set: {
        if (!_features.local().is_active(
              features::feature::replication_factor_change)) {
            co_return cluster::errc::feature_disabled;
        }

        auto value = update.custom_properties.replication_factor.value;
        if (
          !value.has_value()
          || value.value() == cluster::replication_factor(0)) {
            co_return cluster::errc::topic_invalid_replication_factor;
        }

        co_return co_await change_replication_factor(
          update.tp_ns,
          update.custom_properties.replication_factor.value.value(),
          timeout);
    }
    case incremental_update_operation::none:
        co_return cluster::errc::success;
    default:
        co_return cluster::errc::invalid_configuration_update;
    }
}

ss::future<topic_result> topics_frontend::do_update_topic_properties(
  topic_properties_update update, model::timeout_clock::time_point timeout) {
    update_topic_properties_cmd cmd(update.tp_ns, update.properties);
    try {
        auto update_rf_res = co_await do_update_replication_factor(
          update, timeout);
        if (update_rf_res != std::error_code(cluster::errc::success)) {
            co_return topic_result(
              update.tp_ns, cluster::errc(update_rf_res.value()));
        }

        auto ec = co_await replicate_and_wait(
          _stm, _features, _as, std::move(cmd), timeout);
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

ss::future<topic_result> topics_frontend::do_create_non_replicable_topic(
  non_replicable_topic data, model::timeout_clock::time_point timeout) {
    if (!validate_topic_name(data.name)) {
        co_return topic_result(
          topic_result(std::move(data.name), errc::invalid_topic_name));
    }

    create_non_replicable_topic_cmd cmd(data, 0);
    try {
        auto ec = co_await replicate_and_wait(
          _stm, _features, _as, std::move(cmd), timeout);
        co_return topic_result(data.name, map_errc(ec));
    } catch (const std::exception& ex) {
        vlog(
          clusterlog.warn,
          "unable to create non replicated topic {} - source topic - {} "
          "reason: "
          "{}",
          data.name,
          data.source,
          ex.what());
        co_return topic_result(std::move(data.name), errc::replication_error);
    }
}

ss::future<std::vector<topic_result>>
topics_frontend::create_non_replicable_topics(
  std::vector<non_replicable_topic> topics,
  model::timeout_clock::time_point timeout) {
    vlog(clusterlog.trace, "Create non_replicable topics {}", topics);
    std::vector<ss::future<topic_result>> futures;
    futures.reserve(topics.size());

    std::transform(
      std::begin(topics),
      std::end(topics),
      std::back_inserter(futures),
      [this, timeout](non_replicable_topic& d) {
          return do_create_non_replicable_topic(std::move(d), timeout);
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

ss::future<std::vector<topic_result>>
topics_frontend::dispatch_create_non_replicable_to_leader(
  model::node_id leader,
  std::vector<non_replicable_topic> topics,
  model::timeout_clock::duration timeout) {
    vlog(clusterlog.trace, "Dispatching create topics to {}", leader);
    return _connections.local()
      .with_node_client<cluster::controller_client_protocol>(
        _self,
        ss::this_shard_id(),
        leader,
        timeout,
        [topics, timeout](controller_client_protocol cp) mutable {
            return cp.create_non_replicable_topics(
              create_non_replicable_topics_request{
                .topics = std::move(topics), .timeout = timeout},
              rpc::client_opts(timeout));
        })
      .then(&rpc::get_ctx_data<create_non_replicable_topics_reply>)
      .then([topics = std::move(topics)](
              result<create_non_replicable_topics_reply> r) {
          if (r.has_error()) {
              return create_topic_results(topics, map_errc(r.error()));
          }
          return std::move(r.value().results);
      });
}

ss::future<std::vector<topic_result>>
topics_frontend::autocreate_non_replicable_topics(
  std::vector<non_replicable_topic> topics,
  model::timeout_clock::duration timeout) {
    vlog(clusterlog.trace, "Create non_replicable topics {}", topics);

    auto leader = _leaders.local().get_leader(model::controller_ntp);

    // no leader available
    if (!leader) {
        return ss::make_ready_future<std::vector<topic_result>>(
          create_topic_results(topics, errc::no_leader_controller));
    }
    // current node is a leader controller
    if (leader == _self) {
        return create_non_replicable_topics(
          std::move(topics), model::time_from_now(timeout));
    }
    // dispatch to leader
    return dispatch_create_non_replicable_to_leader(
      leader.value(), std::move(topics), timeout);
}

topic_result
make_error_result(const model::topic_namespace& tp_ns, std::error_code ec) {
    if (ec.category() == cluster::error_category()) {
        return topic_result(tp_ns, cluster::errc(ec.value()));
    }

    return topic_result(tp_ns, errc::topic_operation_error);
}

allocation_request
make_allocation_request(const custom_assignable_topic_configuration& ca_cfg) {
    // no custom assignments, lets allocator decide based on partition count
    allocation_request req(get_allocation_domain(ca_cfg.cfg.tp_ns));
    if (!ca_cfg.has_custom_assignment()) {
        req.partitions.reserve(ca_cfg.cfg.partition_count);
        for (auto p = 0; p < ca_cfg.cfg.partition_count; ++p) {
            req.partitions.emplace_back(
              model::partition_id(p), ca_cfg.cfg.replication_factor);
        }
    } else {
        req.partitions.reserve(ca_cfg.custom_assignments.size());
        for (auto& cas : ca_cfg.custom_assignments) {
            allocation_constraints constraints;
            constraints.add(on_nodes(cas.replicas));

            req.partitions.emplace_back(
              cas.id, cas.replicas.size(), std::move(constraints));
        }
    }
    return req;
}

errc topics_frontend::validate_topic_configuration(
  const custom_assignable_topic_configuration& assignable_config) {
    if (!validate_topic_name(assignable_config.cfg.tp_ns)) {
        return errc::invalid_topic_name;
    }

    if (assignable_config.cfg.partition_count < 1) {
        return errc::topic_invalid_partitions;
    }

    if (assignable_config.cfg.replication_factor < 1) {
        return errc::topic_invalid_replication_factor;
    }

    if (assignable_config.has_custom_assignment()) {
        for (auto& custom : assignable_config.custom_assignments) {
            if (
              static_cast<int16_t>(custom.replicas.size())
              != assignable_config.cfg.replication_factor) {
                return errc::topic_invalid_replication_factor;
            }
        }
    }

    return errc::success;
}

ss::future<topic_result> topics_frontend::do_create_topic(
  custom_assignable_topic_configuration assignable_config,
  model::timeout_clock::time_point timeout) {
    auto validation_err = validate_topic_configuration(assignable_config);

    if (validation_err != errc::success) {
        co_return topic_result(assignable_config.cfg.tp_ns, validation_err);
    }

    if (assignable_config.is_read_replica()) {
        if (!assignable_config.cfg.properties.read_replica_bucket) {
            co_return make_error_result(
              assignable_config.cfg.tp_ns, errc::topic_invalid_config);
        }
        auto rr_manager = remote_topic_configuration_source(
          _cloud_storage_api.local());

        errc download_res = co_await rr_manager.set_remote_properties_in_config(
          assignable_config,
          cloud_storage_clients::bucket_name(
            assignable_config.cfg.properties.read_replica_bucket.value()),
          _as.local());

        if (download_res != errc::success) {
            co_return make_error_result(
              assignable_config.cfg.tp_ns, errc::topic_operation_error);
        }

        if (!assignable_config.cfg.properties.remote_topic_properties) {
            vassert(
              assignable_config.cfg.properties.remote_topic_properties,
              "remote_topic_properties not set after successful download of "
              "valid topic manifest");
        }
        assignable_config.cfg.partition_count
          = assignable_config.cfg.properties.remote_topic_properties
              ->remote_partition_count;
    }

    if (assignable_config.is_recovery_enabled()) {
        // Before running the recovery we need to download topic_manifest.

        const auto& bucket_config
          = cloud_storage::configuration::get_bucket_config();
        auto bucket = bucket_config.value();
        if (!bucket.has_value()) {
            vlog(
              clusterlog.error,
              "Can't run topic recovery for the topic {}, {} is not set",
              assignable_config.cfg.tp_ns,
              bucket_config.name());
            co_return make_error_result(
              assignable_config.cfg.tp_ns, errc::topic_operation_error);
        }
        auto cfg_source = remote_topic_configuration_source(
          _cloud_storage_api.local());

        errc download_res = co_await cfg_source.set_recovered_topic_properties(
          assignable_config,
          cloud_storage_clients::bucket_name(bucket.value()),
          _as.local());

        if (download_res != errc::success) {
            vlog(
              clusterlog.error,
              "Can't run topic recovery for the topic {}",
              assignable_config.cfg.tp_ns);
            co_return make_error_result(
              assignable_config.cfg.tp_ns, errc::topic_invalid_config);
        }

        vassert(
          static_cast<bool>(
            assignable_config.cfg.properties.remote_topic_properties),
          "remote_topic_properties not set after successful download of valid "
          "topic manifest");

        vlog(
          clusterlog.info,
          "Configured topic recovery for {}, topic configuration: {}",
          assignable_config.cfg.tp_ns,
          assignable_config.cfg);
    }

    auto units = co_await _allocator.invoke_on(
      partition_allocator::shard, [assignable_config](partition_allocator& al) {
          return al.allocate(make_allocation_request(assignable_config));
      });

    if (!units) {
        co_return make_error_result(assignable_config.cfg.tp_ns, units.error());
    }
    co_return co_await replicate_create_topic(
      std::move(assignable_config.cfg), std::move(units.value()), timeout);
}

ss::future<topic_result> topics_frontend::replicate_create_topic(
  topic_configuration cfg,
  allocation_units::pointer units,
  model::timeout_clock::time_point timeout) {
    auto tp_ns = cfg.tp_ns;
    create_topic_cmd cmd(
      tp_ns,
      topic_configuration_assignment(std::move(cfg), units->get_assignments()));

    for (auto& p_as : cmd.value.assignments) {
        std::shuffle(
          p_as.replicas.begin(),
          p_as.replicas.end(),
          random_generators::internal::gen);
    }

    return replicate_and_wait(_stm, _features, _as, std::move(cmd), timeout)
      .then_wrapped([tp_ns = std::move(tp_ns), units = std::move(units)](
                      ss::future<std::error_code> f) mutable {
          try {
              auto error_code = f.get0();
              auto ret_f = ss::now();
              return ret_f.then(
                [tp_ns = std::move(tp_ns), error_code]() mutable {
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

ss::future<std::vector<topic_result>> topics_frontend::delete_topics(
  std::vector<model::topic_namespace> topics,
  model::timeout_clock::time_point timeout) {
    vlog(clusterlog.info, "Delete topics {}", topics);

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

    return replicate_and_wait(_stm, _features, _as, std::move(cmd), timeout)
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
          without_custom_assignments(std::move(topics)),
          model::timeout_clock::now() + timeout);
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
              create_topics_request{
                .topics = std::move(topics), .timeout = timeout},
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
  model::timeout_clock::time_point tout,
  std::optional<model::term_id> term) {
    auto result = co_await stm_linearizable_barrier(tout);
    if (!result) {
        co_return result.error();
    }

    if (_topics.local().is_update_in_progress(ntp)) {
        co_return errc::update_in_progress;
    }

    move_partition_replicas_cmd cmd(std::move(ntp), std::move(new_replica_set));

    co_return co_await replicate_and_wait(
      _stm, _features, _as, std::move(cmd), tout, term);
}

ss::future<std::error_code> topics_frontend::cancel_moving_partition_replicas(
  model::ntp ntp,
  model::timeout_clock::time_point timeout,
  std::optional<model::term_id> term) {
    auto result = co_await stm_linearizable_barrier(timeout);
    if (!result) {
        co_return result.error();
    }
    if (!_topics.local().is_update_in_progress(ntp)) {
        co_return errc::no_update_in_progress;
    }

    cancel_moving_partition_replicas_cmd cmd(
      std::move(ntp),
      cancel_moving_partition_replicas_cmd_data(force_abort_update::no));

    co_return co_await replicate_and_wait(
      _stm, _features, _as, std::move(cmd), timeout, term);
}

ss::future<std::error_code> topics_frontend::abort_moving_partition_replicas(
  model::ntp ntp,
  model::timeout_clock::time_point timeout,
  std::optional<model::term_id> term) {
    auto result = co_await stm_linearizable_barrier(timeout);
    if (!result) {
        co_return result.error();
    }

    if (!_topics.local().is_update_in_progress(ntp)) {
        co_return errc::no_update_in_progress;
    }
    cancel_moving_partition_replicas_cmd cmd(
      std::move(ntp),
      cancel_moving_partition_replicas_cmd_data(force_abort_update::yes));

    co_return co_await replicate_and_wait(
      _stm, _features, _as, std::move(cmd), timeout, term);
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
    // optimization: if update is not in progress return early
    if (!_topics.local().is_update_in_progress(ntp)) {
        return ss::make_ready_future<std::error_code>(
          errc::no_update_in_progress);
    }
    // current node is a leader, just replicate
    if (leader == _self) {
        finish_moving_partition_replicas_cmd cmd(
          std::move(ntp), std::move(new_replica_set));

        return replicate_and_wait(_stm, _features, _as, std::move(cmd), tout);
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
ss::future<std::error_code> topics_frontend::revert_cancel_partition_move(
  model::ntp ntp, model::timeout_clock::time_point tout) {
    auto leader = _leaders.local().get_leader(model::controller_ntp);

    // no leader available
    if (!leader) {
        return ss::make_ready_future<std::error_code>(
          errc::no_leader_controller);
    }
    // optimization: if update is not in progress return early
    if (!_topics.local().is_update_in_progress(ntp)) {
        return ss::make_ready_future<std::error_code>(
          errc::no_update_in_progress);
    }
    // current node is a leader, just replicate
    if (leader == _self) {
        revert_cancel_partition_move_cmd cmd(
          0, revert_cancel_partition_move_cmd_data{.ntp = std::move(ntp)});

        return replicate_and_wait(_stm, _features, _as, std::move(cmd), tout);
    }

    return _connections.local()
      .with_node_client<controller_client_protocol>(
        _self,
        ss::this_shard_id(),
        *leader,
        tout,
        [ntp = std::move(ntp),
         tout](controller_client_protocol client) mutable {
            return client
              .revert_cancel_partition_move(
                revert_cancel_partition_move_request{.ntp = std::move(ntp)},
                rpc::client_opts(tout))
              .then(&rpc::get_ctx_data<revert_cancel_partition_move_reply>);
        })
      .then([](result<revert_cancel_partition_move_reply> r) {
          return r.has_error() ? r.error() : r.value().result;
      });
}

ss::future<result<model::offset>> topics_frontend::stm_linearizable_barrier(
  model::timeout_clock::time_point timeout) {
    return _stm.invoke_on(controller_stm_shard, [timeout](controller_stm& stm) {
        return stm.insert_linearizable_barrier(timeout);
    });
}

ss::future<std::vector<topic_result>> topics_frontend::create_partitions(
  std::vector<create_partitions_configuration> partitions,
  model::timeout_clock::time_point timeout) {
    auto r = co_await stm_linearizable_barrier(timeout);
    if (!r) {
        std::vector<topic_result> results;
        results.reserve(partitions.size());
        std::transform(
          partitions.begin(),
          partitions.end(),
          std::back_inserter(results),
          [err = r.error()](const create_partitions_configuration& cfg) {
              return make_error_result(cfg.tp_ns, err);
          });
        co_return results;
    }

    auto result = co_await ssx::parallel_transform(
      partitions.begin(),
      partitions.end(),
      [this, timeout](create_partitions_configuration cfg) {
          return do_create_partition(std::move(cfg), timeout);
      });

    co_return result;
}

ss::future<bool>
topics_frontend::validate_shard(model::node_id node, uint32_t shard) const {
    return _allocator.invoke_on(
      partition_allocator::shard, [node, shard](partition_allocator& al) {
          return al.state().validate_shard(node, shard);
      });
}

allocation_request make_allocation_request(
  int16_t replication_factor,
  const int32_t current_partitions_count,
  const create_partitions_configuration& cfg) {
    const auto new_partitions_cnt = cfg.new_total_partition_count
                                    - current_partitions_count;
    allocation_request req(get_allocation_domain(cfg.tp_ns));
    req.partitions.reserve(new_partitions_cnt);
    for (auto p = 0; p < new_partitions_cnt; ++p) {
        req.partitions.emplace_back(model::partition_id(p), replication_factor);
    }
    return req;
}

ss::future<topic_result> topics_frontend::do_create_partition(
  create_partitions_configuration p_cfg,
  model::timeout_clock::time_point timeout) {
    auto tp_cfg = _topics.local().get_topic_cfg(p_cfg.tp_ns);
    auto replication_factor = _topics.local().get_topic_replication_factor(
      p_cfg.tp_ns);
    if (!tp_cfg || !replication_factor) {
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
       rf = replication_factor.value()](partition_allocator& al) {
          return al.allocate(make_allocation_request(rf, current, p_cfg));
      });

    // no assignments, error
    if (!units) {
        co_return make_error_result(p_cfg.tp_ns, units.error());
    }

    auto tp_ns = p_cfg.tp_ns;
    create_partitions_configuration_assignment payload(
      std::move(p_cfg), units.value()->get_assignments());
    create_partition_cmd cmd = create_partition_cmd(tp_ns, std::move(payload));

    try {
        auto ec = co_await replicate_and_wait(
          _stm, _features, _as, std::move(cmd), timeout);
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

ss::future<result<std::vector<move_cancellation_result>>>
topics_frontend::cancel_moving_partition_replicas_node(
  model::node_id node_id,
  partition_move_direction dir,
  model::timeout_clock::time_point timeout) {
    using ret_t = result<std::vector<move_cancellation_result>>;
    auto leader = _leaders.local().get_leader(model::controller_ntp);

    // no leader available
    if (!leader) {
        co_return errc::no_leader_controller;
    }
    if (leader == _self) {
        switch (dir) {
        case partition_move_direction::from_node:
            co_return co_await do_cancel_moving_partition_replicas(
              _topics.local().ntps_moving_from_node(node_id), timeout);
        case partition_move_direction::to_node:
            co_return co_await do_cancel_moving_partition_replicas(
              _topics.local().ntps_moving_to_node(node_id), timeout);
        case partition_move_direction::all:
            co_return co_await do_cancel_moving_partition_replicas(
              _topics.local().all_ntps_moving_per_node(node_id), timeout);
        }
        __builtin_unreachable();
    }

    co_return co_await _connections.local()
      .with_node_client<controller_client_protocol>(
        _self,
        ss::this_shard_id(),
        *leader,
        timeout,
        [timeout, node_id, dir](controller_client_protocol client) mutable {
            return client
              .cancel_node_partition_movements(
                cancel_node_partition_movements_request{
                  .node_id = node_id, .direction = dir},
                rpc::client_opts(timeout))
              .then(&rpc::get_ctx_data<cancel_partition_movements_reply>);
        })
      .then([](result<cancel_partition_movements_reply> r) {
          return r.has_error() ? ret_t(r.error())
                               : std::move(r.value().partition_results);
      });
}

ss::future<result<std::vector<move_cancellation_result>>>
topics_frontend::cancel_moving_all_partition_replicas(
  model::timeout_clock::time_point timeout) {
    using ret_t = result<std::vector<move_cancellation_result>>;
    auto leader = _leaders.local().get_leader(model::controller_ntp);

    // no leader available
    if (!leader) {
        co_return errc::no_leader_controller;
    }
    if (leader == _self) {
        co_return co_await do_cancel_moving_partition_replicas(
          _topics.local().all_updates_in_progress(), timeout);
    }

    co_return co_await _connections.local()
      .with_node_client<controller_client_protocol>(
        _self,
        ss::this_shard_id(),
        *leader,
        timeout,
        [timeout](controller_client_protocol client) mutable {
            return client
              .cancel_all_partition_movements(
                cancel_all_partition_movements_request{},
                rpc::client_opts(timeout))
              .then(&rpc::get_ctx_data<cancel_partition_movements_reply>);
        })
      .then([](result<cancel_partition_movements_reply> r) {
          return r.has_error() ? ret_t(r.error())
                               : std::move(r.value().partition_results);
      });
}

ss::future<std::vector<move_cancellation_result>>
topics_frontend::do_cancel_moving_partition_replicas(
  std::vector<model::ntp> ntps, model::timeout_clock::time_point timeout) {
    std::vector<move_cancellation_result> results;
    results.reserve(ntps.size());
    co_await ss::max_concurrent_for_each(
      ntps, 32, [this, &results, timeout](model::ntp& ntp) {
          return cancel_moving_partition_replicas(ntp, timeout)
            .then([ntp = std::move(ntp), &results](std::error_code ec) mutable {
                results.emplace_back(
                  std::move(ntp), map_update_interruption_error_code(ec));
            });
      });

    co_return results;
}

ss::future<std::error_code> topics_frontend::change_replication_factor(
  model::topic_namespace topic,
  cluster::replication_factor new_replication_factor,
  model::timeout_clock::time_point timeout) {
    auto tp_metadata = _topics.local().get_topic_metadata_ref(topic);
    if (!tp_metadata.has_value()) {
        co_return errc::topic_not_exists;
    }
    if (!tp_metadata.value().get().is_topic_replicable()) {
        co_return errc::topic_operation_error;
    }

    auto current_replication_factor
      = tp_metadata.value().get().get_replication_factor();

    if (current_replication_factor == new_replication_factor) {
        co_return errc::success;
    }

    if (new_replication_factor < current_replication_factor) {
        co_return co_await decrease_replication_factor(
          topic, new_replication_factor, timeout);
    }
    co_return co_await increase_replication_factor(
      topic, new_replication_factor, timeout);
}

ss::future<topics_frontend::capacity_info> topics_frontend::get_health_info(
  model::topic_namespace topic, int32_t partition_count) const {
    capacity_info info;

    partitions_filter::partitions_set_t parititon_set;
    for (auto i = 0; i < partition_count; ++i) {
        parititon_set.emplace(i);
    }

    partitions_filter::topic_map_t topic_map;
    topic_map.emplace(topic.tp, std::move(parititon_set));

    partitions_filter partitions_for_report;
    partitions_for_report.namespaces.emplace(topic.ns, std::move(topic_map));

    node_report_filter filter;
    filter.ntp_filters = std::move(partitions_for_report);

    auto health_report = co_await _hm_frontend.local().get_cluster_health(
      cluster_report_filter{.node_report_filter = std::move(filter)},
      force_refresh::no,
      model::timeout_clock::now() + _get_health_report_timeout);

    if (!health_report) {
        vlog(
          clusterlog.info,
          "unable to get health report - {}",
          health_report.error().message());
        co_return info;
    }

    for (const auto& node_report : health_report.value().node_reports) {
        uint64_t total = 0;
        uint64_t free = 0;

        // This health report is just on the data disk.  If the cache has
        // a separate disk, it is not reflected in the node health.
        total += node_report.local_state.data_disk.total;
        free += node_report.local_state.data_disk.free;

        info.node_disk_reports.emplace(
          node_report.id, node_disk_space(node_report.id, total, total - free));
    }

    for (auto& node_report : health_report.value().node_reports) {
        co_await ss::max_concurrent_for_each(
          std::move(node_report.topics),
          32,
          [&info](const topic_status& status) {
              for (const auto& partition : status.partitions) {
                  info.ntp_sizes[partition.id] = partition.size_bytes;
              }
              return ss::now();
          });
    }

    co_return info;
}

partition_constraints topics_frontend::get_partition_constraints(
  model::partition_id id,
  cluster::replication_factor new_replication_factor,
  double max_disk_usage_ratio,
  const capacity_info& info) const {
    allocation_constraints allocation_constraints;

    // Add constraint on least disk usage
    allocation_constraints.add(
      least_disk_filled(max_disk_usage_ratio, info.node_disk_reports));

    auto partition_size_it = info.ntp_sizes.find(id);
    if (partition_size_it == info.ntp_sizes.end()) {
        return {id, new_replication_factor, std::move(allocation_constraints)};
    }

    // Add constraint on partition max_disk_usage_ratio overfill
    allocation_constraints.add(disk_not_overflowed_by_partition(
      max_disk_usage_ratio, partition_size_it->second, info.node_disk_reports));

    return {id, new_replication_factor, std::move(allocation_constraints)};
}

ss::future<std::error_code> topics_frontend::increase_replication_factor(
  model::topic_namespace topic,
  cluster::replication_factor new_replication_factor,
  model::timeout_clock::time_point timeout) {
    std::vector<move_topic_replicas_data> new_assignments;

    if (
      static_cast<size_t>(new_replication_factor)
      > _members_table.local().node_count()) {
        vlog(
          clusterlog.warn,
          "New replication factor({}) is greater than number of brokers({})",
          new_replication_factor,
          _members_table.local().node_count());
        co_return errc::topic_invalid_replication_factor;
    }

    auto tp_metadata = _topics.local().get_topic_metadata(topic);
    if (!tp_metadata.has_value()) {
        co_return errc::topic_not_exists;
    }

    auto partition_count = tp_metadata->get_configuration().partition_count;

    // units shold exist during replicate_and_wait call
    using units_from_allocator
      = ss::foreign_ptr<std::unique_ptr<allocation_units>>;
    std::vector<units_from_allocator> units;
    units.reserve(partition_count);

    std::optional<std::error_code> error;

    auto healt_report = co_await get_health_info(topic, partition_count);

    auto hard_max_disk_usage_ratio = (100 - _hard_max_disk_usage_ratio())
                                     / 100.0;

    auto partition_constraints = get_partition_constraints(
      model::partition_id(0),
      new_replication_factor,
      hard_max_disk_usage_ratio,
      healt_report);

    co_await ss::max_concurrent_for_each(
      tp_metadata->get_assignments(),
      32,
      [this,
       &units,
       &new_assignments,
       &error,
       &partition_constraints,
       topic,
       new_replication_factor](partition_assignment& assignment) {
          if (error) {
              return ss::now();
          }
          auto p_id = assignment.id;
          if (assignment.replicas.size() > new_replication_factor) {
              vlog(
                clusterlog.warn,
                "Current size of replicas {} > new_replication_factor {} for "
                "topic {} and partition {}",
                assignment.replicas.size(),
                new_replication_factor,
                topic,
                p_id);
              error = errc::topic_invalid_replication_factor;
              return ss::now();
          }

          partition_constraints.partition_id = p_id;

          auto ntp = model::ntp(topic.ns, topic.tp, p_id);

          return _allocator
            .invoke_on(
              partition_allocator::shard,
              [partition_constraints,
               assignment = std::move(assignment),
               ntp = std::move(ntp)](partition_allocator& al) {
                  return al.reallocate_partition(
                    partition_constraints,
                    assignment,
                    get_allocation_domain(ntp));
              })
            .then([&error, &units, &new_assignments, topic, p_id](
                    result<allocation_units> reallocation) {
                if (!reallocation) {
                    vlog(
                      clusterlog.warn,
                      "attempt to find reallocation for topic {}, partition {} "
                      "failed, error: {}",
                      topic,
                      p_id,
                      reallocation.error().message());
                    error = reallocation.error();
                    return;
                }

                units_from_allocator ptr = std::make_unique<allocation_units>(
                  std::move(reallocation.value()));

                units.emplace_back(std::move(ptr));

                new_assignments.emplace_back(
                  p_id, units.back()->get_assignments().front().replicas);
            });
      });

    if (error) {
        co_return error.value();
    }

    move_topic_replicas_cmd cmd(topic, std::move(new_assignments));

    co_return co_await replicate_and_wait(
      _stm, _features, _as, std::move(cmd), timeout);
}

ss::future<std::error_code> topics_frontend::decrease_replication_factor(
  model::topic_namespace topic,
  cluster::replication_factor new_replication_factor,
  model::timeout_clock::time_point timeout) {
    std::vector<move_topic_replicas_data> new_assignments;

    auto tp_metadata = _topics.local().get_topic_metadata_ref(topic);
    if (!tp_metadata.has_value()) {
        co_return errc::topic_not_exists;
    }

    std::optional<std::error_code> error;

    auto metadata_ref = tp_metadata.value().get();

    co_await ss::max_concurrent_for_each(
      metadata_ref.get_assignments(),
      32,
      [&new_assignments, &error, topic, new_replication_factor](
        partition_assignment& assignment) {
          if (error) {
              return ss::now();
          }
          if (assignment.replicas.size() < new_replication_factor) {
              error = errc::topic_invalid_replication_factor;
              return ss::now();
          }

          new_assignments.emplace_back(move_topic_replicas_data());
          new_assignments.back().partition = assignment.id;
          new_assignments.back().replicas.resize(new_replication_factor);
          std::copy_n(
            assignment.replicas.begin(),
            new_replication_factor,
            new_assignments.back().replicas.begin());
          return ss::now();
      });

    if (error) {
        co_return error.value();
    }

    move_topic_replicas_cmd cmd(topic, std::move(new_assignments));

    co_return co_await replicate_and_wait(
      _stm, _features, _as, std::move(cmd), timeout);
}

allocation_request make_allocation_request(
  model::ntp ntp,
  replication_factor tp_replication_factor,
  const std::vector<model::node_id>& new_replicas) {
    allocation_request req(
      get_allocation_domain(model::topic_namespace{ntp.ns, ntp.tp.topic}));
    req.partitions.reserve(1);
    allocation_constraints constraints;
    constraints.add(on_nodes(new_replicas));
    req.partitions.emplace_back(
      ntp.tp.partition, tp_replication_factor, std::move(constraints));
    return req;
}

ss::future<result<std::vector<partition_assignment>>>
topics_frontend::generate_reassignments(
  model::ntp ntp, std::vector<model::node_id> new_replicas) {
    auto tp_metadata = _topics.local().get_topic_metadata_ref(
      model::topic_namespace{ntp.ns, ntp.tp.topic});
    if (!tp_metadata.has_value()) {
        co_return errc::topic_not_exists;
    }

    auto tp_replication_factor
      = tp_metadata.value().get().get_replication_factor();

    auto units = co_await _allocator.invoke_on(
      partition_allocator::shard,
      [ntp, tp_replication_factor, new_replicas{std::move(new_replicas)}](
        partition_allocator& al) {
          return al.allocate(
            make_allocation_request(ntp, tp_replication_factor, new_replicas));
      });

    if (!units) {
        co_return units.error();
    }

    auto assignments = units.value()->get_assignments();
    if (assignments.empty()) {
        co_return errc::no_partition_assignments;
    }

    co_return assignments;
}

ss::future<std::error_code> topics_frontend::move_partition_replicas(
  model::ntp ntp,
  std::vector<model::node_id> new_replica_set,
  model::timeout_clock::time_point tout,
  std::optional<model::term_id> term) {
    auto assignments = co_await generate_reassignments(
      ntp, std::move(new_replica_set));

    // The return type from generate_reassignments is a
    // result<vector<partition_assignment>>. So we need to make sure
    // the result: 1) has a value and 2) the vector should only have one replica
    // set, so check that the front partition id matches the request.

    if (!assignments) {
        co_return assignments.error();
    }

    if (assignments.value().front().id != ntp.tp.partition) {
        co_return errc::allocation_error;
    }

    co_return co_await move_partition_replicas(
      ntp, std::move(assignments.value().front().replicas), tout, term);
}

ss::future<result<partition_state_reply>>
topics_frontend::do_get_partition_state(model::node_id node, model::ntp ntp) {
    if (node == _self) {
        partition_state_reply reply{};
        auto shard = _shard_table.local().shard_for(ntp);
        if (!shard) {
            reply.error_code = errc::partition_not_exists;
            return ss::make_ready_future<result<partition_state_reply>>(reply);
        }
        return _pm.invoke_on(
          *shard,
          [ntp = std::move(ntp),
           reply = std::move(reply)](partition_manager& pm) mutable {
              auto partition = pm.get(ntp);
              if (!partition) {
                  reply.error_code = errc::partition_not_exists;
                  return ss::make_ready_future<result<partition_state_reply>>(
                    reply);
              }
              reply.state = ::cluster::get_partition_state(partition);
              reply.error_code = errc::success;
              return ss::make_ready_future<result<partition_state_reply>>(
                reply);
          });
    }
    auto timeout = model::timeout_clock::now() + 5s;
    return _connections.local().with_node_client<controller_client_protocol>(
      _self,
      ss::this_shard_id(),
      node,
      timeout,
      [ntp = std::move(ntp),
       timeout](controller_client_protocol client) mutable {
          return client
            .get_partition_state(
              partition_state_request{.ntp = ntp}, rpc::client_opts(timeout))
            .then(&rpc::get_ctx_data<partition_state_reply>);
      });
}

ss::future<result<std::vector<partition_state>>>
topics_frontend::get_partition_state(model::ntp ntp) {
    const auto& topics = _topics.local();
    if (!topics.contains(model::topic_namespace_view(ntp))) {
        co_return errc::partition_not_exists;
    }
    std::set<model::node_id> nodes_to_query;
    const auto& current = topics.get_partition_assignment(ntp);
    if (current) {
        const auto& bss = current->replicas;
        std::for_each(
          bss.begin(),
          bss.end(),
          [&nodes_to_query](const model::broker_shard& bs) {
              nodes_to_query.insert(bs.node_id);
          });
    }
    const auto& prev = topics.get_previous_replica_set(ntp);
    if (prev) {
        std::for_each(
          prev.value().begin(),
          prev.value().end(),
          [&nodes_to_query](const model::broker_shard& bs) {
              nodes_to_query.insert(bs.node_id);
          });
    }

    if (nodes_to_query.empty()) {
        co_return errc::no_partition_assignments;
    }

    std::vector<ss::future<result<partition_state_reply>>> futures;
    futures.reserve(nodes_to_query.size());
    for (const auto& node : nodes_to_query) {
        futures.push_back(do_get_partition_state(node, ntp));
    }

    auto finished_futures = co_await ss::when_all(
      futures.begin(), futures.end());
    std::vector<partition_state> results;
    results.reserve(finished_futures.size());
    for (auto& fut : finished_futures) {
        if (fut.failed()) {
            auto ex = fut.get_exception();
            vlog(
              clusterlog.debug,
              "Failed to get partition state for ntp: {}, failure: {}",
              ntp,
              ex);
            continue;
        }
        auto result = fut.get();
        if (result.has_error()) {
            vlog(
              clusterlog.debug,
              "Failed to get partition state for ntp: {}, result: {}",
              ntp,
              result.error());
            continue;
        }
        auto res = std::move(result.value());
        if (res.error_code != errc::success || !res.state) {
            vlog(
              clusterlog.debug,
              "Error during partition state fetch for ntp: {}, error: "
              "{}",
              ntp,
              res.error_code);
            continue;
        }
        results.push_back(std::move(*res.state));
    }
    co_return results;
}

} // namespace cluster

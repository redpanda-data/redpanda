// Copyright 2023 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cluster/tx_registry_frontend.h"

#include "cluster/controller.h"
#include "cluster/logger.h"
#include "cluster/members_table.h"
#include "cluster/metadata_cache.h"
#include "cluster/partition_leaders_table.h"
#include "cluster/partition_manager.h"
#include "cluster/shard_table.h"
#include "cluster/topics_frontend.h"
#include "cluster/tx_coordinator_mapper.h"
#include "cluster/tx_gateway_service.h"
#include "cluster/tx_helpers.h"
#include "cluster/types.h"
#include "config/configuration.h"
#include "model/namespace.h"
#include "model/record_batch_reader.h"
#include "rpc/connection_cache.h"
#include "vformat.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/do_with.hh>
#include <seastar/core/sharded.hh>

namespace cluster {
using namespace std::chrono_literals;

cluster::errc map_errc_fixme(std::error_code ec);

tx_registry_frontend::tx_registry_frontend(
  ss::smp_service_group ssg,
  ss::sharded<cluster::partition_manager>& partition_manager,
  ss::sharded<cluster::shard_table>& shard_table,
  ss::sharded<cluster::metadata_cache>& metadata_cache,
  ss::sharded<rpc::connection_cache>& connection_cache,
  ss::sharded<partition_leaders_table>& leaders,
  std::unique_ptr<cluster::controller>& controller,
  ss::sharded<cluster::tx_coordinator_mapper>& tx_coordinator_ntp_mapper,
  ss::sharded<features::feature_table>& feature_table)
  : _ssg(ssg)
  , _partition_manager(partition_manager)
  , _shard_table(shard_table)
  , _metadata_cache(metadata_cache)
  , _connection_cache(connection_cache)
  , _leaders(leaders)
  , _controller(controller)
  , _tx_coordinator_ntp_mapper(tx_coordinator_ntp_mapper)
  , _feature_table(feature_table)
  , _metadata_dissemination_retries(
      config::shard_local_cfg().metadata_dissemination_retries.value())
  , _metadata_dissemination_retry_delay_ms(
      config::shard_local_cfg().metadata_dissemination_retry_delay_ms.value()) {
}

template<typename Func>
auto tx_registry_frontend::with_stm(Func&& func) {
    auto partition = _partition_manager.local().get(model::tx_registry_ntp);
    if (!partition) {
        vlog(
          txlog.warn, "can't get partition by {} ntp", model::tx_registry_ntp);
        return func(tx_errc::partition_not_found);
    }

    auto stm = partition->tx_registry_stm();

    if (!stm) {
        vlog(
          txlog.warn,
          "can't get tx_registry_stm of the {}' partition",
          model::tx_registry_ntp);
        return func(tx_errc::stm_not_found);
    }

    if (stm->gate().is_closed()) {
        return func(tx_errc::not_coordinator);
    }

    return ss::with_gate(stm->gate(), [func = std::forward<Func>(func), stm]() {
        vlog(txlog.trace, "entered tx_registry_stm's gate");
        return func(stm).finally(
          [] { vlog(txlog.trace, "leaving tx_registry_stm's gate"); });
    });
}

ss::future<bool> tx_registry_frontend::ensure_tx_topic_exists() {
    if (_metadata_cache.local().contains(model::tx_manager_nt)) {
        co_return true;
    }

    if (!co_await try_create_tx_topic()) {
        vlog(clusterlog.warn, "failed to create {}", model::tx_manager_nt);
        co_return false;
    }

    auto retries = _metadata_dissemination_retries;
    auto delay_ms = _metadata_dissemination_retry_delay_ms;
    std::optional<std::string> error;
    while (!_as.abort_requested() && 0 < retries--) {
        auto is_cache_filled = _metadata_cache.local().contains(
          model::tx_manager_nt);
        if (unlikely(!is_cache_filled)) {
            error = vformat(
              fmt::runtime("can't find {} in the metadata_cache cache"),
              model::tx_manager_nt);
            vlog(
              clusterlog.trace,
              "waiting for {} to fill metadata_cache cache, retries left: {}",
              model::tx_manager_nt,
              retries);
            co_await sleep_abortable(delay_ms, _as);
            continue;
        }
        co_return true;
    }

    if (error) {
        vlog(clusterlog.warn, "{}", error.value());
    }

    co_return false;
}

ss::future<find_coordinator_reply> tx_registry_frontend::find_coordinator(
  kafka::transactional_id tid, model::timeout_clock::duration timeout) {
    if (!_metadata_cache.local().contains(model::tx_manager_nt)) {
        if (!co_await try_create_tx_topic()) {
            vlog(clusterlog.warn, "failed to create {}", model::tx_manager_nt);
            co_return find_coordinator_reply(
              std::nullopt, std::nullopt, errc::topic_not_exists);
        }
    }

    if (!_feature_table.local().is_active(
          features::feature::transaction_partitioning)) {
        co_return co_await find_coordinator_statically(tid);
    }

    auto has_topic = true;

    if (!_metadata_cache.local().contains(
          model::tx_registry_nt, model::partition_id(0))) {
        has_topic = co_await try_create_tx_registry_topic();
    }

    if (!has_topic) {
        vlog(
          clusterlog.warn,
          "can't find {} in the metadata cache",
          model::tx_registry_nt);
        co_return find_coordinator_reply(
          std::nullopt, std::nullopt, errc::topic_not_exists);
    }

    auto _self = _controller->self();

    auto r = find_coordinator_reply(
      std::nullopt, std::nullopt, errc::not_leader);

    auto retries = _metadata_dissemination_retries;
    auto delay_ms = _metadata_dissemination_retry_delay_ms;
    std::optional<std::string> error;
    while (!_as.abort_requested() && 0 < retries--) {
        auto leader_opt = _leaders.local().get_leader(model::tx_registry_ntp);
        if (unlikely(!leader_opt)) {
            error = vformat(
              fmt::runtime("can't find {} in the leaders cache"),
              model::tx_registry_ntp);
            vlog(
              clusterlog.trace,
              "waiting for {} to fill leaders cache, retries left: {}",
              model::tx_registry_ntp,
              retries);
            co_await sleep_abortable(delay_ms, _as);
            continue;
        }
        auto leader = leader_opt.value();

        if (leader == _self) {
            r = co_await find_coordinator_locally(tid);
        } else {
            vlog(
              clusterlog.trace,
              "dispatching find_coordinator({}) from {} to {} ",
              tid,
              _self,
              leader);
            r = co_await dispatch_find_coordinator(leader, tid, timeout);
        }

        if (likely(r.ec == errc::success)) {
            error = std::nullopt;
            break;
        }

        if (likely(r.ec != errc::replication_error)) {
            error = vformat(
              fmt::runtime("find_coordinator({}) failed with {}"), tid, r.ec);
            break;
        }

        error = vformat(
          fmt::runtime("find_coordinator({}) failed with {}"), tid, r.ec);
        vlog(
          clusterlog.trace,
          "find_coordinator({}) failed, retries left: {}",
          tid,
          retries);
        co_await sleep_abortable(delay_ms, _as);
    }

    if (error) {
        vlog(clusterlog.warn, "{}", error.value());
    }

    co_return r;
}

ss::future<find_coordinator_reply>
tx_registry_frontend::dispatch_find_coordinator(
  model::node_id leader,
  kafka::transactional_id tid,
  model::timeout_clock::duration timeout) {
    return _connection_cache.local()
      .with_node_client<cluster::tx_gateway_client_protocol>(
        _controller->self(),
        ss::this_shard_id(),
        leader,
        timeout,
        [timeout, tid](tx_gateway_client_protocol cp) {
            return cp.find_coordinator(
              find_coordinator_request(tid),
              rpc::client_opts(model::timeout_clock::now() + timeout));
        })
      .then(&rpc::get_ctx_data<find_coordinator_reply>)
      .then([](result<find_coordinator_reply> r) {
          if (r.has_error()) {
              vlog(
                clusterlog.warn,
                "got error {} on remote find_coordinator",
                r.error().message());

              return find_coordinator_reply(
                std::nullopt, std::nullopt, errc::timeout);
          }
          return r.value();
      });
}

ss::future<find_coordinator_reply>
tx_registry_frontend::find_coordinator_locally(kafka::transactional_id tid) {
    auto shard = _shard_table.local().shard_for(model::tx_registry_ntp);

    if (!shard) {
        vlog(
          clusterlog.warn,
          "can't find {} in the shard table",
          model::tx_registry_ntp);
        co_return find_coordinator_reply(
          std::nullopt, std::nullopt, errc::not_leader);
    }

    co_return co_await container().invoke_on(
      *shard, _ssg, [tid](tx_registry_frontend& self) mutable {
          return ss::with_gate(self._gate, [&self, tid]() {
              return self.with_stm(
                [&self,
                 tid](checked<ss::shared_ptr<tx_registry_stm>, tx_errc> r) {
                    if (!r) {
                        return ss::make_ready_future<find_coordinator_reply>(
                          find_coordinator_reply(
                            std::nullopt,
                            std::nullopt,
                            errc::generic_tx_error));
                    }
                    auto stm = r.value();
                    return stm->read_lock().then(
                      [&self, tid](ss::basic_rwlock<>::holder unit) mutable {
                          return self.do_find_coordinator_locally(tid).finally(
                            [u = std::move(unit)] {});
                      });
                });
          });
      });
}

ss::future<find_coordinator_reply>
tx_registry_frontend::do_find_coordinator_locally(kafka::transactional_id tid) {
    return find_coordinator_statically(tid);
}

ss::future<find_coordinator_reply>
tx_registry_frontend::find_coordinator_statically(kafka::transactional_id tid) {
    std::optional<model::node_id> leader = std::nullopt;

    auto ntp = co_await _tx_coordinator_ntp_mapper.local().ntp_for(tid);
    if (ntp) {
        leader = _metadata_cache.local().get_leader_id(ntp.value());
    } else {
        vlog(
          txlog.warn,
          "Topic {} doesn't exist in metadata cache",
          model::tx_manager_nt);
    }

    co_return find_coordinator_reply(leader, ntp, errc::success);
}

ss::future<bool> tx_registry_frontend::try_create_tx_registry_topic() {
    cluster::topic_configuration topic{
      model::kafka_internal_namespace,
      model::tx_registry_topic,
      1,
      _controller->internal_topic_replication()};

    topic.properties.cleanup_policy_bitflags
      = model::cleanup_policy_bitflags::none;

    return _controller->get_topics_frontend()
      .local()
      .autocreate_topics(
        {std::move(topic)}, config::shard_local_cfg().create_topic_timeout_ms())
      .then([](std::vector<cluster::topic_result> res) {
          vassert(res.size() == 1, "expected exactly one result");
          if (res[0].ec == cluster::errc::topic_already_exists) {
              return true;
          }
          if (res[0].ec != cluster::errc::success) {
              vlog(
                clusterlog.warn,
                "can not create {}/{} topic - error: {}",
                model::kafka_internal_namespace,
                model::tx_registry_topic,
                cluster::make_error_code(res[0].ec).message());
              return false;
          }
          return true;
      })
      .handle_exception([](std::exception_ptr e) {
          vlog(
            clusterlog.warn,
            "can not create {}/{} topic - error: {}",
            model::kafka_internal_namespace,
            model::tx_registry_topic,
            e);
          return false;
      });
}

ss::future<bool> tx_registry_frontend::try_create_tx_topic() {
    int32_t partitions_amount = 1;
    if (_feature_table.local().is_active(
          features::feature::transaction_partitioning)) {
        partitions_amount
          = config::shard_local_cfg().transaction_coordinator_partitions();
    }

    cluster::topic_configuration topic{
      model::kafka_internal_namespace,
      model::tx_manager_topic,
      partitions_amount,
      _controller->internal_topic_replication()};

    topic.properties.segment_size
      = config::shard_local_cfg().transaction_coordinator_log_segment_size;
    topic.properties.retention_duration = tristate<std::chrono::milliseconds>(
      config::shard_local_cfg().transaction_coordinator_delete_retention_ms());
    topic.properties.cleanup_policy_bitflags
      = config::shard_local_cfg().transaction_coordinator_cleanup_policy();

    return _controller->get_topics_frontend()
      .local()
      .autocreate_topics(
        {std::move(topic)},
        config::shard_local_cfg().create_topic_timeout_ms() * partitions_amount)
      .then([](std::vector<cluster::topic_result> res) {
          vassert(res.size() == 1, "expected exactly one result");
          if (res[0].ec == cluster::errc::topic_already_exists) {
              return true;
          }
          if (res[0].ec != cluster::errc::success) {
              vlog(
                clusterlog.warn,
                "can not create {}/{} topic - error: {}",
                model::kafka_internal_namespace,
                model::tx_manager_topic,
                cluster::make_error_code(res[0].ec).message());
              return false;
          }
          return true;
      })
      .handle_exception([](std::exception_ptr e) {
          vlog(
            txlog.warn,
            "can not create {}/{} topic - error: {}",
            model::kafka_internal_namespace,
            model::tx_manager_topic,
            e);
          return false;
      });
}

} // namespace cluster

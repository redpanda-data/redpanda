// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cluster/logger.h"
#include "cluster/metadata_cache.h"
#include "cluster/partition_leaders_table.h"
#include "cluster/partition_manager.h"
#include "cluster/rm_group_proxy.h"
#include "cluster/rm_partition_frontend.h"
#include "cluster/shard_table.h"
#include "cluster/tm_stm.h"
#include "cluster/tm_stm_types.h"
#include "cluster/tx_coordinator_mapper.h"
#include "cluster/tx_errc.h"
#include "cluster/tx_gateway_frontend.h"
#include "cluster/tx_gateway_service.h"
#include "cluster/tx_helpers.h"
#include "cluster/tx_topic_manager.h"
#include "config/configuration.h"
#include "model/fundamental.h"
#include "model/namespace.h"
#include "rpc/connection_cache.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/future.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/shared_ptr.hh>

#include <algorithm>
#include <utility>

namespace cluster {
using namespace std::chrono_literals;

template<typename Func>
static auto with(
  ss::shared_ptr<tm_stm> stm,
  const kafka::transactional_id& tx_id,
  const std::string_view name,
  Func&& func) {
    return stm->lock_tx(tx_id, name)
      .then([stm, func = std::forward<Func>(func)](auto units) mutable {
          return ss::futurize_invoke(std::forward<Func>(func))
            .finally([units = std::move(units)] {});
      });
}

namespace {

bool need_to_advance_progress(const tx_metadata& tx) {
    return tx.status == tx_status::preparing_abort
           || tx.status == tx_status::preparing_commit
           || tx.status == tx_status::preparing_internal_abort;
}

tx::errc map_state_update_outcome(tm_stm::op_status result) {
    switch (result) {
    case tm_stm::success:
        return tx::errc::none;
    case tm_stm::conflict:
        return tx::errc::invalid_txn_state;
    case tm_stm::unknown:
    case tm_stm::timeout:
        return tx::errc::timeout;
    case tm_stm::not_leader:
        return tx::errc::not_coordinator;
    case tm_stm::partition_not_found:
        return tx::errc::partition_not_found;
    case tm_stm::not_found:
        return tx::errc::unknown_server_error;
    }
}
ss::future<checked<model::term_id, tx::errc>>
sync_stm(ss::shared_ptr<tm_stm>& stm) {
    auto r = co_await stm->sync();
    if (!r) {
        co_return map_state_update_outcome(r.error());
    }
    co_return r.value();
}
} // namespace

static auto send(tx_gateway_client_protocol& cp, try_abort_request&& request) {
    auto timeout = request.timeout;
    return cp.try_abort(
      std::move(request),
      rpc::client_opts(model::timeout_clock::now() + timeout));
}

template<typename Func>
auto tx_gateway_frontend::with_stm(model::partition_id tm, Func&& func) {
    model::ntp tx_ntp(model::tx_manager_nt.ns, model::tx_manager_nt.tp, tm);
    auto partition = _partition_manager.local().get(tx_ntp);
    if (!partition) {
        vlog(txlog.warn, "can't get partition by {} ntp", tx_ntp);
        return func(tx::errc::partition_not_found);
    }

    auto stm = partition->tm_stm();

    if (!stm) {
        vlog(txlog.warn, "can't get tm stm of the {}' partition", tx_ntp);
        return func(tx::errc::stm_not_found);
    }

    if (stm->gate().is_closed()) {
        return func(tx::errc::not_coordinator);
    }

    return ss::with_gate(
      stm->gate(),
      [func = std::forward<Func>(func), stm]() mutable { return func(stm); });
}

template<typename T>
ss::future<typename T::reply>
tx_gateway_frontend::do_route_globally(model::ntp tx_ntp, T&& request) {
    vlog(txlog.trace, "route globally ntp: {}, request: {}", tx_ntp, request);

    if (!_metadata_cache.local().contains(tx_ntp)) {
        vlog(txlog.warn, "can't find {} in the metadata cache", tx_ntp);
        co_return typename T::reply(tx::errc::partition_not_exists);
    }

    auto leader_opt = _leaders.local().get_leader(tx_ntp);
    if (!leader_opt) {
        vlog(txlog.warn, "can't find a leader for {}", tx_ntp);
        co_return typename T::reply(tx::errc::leader_not_found);
    }
    auto leader = leader_opt.value();

    if (leader == _self) {
        co_return co_await do_route_locally(tx_ntp, std::forward<T>(request));
    }

    co_return co_await do_dispatch(leader, std::forward<T>(request));
}

template<typename T>
ss::future<typename T::reply>
tx_gateway_frontend::do_dispatch(model::node_id target, T&& request) {
    vlog(
      txlog.trace,
      "dispatching name: {}, from: {}, to: {}, request: {}",
      T::name,
      _self,
      target,
      request);
    auto timeout = request.timeout;
    return _connection_cache.local()
      .with_node_client<tx_gateway_client_protocol>(
        _self,
        ss::this_shard_id(),
        target,
        timeout,
        [request = std::forward<T>(request)](
          tx_gateway_client_protocol cp) mutable {
            return send(cp, std::move(request));
        })
      .then(&rpc::get_ctx_data<typename T::reply>)
      .then([](result<typename T::reply> r) {
          if (r.has_error()) {
              vlog(
                txlog.warn, "received name: {} error: {}", T::name, r.error());
              return typename T::reply(tx::errc::unknown_server_error);
          }
          auto reply = r.value();
          vlog(txlog.trace, "received name: {} {}", T::name, reply);
          return reply;
      });
}

template<typename T>
ss::future<typename T::reply>
tx_gateway_frontend::do_route_locally(model::ntp tx_ntp, T&& request) {
    vlog(txlog.trace, "processing name: {} {}", T::name, request);

    auto shard = _shard_table.local().shard_for(tx_ntp);

    if (!shard.has_value()) {
        vlog(
          txlog.warn,
          "ntp {} shard not found when processing: {}",
          tx_ntp,
          request);
        co_return typename T::reply(tx::errc::shard_not_found);
    }

    co_return co_await container().invoke_on(
      *shard,
      _ssg,
      [tm = tx_ntp.tp.partition, request = std::forward<T>(request)](
        tx_gateway_frontend& self) -> ss::future<typename T::reply> {
          if (self._gate.is_closed()) {
              return ss::make_ready_future<typename T::reply>(
                tx::errc::not_coordinator);
          }

          return ss::with_gate(
            self._gate, [&self, tm, request = std::move(request)] {
                return self.with_stm(
                  tm,
                  [&self, request = std::move(request)](
                    checked<ss::shared_ptr<tm_stm>, tx::errc> r) mutable {
                      if (!r) {
                          return ss::make_ready_future<typename T::reply>(
                            r.error());
                      }
                      auto stm = r.value();
                      return self.process_locally(stm, std::move(request))
                        .then([](typename T::reply r) {
                            vlog(txlog.trace, "result of {}: {}", T::name, r);
                            return r;
                        });
                  });
            });
      });
}

static add_partitions_tx_reply make_add_partitions_error_response(
  const add_partitions_tx_request& request, tx::errc ec) {
    add_partitions_tx_reply response;
    response.results.reserve(request.topics.size());
    for (auto& req_topic : request.topics) {
        add_partitions_tx_reply::topic_result res_topic;
        res_topic.name = req_topic.name;
        res_topic.results.reserve(req_topic.partitions.size());
        for (model::partition_id req_partition : req_topic.partitions) {
            add_partitions_tx_reply::partition_result res_partition;
            res_partition.partition_index = req_partition;
            res_partition.error_code = ec;
            res_topic.results.push_back(res_partition);
        }
        response.results.push_back(res_topic);
    }
    return response;
}

ss::future<add_partitions_tx_reply> tx_gateway_frontend::add_partition_to_tx(
  add_partitions_tx_request request, model::timeout_clock::duration timeout) {
    auto tx_ntp_opt = ntp_for_tx_id(request.transactional_id);
    if (!tx_ntp_opt) {
        vlog(
          txlog.trace,
          "[tx_id={}] unable to find ntp, producer_id: {}, epoch: {}",
          request.transactional_id,
          request.producer_id,
          request.producer_epoch);
        co_return make_add_partitions_error_response(
          request, tx::errc::coordinator_not_available);
    }
    auto tx_ntp = std::move(tx_ntp_opt.value());
    auto leader = co_await wait_for_leader(tx_ntp);
    if (leader != _self) {
        vlog(
          txlog.trace,
          "[tx_id={}] current node is not a leader for {}, current leader: {}",
          request.transactional_id,
          tx_ntp,
          leader);
        co_return make_add_partitions_error_response(
          request, tx::errc::not_coordinator);
    }

    auto shard = _shard_table.local().shard_for(tx_ntp);

    if (shard == std::nullopt) {
        vlog(
          txlog.trace,
          "[tx_id={}] can't find a shard for {}, producer_id: {}, epoch: {}",
          request.transactional_id,
          tx_ntp,
          request.producer_id,
          request.producer_epoch);
        co_return make_add_partitions_error_response(
          request, tx::errc::coordinator_not_available);
    }
    vlog(
      txlog.trace,
      "[tx_id={}] adding partition to tx. pid: {}, epoch: {}, topics: {}",
      request.transactional_id,
      request.producer_id,
      request.producer_epoch,
      request.topics);
    co_return co_await container().invoke_on(
      *shard,
      _ssg,
      [request = std::move(request), timeout, tm = tx_ntp.tp.partition](
        tx_gateway_frontend& self) mutable
        -> ss::future<add_partitions_tx_reply> {
          return ss::with_gate(
            self._gate,
            [request = std::move(request), timeout, tm, &self]() mutable
              -> ss::future<add_partitions_tx_reply> {
                return self.with_stm(
                  tm,
                  [request = std::move(request), timeout, &self](
                    checked<ss::shared_ptr<tm_stm>, tx::errc> r) mutable {
                      if (!r) {
                          return ss::make_ready_future<add_partitions_tx_reply>(
                            make_add_partitions_error_response(
                              request, r.error()));
                      }
                      auto stm = r.value();
                      return stm->read_lock().then(
                        [&self, stm, request = std::move(request), timeout](
                          ss::basic_rwlock<>::holder unit) mutable {
                            auto tx_id = request.transactional_id;
                            return with(
                                     stm,
                                     tx_id,
                                     "add_partition_to_tx",
                                     [&self,
                                      stm,
                                      request = std::move(request),
                                      timeout]() mutable {
                                         return self.do_add_partition_to_tx(
                                           stm, std::move(request), timeout);
                                     })
                              .finally([u = std::move(unit)] {});
                        });
                  });
            });
      });
}
ss::future<tx_gateway_frontend::op_result_t>
tx_gateway_frontend::init_add_resource_to_tx(
  const kafka::transactional_id& transactional_id,
  model::term_id term,
  model::producer_identity pid,
  ss::shared_ptr<tm_stm> stm,
  model::timeout_clock::duration timeout) {
    auto latest_tx = co_await get_latest_tx(
      term, stm, pid, transactional_id, timeout);

    if (!latest_tx.has_value()) {
        vlog(
          txlog.info,
          "[tx_id={}] error getting latest transaction - {}",
          transactional_id,
          latest_tx.error());
        if (latest_tx.error() == tx::errc::tx_not_found) {
            // tx doesn't exist when it was expected
            // if tx doesn't exist then the tx.id -> pid mapping doesn't
            // exist either meaning any provided mapping is wrong
            co_return tx::errc::invalid_producer_id_mapping;
        }

        co_return latest_tx.error();
    }
    auto tx = std::move(latest_tx.value());
    vlog(
      txlog.trace,
      "[tx_id={}] initializing transaction {} for add resources",
      transactional_id,
      tx,
      pid);
    /**
     * Adding resources to transaction is the request that producer is sending
     * to the broker when a transaction is started or already ongoing. In the
     * case when it is a new transaction increment the transactional sequence
     * number.
     */
    if (tx.is_finished()) {
        auto reset_result = stm->reset_transaction_state(tx);
        if (!reset_result) {
            vlog(
              txlog.warn,
              "[tx_id={}] unable to reset transaction {} state",
              transactional_id,
              tx);
        }
        tx = std::move(reset_result.value());
    }

    // validate if transaction is in valid state and we can proceed
    if (!is_state_transition_valid(tx, tx_status::ongoing)) {
        vlog(
          txlog.warn,
          "[tx_id={}] transaction {} is invalid state, can not "
          "proceed with adding resources to transaction",
          transactional_id,
          tx);
        co_return tx::errc::invalid_txn_state;
    }

    co_return std::move(tx);
}

ss::future<add_partitions_tx_reply> tx_gateway_frontend::do_add_partition_to_tx(
  ss::shared_ptr<tm_stm> stm,
  add_partitions_tx_request request,
  model::timeout_clock::duration timeout) {
    model::producer_identity pid{request.producer_id, request.producer_epoch};
    auto sync_result = co_await sync_stm(stm);

    if (!sync_result.has_value()) {
        co_return make_add_partitions_error_response(
          request, sync_result.error());
    }
    auto term = sync_result.value();
    auto r = co_await init_add_resource_to_tx(
      request.transactional_id, term, pid, stm, timeout);
    if (r.has_error()) {
        co_return make_add_partitions_error_response(request, r.error());
    }
    auto tx = std::move(r.value());

    /**
     * First validate if we can add partitions to this transaction
     */
    if (!is_state_transition_valid(tx, tx_status::ongoing)) {
        vlog(
          txlog.warn,
          "unable to add partitions tp transaction in {} state",
          tx.status);
        co_return make_add_partitions_error_response(
          request, tx::errc::invalid_txn_state);
    }
    add_partitions_tx_reply response;
    std::vector<model::ntp> new_partitions;

    for (auto& req_topic : request.topics) {
        add_partitions_tx_reply::topic_result res_topic;
        res_topic.name = req_topic.name;

        model::topic topic(req_topic.name);

        const auto* disabled_set
          = _metadata_cache.local().get_topic_disabled_set(
            model::topic_namespace_view{model::kafka_namespace, topic});

        res_topic.results.reserve(req_topic.partitions.size());
        for (model::partition_id req_partition : req_topic.partitions) {
            model::ntp ntp(model::kafka_namespace, topic, req_partition);
            auto has_ntp = std::any_of(
              tx.partitions.begin(),
              tx.partitions.end(),
              [ntp](const auto& rm) { return rm.ntp == ntp; });
            if (has_ntp) {
                add_partitions_tx_reply::partition_result res_partition;
                res_partition.partition_index = req_partition;
                res_partition.error_code = tx::errc::none;
                res_topic.results.push_back(res_partition);
            } else if (
              disabled_set && disabled_set->is_disabled(req_partition)) {
                add_partitions_tx_reply::partition_result res_partition;
                res_partition.partition_index = req_partition;
                res_partition.error_code = tx::errc::partition_disabled;
                res_topic.results.push_back(res_partition);
            } else {
                new_partitions.push_back(ntp);
            }
        }
        response.results.push_back(res_topic);
    }

    std::vector<tx_metadata::tx_partition> partitions;
    std::vector<begin_tx_reply> data_partition_begin_replies;
    auto retries = _metadata_dissemination_retries;
    auto delay_ms = _metadata_dissemination_retry_delay_ms;

    while (0 < retries--) {
        partitions.clear();
        data_partition_begin_replies.clear();
        bool should_retry = false;
        bool should_abort = false;
        std::vector<ss::future<begin_tx_reply>> bfs;
        bfs.reserve(new_partitions.size());
        for (auto& ntp : new_partitions) {
            bfs.push_back(_rm_partition_frontend.local().begin_tx(
              ntp,
              tx.pid,
              tx.tx_seq,
              tx.timeout_ms,
              timeout,
              stm->get_partition()));
        }
        data_partition_begin_replies = co_await when_all_succeed(
          bfs.begin(), bfs.end());
        for (auto& br : data_partition_begin_replies) {
            auto topic_it = std::find_if(
              response.results.begin(),
              response.results.end(),
              [&br](const auto& r) { return r.name == br.ntp.tp.topic(); });
            vassert(
              topic_it != response.results.end(),
              "can't find expected topic {}",
              br.ntp.tp.topic());
            vassert(
              std::none_of(
                topic_it->results.begin(),
                topic_it->results.end(),
                [&br](const auto& r) {
                    return r.partition_index == br.ntp.tp.partition();
                }),
              "partition {} is already part of the response",
              br.ntp.tp.partition());

            bool expected_ec = br.ec == tx::errc::leader_not_found
                               || br.ec == tx::errc::shard_not_found
                               || br.ec == tx::errc::stale
                               || br.ec == tx::errc::timeout
                               || br.ec == tx::errc::partition_not_exists
                               || br.ec == tx::errc::producer_creation_error;
            should_abort = should_abort
                           || (br.ec != tx::errc::none && !expected_ec);
            should_retry = should_retry || expected_ec;

            if (br.ec == tx::errc::none) {
                partitions.push_back(
                  tx_metadata::tx_partition{
                    .ntp = br.ntp,
                    .etag = br.etag,
                    .topic_revision = br.topic_revision});
            }
        }
        if (should_abort) {
            break;
        }
        if (should_retry) {
            if (!co_await sleep_abortable(delay_ms, _as)) {
                break;
            }
            continue;
        }
        break;
    }

    auto status = co_await stm->add_partitions(
      term, tx.id, tx.tx_seq, partitions);
    /**
     * If we failed to update the transaction state return error, client will
     * retry if needed and advance transaction state as data/group partition
     * operations are idempotent and can easily be retried.
     */
    if (status != tm_stm::op_status::success) {
        vlog(
          txlog.warn,
          "[tx_id={}] adding partitions failed pid: {} - {}",
          request.transactional_id,
          pid,
          status);
        co_return make_add_partitions_error_response(
          request, map_state_update_outcome(status));
    }

    for (auto& reply : data_partition_begin_replies) {
        auto topic_it = std::find_if(
          response.results.begin(),
          response.results.end(),
          [&reply](const auto& r) { return r.name == reply.ntp.tp.topic(); });

        add_partitions_tx_reply::partition_result res_partition;
        res_partition.partition_index = reply.ntp.tp.partition;
        res_partition.error_code = reply.ec;

        auto level_for = [](tx::errc ec) {
            if (ec == tx::errc::none) {
                return ss::log_level::trace;
            }
            if (ec == tx::errc::partition_writes_locked) {
                return ss::log_level::warn;
            }
            return ss::log_level::error;
        };
        vlogl(
          txlog,
          level_for(reply.ec),
          "[tx_id={}] begin_tx request for pid: {} at ntp: {} result: {}",
          request.transactional_id,
          pid,
          reply.ntp,
          reply.ec);

        topic_it->results.push_back(res_partition);
    }
    co_return response;
}

ss::future<add_offsets_tx_reply> tx_gateway_frontend::add_offsets_to_tx(
  add_offsets_tx_request request, model::timeout_clock::duration timeout) {
    vlog(
      txlog.trace,
      "[tx_id={}] adding offsets to tx, group_id: {}, producer id: "
      "{}, producer epoch: {}",
      request.transactional_id,
      request.group_id,
      request.producer_id,
      request.producer_epoch);

    auto tx_ntp_opt = ntp_for_tx_id(request.transactional_id);
    if (!tx_ntp_opt) {
        vlog(
          txlog.warn,
          "[tx_id={}] unable to find coordinator ntp, producer id: {}, "
          "producer epoch: {}",
          request.transactional_id,
          request.producer_id,
          request.producer_epoch);
        co_return add_offsets_tx_reply{
          .error_code = tx::errc::coordinator_not_available};
    }
    auto tx_ntp = std::move(tx_ntp_opt.value());
    auto leader = co_await wait_for_leader(tx_ntp);
    if (leader != _self) {
        vlog(
          txlog.trace,
          "[tx_id={}] current node is not a leader for {}, current leader: {}",
          request.transactional_id,
          tx_ntp,
          leader);
        co_return add_offsets_tx_reply{.error_code = tx::errc::not_coordinator};
    }

    auto shard = _shard_table.local().shard_for(tx_ntp);

    if (shard == std::nullopt) {
        vlog(
          txlog.warn,
          "[tx_id={}] can't find a shard for {} producer id: {}, "
          "producer epoch: {}",
          request.transactional_id,
          tx_ntp,
          request.producer_id,
          request.producer_epoch);
        co_return add_offsets_tx_reply{
          .error_code = tx::errc::coordinator_not_available};
    }

    co_return co_await container().invoke_on(
      *shard,
      _ssg,
      [request = std::move(request), timeout, tm = tx_ntp.tp.partition](
        tx_gateway_frontend& self) mutable -> ss::future<add_offsets_tx_reply> {
          return ss::with_gate(
            self._gate,
            [request = std::move(request), timeout, tm, &self]() mutable
              -> ss::future<add_offsets_tx_reply> {
                return self.with_stm(
                  tm,
                  [request = std::move(request), timeout, &self](
                    checked<ss::shared_ptr<tm_stm>, tx::errc> r) mutable {
                      if (!r) {
                          return ss::make_ready_future<add_offsets_tx_reply>(
                            add_offsets_tx_reply{.error_code = r.error()});
                      }
                      auto stm = r.value();
                      return stm->read_lock().then(
                        [&self, stm, request = std::move(request), timeout](
                          ss::basic_rwlock<>::holder unit) mutable {
                            auto tx_id = request.transactional_id;
                            return with(
                                     stm,
                                     tx_id,
                                     "add_offsets_to_tx",
                                     [&self,
                                      stm,
                                      request = std::move(request),
                                      timeout]() mutable {
                                         return self.do_add_offsets_to_tx(
                                           stm, std::move(request), timeout);
                                     })
                              .finally([u = std::move(unit)] {});
                        });
                  });
            });
      });
}

ss::future<add_offsets_tx_reply> tx_gateway_frontend::do_add_offsets_to_tx(
  ss::shared_ptr<tm_stm> stm,
  add_offsets_tx_request request,
  model::timeout_clock::duration timeout) {
    model::producer_identity pid{request.producer_id, request.producer_epoch};

    auto sync_result = co_await sync_stm(stm);
    if (!sync_result.has_value()) {
        co_return add_offsets_tx_reply{.error_code = sync_result.error()};
    }
    auto term = sync_result.value();

    auto r = co_await init_add_resource_to_tx(
      request.transactional_id, term, pid, stm, timeout);
    if (r.has_error()) {
        co_return add_offsets_tx_reply{.error_code = r.error()};
    }
    auto tx = std::move(r.value());

    auto group_info = co_await _rm_group_proxy->begin_group_tx(
      request.group_id, pid, tx.tx_seq, tx.timeout_ms, stm->get_partition());
    if (group_info.ec != tx::errc::none) {
        vlog(
          txlog.warn,
          "[tx_id={}] error starting group transaction for pid: {}, group: {} "
          "- {}",
          request.transactional_id,
          pid,
          request.group_id,
          group_info.ec);

        co_return add_offsets_tx_reply{.error_code = group_info.ec};
    }

    auto status = co_await stm->add_group(
      term, tx.id, tx.tx_seq, request.group_id, group_info.etag);
    auto has_added = status == tm_stm::op_status::success;
    if (!has_added) {
        vlog(
          txlog.warn,
          "[tx_id={}] error adding group to tm_stm for pid: {} group: {}",
          request.transactional_id,
          pid,
          request.group_id);
        co_return add_offsets_tx_reply{
          .error_code = tx::errc::invalid_txn_state};
    }
    co_return add_offsets_tx_reply{.error_code = tx::errc::none};
}

ss::future<end_tx_reply> tx_gateway_frontend::end_txn(
  end_tx_request request, model::timeout_clock::duration timeout) {
    vlog(
      txlog.trace,
      "[tx_id={}] end transaction. producer id: {}, producer epoch: {}, "
      "committed: {}",
      request.transactional_id,
      request.producer_id,
      request.producer_epoch,
      request.committed);

    auto tx_ntp_opt = ntp_for_tx_id(request.transactional_id);
    if (!tx_ntp_opt) {
        vlog(
          txlog.trace,
          "[tx_id={}] can not find coordinator ntp, producer id: {}, producer "
          "epoch: {}",
          request.transactional_id,
          request.producer_id,
          request.producer_epoch);
        co_return end_tx_reply{
          .error_code = tx::errc::coordinator_not_available};
    }
    auto tx_ntp = std::move(tx_ntp_opt.value());

    auto leader = co_await wait_for_leader(tx_ntp);
    if (leader != _self) {
        vlog(
          txlog.trace,
          "[tx_id={}] current node is not a leader for {}, current leader: {}",
          request.transactional_id,
          tx_ntp,
          leader);
        co_return end_tx_reply{.error_code = tx::errc::not_coordinator};
    }
    auto shard = _shard_table.local().shard_for(tx_ntp);

    if (shard == std::nullopt) {
        vlog(
          txlog.warn,
          "[tx_id={}] can't find a shard for {}, producer id: {}, producer "
          "epoch: {}",
          request.transactional_id,
          tx_ntp,
          request.producer_id,
          request.producer_epoch);
        co_return end_tx_reply{
          .error_code = tx::errc::coordinator_not_available};
    }

    co_return co_await container().invoke_on(
      *shard,
      _ssg,
      [request = std::move(request), timeout, tm = tx_ntp.tp.partition](
        tx_gateway_frontend& self) mutable -> ss::future<end_tx_reply> {
          return ss::with_gate(
            self._gate,
            [request = std::move(request), timeout, tm, &self]() mutable
              -> ss::future<end_tx_reply> {
                return self.with_stm(
                  tm,
                  [request = std::move(request), timeout, &self](
                    checked<ss::shared_ptr<tm_stm>, tx::errc> r) mutable {
                      return self.do_end_txn(
                        std::move(r), std::move(request), timeout);
                  });
            });
      });
}

ss::future<end_tx_reply> tx_gateway_frontend::do_end_txn(
  checked<ss::shared_ptr<tm_stm>, tx::errc> r,
  end_tx_request request,
  model::timeout_clock::duration timeout) {
    if (!r) {
        model::producer_identity pid{
          request.producer_id, request.producer_epoch};
        vlog(
          txlog.warn,
          "[tx_id={}] error getting transaction from coordinator pid: {} - {}",
          request.transactional_id,
          pid,
          r.error());
        return ss::make_ready_future<end_tx_reply>(
          end_tx_reply{.error_code = r.error()});
    }
    auto stm = r.value();
    auto outcome = ss::make_lw_shared<available_promise<tx::errc>>();
    // commit_tm_tx and abort_tm_tx remove transient data during its
    // execution. however the outcome of the commit/abort operation
    // is already known before the cleanup started. to optimize this
    // they return the outcome promise to return the outcome before
    // cleaning up and before returing the actual control flow
    auto decided = outcome->get_future();

    // re-entering the gate to keep its open until the spawned fiber
    // is active
    if (stm->gate().is_closed()) {
        return ss::make_ready_future<end_tx_reply>(
          end_tx_reply{.error_code = tx::errc::coordinator_not_available});
    }

    auto h = stm->gate().hold();

    ssx::spawn_with_gate(
      _gate,
      [request = std::move(request),
       this,
       stm,
       timeout,
       outcome,
       h = std::move(h)]() mutable {
          return stm->read_lock()
            .then([request = std::move(request),
                   this,
                   stm,
                   timeout,
                   outcome,
                   h = std::move(h)](ss::basic_rwlock<>::holder unit) mutable {
                auto tx_id = request.transactional_id;
                return with(
                         stm,
                         tx_id,
                         "end_txn",
                         [request = std::move(request),
                          this,
                          stm,
                          timeout,
                          outcome,
                          h = std::move(h)]() mutable {
                             model::producer_identity pid{
                               request.producer_id, request.producer_epoch};
                             auto tx_id = request.transactional_id;
                             return do_end_txn(
                                      std::move(request), stm, timeout, outcome)
                               .finally([outcome,
                                         stm,
                                         h = std::move(h),
                                         tx_id = std::move(tx_id),
                                         pid]() {
                                   if (!outcome->available()) {
                                       vlog(
                                         txlog.warn,
                                         "[tx_id={}] outcome for transaction "
                                         "is missing, pid: {}",
                                         tx_id,
                                         pid);
                                       outcome->set_value(
                                         tx::errc::unknown_server_error);
                                   }
                               });
                         })
                  .finally([u = std::move(unit)] {});
            })
            .discard_result();
      });

    return decided.then(
      [](tx::errc ec) { return end_tx_reply{.error_code = ec}; });
}

ss::future<tx_gateway_frontend::op_result_t> tx_gateway_frontend::do_end_txn(
  end_tx_request request,
  ss::shared_ptr<cluster::tm_stm> stm,
  model::timeout_clock::duration timeout,
  ss::lw_shared_ptr<available_promise<tx::errc>> outcome) {
    model::producer_identity pid{request.producer_id, request.producer_epoch};

    auto sync_result = co_await sync_stm(stm);

    if (!sync_result.has_value()) {
        vlog(
          txlog.warn,
          "[tx_id={}] sync on end_txn failed pid: {} - {}",
          request.transactional_id,
          pid,
          sync_result.error());
        outcome->set_value(sync_result.error());
        co_return sync_result.error();
    }
    auto term = sync_result.value();

    auto r0 = co_await get_latest_tx(
      term, stm, pid, request.transactional_id, timeout);
    if (!r0.has_value()) {
        auto err = r0.error();
        if (err == tx::errc::tx_not_found) {
            vlog(
              txlog.warn,
              "[tx_id={}] can't find an ongoing transaction pid: {} to "
              "commit/abort",
              request.transactional_id,
              pid);
            err = tx::errc::invalid_producer_id_mapping;
        }
        outcome->set_value(err);
        co_return err;
    }
    auto tx = std::move(r0.value());

    op_result_t r(tx::errc::unknown_server_error);
    if (request.committed) {
        co_return co_await handle_commit_tx(
          term, stm, std::move(tx), timeout, outcome);
    }
    co_return co_await handle_abort_tx(
      term, stm, std::move(tx), timeout, outcome);
}

ss::future<tx_gateway_frontend::op_result_t>
tx_gateway_frontend::handle_commit_tx(
  model::term_id term,
  ss::shared_ptr<cluster::tm_stm> stm,
  cluster::tx_metadata tx,
  model::timeout_clock::duration timeout,
  ss::lw_shared_ptr<available_promise<tx::errc>> outcome) {
    if (tx.status == tx_status::preparing_internal_abort) {
        vlog(
          txlog.warn,
          "[tx_id={}] can not commit an expired transaction: {} in term: {}",
          tx.id,
          tx,
          term);
        outcome->set_value(tx::errc::fenced);
        co_return tx::errc::fenced;
    }

    /**
     * Completed commit transaction do not require any further steps, simply
     * return success to make the end_txn request fully idempotent.
     */
    if (tx.status == tx_status::completed_commit) {
        vlog(
          txlog.warn,
          "[tx_id={}] transaction is {} already committed",
          tx.id,
          tx);
        if (!outcome->available()) {
            outcome->set_value(tx::errc::none);
        }
        co_return tx::errc::none;
    }

    if (is_state_transition_valid(tx, tx_status::preparing_commit)) {
        try {
            auto r = co_await do_commit_tm_tx(term, stm, tx, timeout, outcome);
            if (r.has_value()) {
                co_return r;
            }
            co_return r.error();
        } catch (...) {
            vlogl(
              txlog,
              ssx::is_shutdown_exception(std::current_exception())
                ? ss::log_level::debug
                : ss::log_level::error,
              "[tx_id={}] error committing transaction: {} - {}",
              tx.id,
              tx,
              std::current_exception());
            if (!outcome->available()) {
                outcome->set_value(tx::errc::unknown_server_error);
            }
            co_return tx::errc::unknown_server_error;
        }
    } else {
        vlog(
          txlog.warn,
          "[tx_id={}] can not commit transaction: {} in term: {}, invalid "
          "status",
          tx.id,
          tx,
          term);
        outcome->set_value(tx::errc::invalid_txn_state);
        co_return tx::errc::invalid_txn_state;
    }
}

ss::future<tx_gateway_frontend::op_result_t>
tx_gateway_frontend::handle_abort_tx(
  model::term_id term,
  ss::shared_ptr<cluster::tm_stm> stm,
  cluster::tx_metadata tx,
  model::timeout_clock::duration timeout,
  ss::lw_shared_ptr<available_promise<tx::errc>> outcome) {
    if (tx.status == tx_status::preparing_internal_abort) {
        vlog(
          txlog.warn,
          "[tx_id={}] can't abort an expired transaction: {} in term: {}",
          tx.id,
          tx,
          term);
        outcome->set_value(tx::errc::fenced);
        co_return tx::errc::fenced;
    }
    try {
        bool is_status_ok
          = is_state_transition_valid(tx, tx_status::preparing_abort)
            || is_state_transition_valid(tx, tx_status::completed_abort);
        if (is_status_ok) {
            auto r = co_await do_abort_tm_tx(term, stm, tx, timeout);
            if (r.has_value()) {
                outcome->set_value(tx::errc::none);
                co_return r.value();
            }
            vlog(
              txlog.warn,
              "[tx_id={}] error aborting transaction: {} - {}",
              tx.id,
              tx,
              r.error());
            outcome->set_value(r.error());
            co_return r.error();
        }
        outcome->set_value(tx::errc::invalid_txn_state);
        co_return tx::errc::invalid_txn_state;
    } catch (...) {
        auto ex = std::current_exception();
        auto log_level = ssx::is_shutdown_exception(ex) ? ss::log_level::debug
                                                        : ss::log_level::error;
        vlogl(
          txlog,
          log_level,
          "[tx_id={}] exception aborting transaction: {} - {}",
          tx.id,
          tx,
          ex);
        outcome->set_value(tx::errc::unknown_server_error);
        co_return tx::errc::unknown_server_error;
    }
}

ss::future<tx_metadata> tx_gateway_frontend::remove_deleted_partitions_from_tx(
  ss::shared_ptr<tm_stm> stm, model::term_id term, cluster::tx_metadata tx) {
    std::deque<tx_metadata::tx_partition> deleted_partitions;
    std::copy_if(
      tx.partitions.begin(),
      tx.partitions.end(),
      std::back_inserter(deleted_partitions),
      [this](const tx_metadata::tx_partition& part) {
          return part.topic_revision() >= 0
                 && _metadata_cache.local().get_topic_state(
                      model::topic_namespace_view(part.ntp),
                      part.topic_revision)
                      == topic_table::topic_state::not_exists;
      });

    for (auto& part : deleted_partitions) {
        auto result = co_await stm->delete_partition_from_tx(term, tx.id, part);
        if (result) {
            vlog(
              txlog.info,
              "[tx_id={}] Deleted non existent partition {} from transaction",
              tx.id,
              part.ntp);
            tx = result.value();
        } else {
            vlog(
              txlog.debug,
              "[tx_id={}] Error deleting partition {} from transaction - {}",
              tx.id,
              part.ntp,
              result.error());
            break;
        }
    }
    co_return tx;
}

ss::future<tx_gateway_frontend::op_result_t>
tx_gateway_frontend::do_abort_tm_tx(
  model::term_id expected_term,
  ss::shared_ptr<cluster::tm_stm> stm,
  cluster::tx_metadata tx,
  model::timeout_clock::duration timeout) {
    if (!stm->is_actual_term(expected_term)) {
        vlog(
          txlog.trace,
          "[tx_id={}] txn coordinator isn't synced with term: {} pid: {} etag: "
          "{} tx_seq: {}",
          tx.id,
          expected_term,
          tx.pid,
          tx.etag,
          tx.tx_seq);
        co_return tx::errc::not_coordinator;
    }

    if (tx.status == tx_status::ongoing || tx.status == tx_status::empty) {
        auto update_result = co_await stm->update_transaction_status(
          expected_term, tx.id, tx_status::preparing_abort);
        if (update_result.has_error()) {
            co_return map_state_update_outcome(update_result.error());
        }
    }

    auto abort_result = co_await abort_data(stm, expected_term, tx, timeout);
    if (abort_result.has_error()) {
        co_return abort_result.error();
    }

    auto update_result = co_await stm->finish_transaction(
      expected_term, tx.id, tx_status::completed_abort);
    if (update_result.has_error()) {
        co_return map_state_update_outcome(update_result.error());
    }
    co_return std::move(update_result.value());
}

ss::future<tx_gateway_frontend::op_result_t>
tx_gateway_frontend::do_commit_tm_tx(
  model::term_id expected_term,
  ss::shared_ptr<cluster::tm_stm> stm,
  cluster::tx_metadata tx,
  model::timeout_clock::duration timeout,
  ss::lw_shared_ptr<available_promise<tx::errc>> outcome) {
    try {
        if (!stm->is_actual_term(expected_term)) {
            outcome->set_value(tx::errc::not_coordinator);
            co_return tx::errc::not_coordinator;
        }

        if (tx.status == tx_status::ongoing || tx.status == tx_status::empty) {
            auto update_result = co_await stm->update_transaction_status(
              expected_term, tx.id, tx_status::preparing_commit);
            if (!update_result) {
                auto err = map_state_update_outcome(update_result.error());
                outcome->set_value(err);
                co_return err;
            }
        }
        // release control to the client before committing transaction on data
        // partitions & groups as the outcome has already been decided.
        outcome->set_value(tx::errc::none);
    } catch (...) {
        vlog(
          txlog.warn,
          "[tx_id={}] error committing transaction - {}",
          tx.id,
          std::current_exception());
        outcome->set_value(tx::errc::unknown_server_error);
        co_return tx::errc::unknown_server_error;
    }

    auto commit_result = co_await commit_data(stm, expected_term, tx, timeout);
    if (commit_result.has_error()) {
        co_return commit_result.error();
    }
    vlog(
      txlog.trace,
      "[tx_id={}] marking transaction {} as commit completed in term: {}",
      tx.id,
      tx,
      expected_term);
    auto committed_tx = co_await stm->finish_transaction(
      expected_term, tx.id, tx_status::completed_commit);
    if (!committed_tx.has_value()) {
        vlog(
          txlog.trace,
          "[tx_id={}] error committing transaction: {} - {}",
          tx.id,
          tx,
          committed_tx.error());
        auto err = map_state_update_outcome(committed_tx.error());

        co_return err;
    }

    co_return std::move(committed_tx.value());
}

/**
 * Make sure we progress the transaction here
 */
ss::future<tx_gateway_frontend::op_result_t>
tx_gateway_frontend::maybe_progress_transaction(
  model::term_id expected_term,
  ss::shared_ptr<cluster::tm_stm> stm,
  cluster::tx_metadata tx,
  model::timeout_clock::duration timeout) {
    if (!stm->is_actual_term(expected_term)) {
        co_return tx::errc::not_coordinator;
    }

    if (!need_to_advance_progress(tx)) {
        co_return std::move(tx);
    }
    vlog(
      txlog.trace,
      "[tx_id={}] trying to progress transaction {} in term: {}",
      tx.id,
      tx,
      expected_term);
    tx_gateway_frontend::op_result_t op_r(tx::errc::unknown_server_error);
    if (tx.status == tx_status::preparing_commit) {
        op_r = co_await commit_data(stm, expected_term, tx, timeout);
    } else {
        op_r = co_await abort_data(stm, expected_term, tx, timeout);
    }

    if (op_r.has_error()) {
        co_return op_r;
    }
    auto final_status = tx.status == tx_status::preparing_commit
                          ? tx_status::completed_commit
                          : tx_status::completed_abort;

    /**
     * If the transaction was aborted internally i.e. expired an epoch bump is
     * required to force producer to initialize a new session. After the epoch
     * bump the producer will be fenced as the coordinator validates the
     * producer epoch on every transaction state update.
     */
    const bool needs_epoch_bump = tx.status
                                  == tx_status::preparing_internal_abort;
    auto finish_result = co_await stm->finish_transaction(
      expected_term, tx.id, final_status, needs_epoch_bump);

    if (!finish_result.has_value()) {
        vlog(
          txlog.trace,
          "[tx_id={}] error finishing transaction: {} - {}",
          tx.id,
          tx,
          finish_result.error());
        co_return map_state_update_outcome(finish_result.error());
    }

    co_return std::move(finish_result.value());
}

ss::future<tx_gateway_frontend::op_result_t> tx_gateway_frontend::commit_data(
  ss::shared_ptr<tm_stm> stm,
  model::term_id expected_term,
  tx_metadata tx,
  model::timeout_clock::duration timeout) {
    auto retries = _metadata_dissemination_retries;
    auto delay_ms = _metadata_dissemination_retry_delay_ms;
    auto done = false;
    vlog(txlog.trace, "[tx_id={}] committing transaction: {} data", tx.id, tx);
    while (0 < retries--) {
        std::vector<ss::future<commit_group_tx_reply>> gfs;
        gfs.reserve(tx.groups.size());
        for (const auto& group : tx.groups) {
            gfs.push_back(_rm_group_proxy->commit_group_tx(
              group.group_id, tx.pid, tx.tx_seq, timeout));
        }
        std::vector<ss::future<commit_tx_reply>> cfs;
        cfs.reserve(tx.partitions.size());
        for (const auto& rm : tx.partitions) {
            cfs.push_back(_rm_partition_frontend.local().commit_tx(
              rm.ntp, tx.pid, tx.tx_seq, timeout));
        }
        auto ok = true;
        auto failed = false;
        auto rejected = false;
        auto grs = co_await when_all_succeed(gfs.begin(), gfs.end());
        for (const auto& r : grs) {
            if (r.ec == tx::errc::request_rejected) {
                rejected = true;
                vlog(
                  txlog.warn,
                  "[tx_id={}] commit_tx on consumer groups etag: {} pid: {} "
                  "tx_seq: {} "
                  "status: {} in term: {} was rejected",
                  tx.id,
                  tx.etag,
                  tx.pid,
                  tx.tx_seq,
                  tx.status,
                  expected_term);
            } else if (r.ec != tx::errc::none) {
                failed = true;
                vlog(
                  txlog.trace,
                  "[tx_id={}] commit_tx on consumer groups etag: {} pid: {} "
                  "tx_seq: {} status:  {} in term: {} failed with {}",
                  tx.id,
                  tx.etag,
                  tx.pid,
                  tx.tx_seq,
                  tx.status,
                  expected_term,
                  r.ec);
            }
            ok = ok && (r.ec == tx::errc::none);
        }
        auto crs = co_await when_all_succeed(cfs.begin(), cfs.end());
        for (const auto& r : crs) {
            if (r.ec == tx::errc::request_rejected) {
                rejected = true;
                vlog(
                  txlog.warn,
                  "[tx_id={}] commit_tx on data partition etag: {} pid: {} "
                  "tx_seq: {} status: {} in term: {} was rejected",
                  tx.id,
                  tx.etag,
                  tx.pid,
                  tx.tx_seq,
                  tx.status,
                  expected_term);
            } else if (r.ec != tx::errc::none) {
                failed = true;
                vlog(
                  txlog.trace,
                  "[tx_id={}] commit_tx on data partition etag: {} pid: {} "
                  "tx_seq: {} status: {} in term: {} failed with {}",
                  tx.id,
                  tx.etag,
                  tx.pid,
                  tx.tx_seq,
                  tx.status,
                  expected_term,
                  r.ec);
            }
            ok = ok && (r.ec == tx::errc::none);
        }
        if (ok) {
            done = true;
            break;
        }
        if (rejected && !failed) {
            // per partition commits either passed or was rejected;
            // no need to try deleting partition because we have a
            // positive confirmation it exists
            // request_rejected means *I have state* indicating this
            // request won't be ever processed
            vlog(
              txlog.warn,
              "[tx_id={}] remote commit etag: {} pid: {} tx_seq: {} in term: "
              "{} rejected",
              tx.id,
              tx.etag,
              tx.pid,
              tx.tx_seq,
              expected_term);
            co_return tx::errc::request_rejected;
        }

        tx = co_await remove_deleted_partitions_from_tx(stm, expected_term, tx);
        if (co_await sleep_abortable(delay_ms, _as)) {
            vlog(
              txlog.trace,
              "[tx_id={}] retrying re-commit etag: {} pid: {} tx_seq: {}",
              tx.id,
              tx.etag,
              tx.pid,
              tx.tx_seq);
        } else {
            break;
        }
    }
    if (!done) {
        vlog(
          txlog.warn,
          "[tx_id={}] remote commit etag: {} pid: {} tx_seq: {} in term: {} "
          "failed",
          tx.id,
          tx.etag,
          tx.pid,
          tx.tx_seq,
          expected_term);
        co_return tx::errc::timeout;
    }
    co_return tx;
}

ss::future<tx_gateway_frontend::op_result_t> tx_gateway_frontend::abort_data(
  ss::shared_ptr<tm_stm> stm,
  model::term_id expected_term,
  tx_metadata tx,
  model::timeout_clock::duration timeout) {
    auto retries = _metadata_dissemination_retries;
    auto delay_ms = _metadata_dissemination_retry_delay_ms;
    auto done = false;
    vlog(txlog.trace, "[tx_id={}] aborting transaction: {} data", tx.id, tx);
    while (0 < retries--) {
        std::vector<ss::future<abort_tx_reply>> pfs;
        pfs.reserve(tx.partitions.size());
        for (const auto& rm : tx.partitions) {
            pfs.push_back(_rm_partition_frontend.local().abort_tx(
              rm.ntp, tx.pid, tx.tx_seq, timeout));
        }
        std::vector<ss::future<abort_group_tx_reply>> gfs;
        gfs.reserve(tx.groups.size());
        for (const auto& group : tx.groups) {
            gfs.push_back(_rm_group_proxy->abort_group_tx(
              group.group_id, tx.pid, tx.tx_seq, timeout));
        }
        auto prs = co_await when_all_succeed(pfs.begin(), pfs.end());
        auto grs = co_await when_all_succeed(gfs.begin(), gfs.end());
        auto ok = true;
        auto failed = false;
        auto rejected = false;
        for (const auto& r : prs) {
            if (r.ec == tx::errc::request_rejected) {
                rejected = true;
                vlog(
                  txlog.warn,
                  "[tx_id={}] abort_tx on data partition etag: {} pid: {} "
                  "tx_seq: {} status: {} in term: {} was rejected",
                  tx.id,
                  tx.etag,
                  tx.pid,
                  tx.tx_seq,
                  tx.status,
                  expected_term);
            } else if (r.ec != tx::errc::none) {
                failed = true;
                vlog(
                  txlog.info,
                  "[tx_id={}] abort_tx on data partition etag: {} pid: {} "
                  "tx_seq: {} status: {} in term: {} failed with {}",
                  tx.id,
                  tx.etag,
                  tx.pid,
                  tx.tx_seq,
                  tx.status,
                  expected_term,
                  r.ec);
            }
            ok = ok && (r.ec == tx::errc::none);
        }
        for (const auto& r : grs) {
            if (r.ec == tx::errc::request_rejected) {
                rejected = true;
                vlog(
                  txlog.warn,
                  "[tx_id={}] abort_tx on consumer groups etag: {} pid: {} "
                  "tx_seq: {} status: {} in term: {} was rejected",
                  tx.id,
                  tx.etag,
                  tx.pid,
                  tx.tx_seq,
                  tx.status,
                  expected_term);
            } else if (r.ec != tx::errc::none) {
                failed = true;
                vlog(
                  txlog.trace,
                  "[tx_id={}]  abort_tx on consumer groups etag: {} pid: {} "
                  "tx_seq: {} status: {} in term: {} failed with {}",
                  tx.id,
                  tx.etag,
                  tx.pid,
                  tx.tx_seq,
                  tx.status,
                  expected_term,
                  r.ec);
            }
            ok = ok && (r.ec == tx::errc::none);
        }
        if (ok) {
            done = true;
            break;
        }
        if (rejected && !failed) {
            vlog(
              txlog.warn,
              "[tx_id={}] remote abort etag: {} pid: {} tx_seq: {} in term: {} "
              "was rejected",
              tx.id,
              tx.etag,
              tx.pid,
              tx.tx_seq,
              expected_term);
            co_return tx::errc::request_rejected;
        }
        tx = co_await remove_deleted_partitions_from_tx(stm, expected_term, tx);
        if (!co_await sleep_abortable(delay_ms, _as)) {
            break;
        }
    }
    if (!done) {
        vlog(
          txlog.warn,
          "[tx_id={}] remote abort etag: {} pid: {} tx_seq: {} in term: {} "
          "failed",
          tx.id,
          tx.etag,
          tx.pid,
          tx.tx_seq,
          expected_term);
        co_return tx::errc::timeout;
    }
    co_return tx;
}

ss::future<tx_gateway_frontend::op_result_t> tx_gateway_frontend::forget_tx(
  model::term_id term,
  ss::shared_ptr<cluster::tm_stm> stm,
  cluster::tx_metadata tx,
  model::timeout_clock::duration timeout) {
    op_result_t r1(tx::errc::unknown_server_error);
    if (tx.status == tx_status::completed_commit) {
        r1 = co_await commit_data(stm, term, tx, timeout);
    } else if (tx.status == tx_status::preparing_abort) {
        r1 = co_await abort_data(stm, term, tx, timeout);
    } else {
        r1 = tx;
    }

    // rolling forward is best effort it's ok to ignore if it can't
    // happen; the reason by it's rejected (write from the future)
    // will be aborted on its own via try_abort
    if (!r1.has_value() && r1.error() != tx::errc::request_rejected) {
        vlog(
          txlog.warn,
          "[tx_id={}] error rolling previous id with status: {} - {}",
          tx.id,
          tx.status,
          r1.error());

        // until any decision is made it's ok to ask user retry
        co_return tx::errc::not_coordinator;
    }

    auto ec = co_await stm->expire_tx(term, tx.id);
    if (ec != tm_stm::op_status::success) {
        vlog(
          txlog.warn, "[tx_id={}] error expiring transaction - {}", tx.id, ec);
        co_return tx::errc::not_coordinator;
    }

    // just wrote a tombstone
    co_return tx::errc::tx_not_found;
}

ss::future<tx_gateway_frontend::op_result_t>
tx_gateway_frontend::find_and_try_progressing_transaction(
  model::term_id term,
  ss::shared_ptr<tm_stm> stm,
  kafka::transactional_id tid,
  model::timeout_clock::duration timeout) {
    auto tx_opt = co_await stm->get_tx(tid);
    if (!tx_opt.has_value()) {
        if (tx_opt.error() == tm_stm::op_status::not_found) {
            co_return tx::errc::tx_not_found;
        }
        vlog(
          txlog.warn,
          "[tx_id={}] error getting transaction - {}",
          tid,
          tx_opt.error());

        co_return tx::errc::not_coordinator;
    }

    auto tx = std::move(tx_opt.value());

    if (tx.etag > term) {
        // tx was written by a future leader meaning current
        // node can't be a leader
        co_return tx::errc::not_coordinator;
    }

    op_result_t progress_result = co_await maybe_progress_transaction(
      term, stm, tx, timeout);
    if (!progress_result) {
        vlog(
          txlog.warn,
          "[tx_id={}] error progressing transaction: {} - {}",
          tid,
          tx,
          progress_result.error());

        // until any decision is made it's ok to ask user retry
        co_return tx::errc::concurrent_transactions;
    }
    co_return progress_result.value();

    vlog(
      txlog.warn, "[tx_id={}] Transaction {} has unexpected status", tid, tx);

    co_return tx::errc::unknown_server_error;
}

ss::future<tx_gateway_frontend::op_result_t> tx_gateway_frontend::get_latest_tx(
  model::term_id term,
  ss::shared_ptr<tm_stm> stm,
  model::producer_identity pid,
  kafka::transactional_id tx_id,
  model::timeout_clock::duration timeout) {
    vlog(
      txlog.trace,
      "[tx_id={}] Getting latest tx for pid: {} in term: {}",
      tx_id,
      pid,
      term);
    auto tx_result = co_await find_and_try_progressing_transaction(
      term, stm, tx_id, timeout);
    if (!tx_result.has_value()) {
        co_return tx_result.error();
    }

    auto latest_tx = std::move(tx_result.value());
    if (latest_tx.pid == pid) {
        co_return latest_tx;
    }

    if (latest_tx.pid.id == pid.id && latest_tx.pid.epoch > pid.epoch) {
        vlog(
          txlog.info,
          "[tx_id={}] producer {} is fenced of by {}",
          tx_id,
          pid,
          latest_tx.pid);
        co_return tx::errc::fenced;
    }
    vlog(
      txlog.info,
      "[tx_id={}] transaction is mapped to {} not {}",
      tx_id,
      latest_tx.pid,
      pid);

    co_return tx::errc::invalid_producer_id_mapping;
}

void tx_gateway_frontend::expire_old_txs() {
    ssx::spawn_with_gate(_gate, [this] {
        auto ntp_meta = _metadata_cache.local().get_topic_metadata(
          model::tx_manager_nt);
        if (!ntp_meta) {
            vlog(
              txlog.debug,
              "Topic {} doesn't exist in metadata cache,",
              model::tx_manager_nt);
            return ss::now();
        }

        std::vector<model::partition_id> partitions;
        partitions.reserve(ntp_meta->get_assignments().size());
        for (auto& [_, pa] : ntp_meta->get_assignments()) {
            partitions.push_back(pa.id);
        }

        return ss::do_with(
          std::move(partitions),
          [this](const std::vector<model::partition_id>& ps) {
              return ss::do_for_each(ps, [this](model::partition_id pid) {
                  auto tx_ntp = model::ntp(
                    model::tx_manager_nt.ns, model::tx_manager_nt.tp, pid);
                  return expire_old_txs(tx_ntp).finally([this] {
                      // TODO: Create per shard timer
                      // https://github.com/redpanda-data/redpanda/issues/9606
                      // to consider: most likely it's ok to re-arm the timer
                      // only once out of the do_for_each
                      rearm_expire_timer();
                  });
              });
          });
    });
}

ss::future<> tx_gateway_frontend::expire_old_txs(const model::ntp& tx_ntp) {
    auto shard = _shard_table.local().shard_for(tx_ntp);

    if (!shard) {
        return ss::now();
    }

    return container().invoke_on(
      *shard,
      _ssg,
      [tm = tx_ntp.tp.partition](
        tx_gateway_frontend& self) -> ss::future<void> {
          return ss::with_gate(self._gate, [tm, &self]() -> ss::future<void> {
              return self.with_stm(
                tm, [&self](checked<ss::shared_ptr<tm_stm>, tx::errc> r) {
                    if (!r) {
                        return ss::now();
                    }
                    auto stm = r.value();
                    return stm->read_lock().then(
                      [&self, stm](ss::basic_rwlock<>::holder unit) {
                          return self.expire_old_txs(stm).finally(
                            [u = std::move(unit)] {});
                      });
                });
          });
      });
}

ss::future<> tx_gateway_frontend::expire_old_txs(ss::shared_ptr<tm_stm> stm) {
    auto tx_ids = stm->get_expired_txs();
    for (const auto& tx_id : tx_ids) {
        co_await expire_old_tx(stm, tx_id);
    }
}

ss::future<> tx_gateway_frontend::expire_old_tx(
  ss::shared_ptr<tm_stm> stm, kafka::transactional_id tx_id) {
    auto units = co_await stm->lock_tx(tx_id, "expire_old_tx");

    auto sync_result = co_await sync_stm(stm);
    if (!sync_result.has_value()) {
        vlog(
          txlog.debug,
          "[tx_id={}] error syncing state machine - {}",
          tx_id,
          sync_result.error());
        co_return;
    }

    auto term = sync_result.value();
    auto timeout = config::shard_local_cfg().internal_rpc_request_timeout_ms();

    auto tx_maybe = co_await find_and_try_progressing_transaction(
      term, stm, tx_id, timeout);
    if (!tx_maybe.has_value()) {
        co_return;
    }
    auto tx = tx_maybe.value();

    co_await do_expire_old_tx(stm, term, tx, timeout, false);
}

ss::future<tx::errc> tx_gateway_frontend::do_expire_old_tx(
  ss::shared_ptr<tm_stm> stm,
  model::term_id term,
  tx_metadata tx,
  model::timeout_clock::duration timeout,
  bool ignore_update_ts) {
    if (!ignore_update_ts && !stm->is_expired(tx)) {
        co_return tx::errc::none;
    }

    op_result_t r(tx);

    vlog(
      txlog.trace,
      "[tx_id={}] attempting to expire transaction pid: {} tx_seq: {} status: "
      "{}",
      tx.id,
      tx.pid,
      tx.tx_seq,
      tx.status);

    if (tx.status == tx_status::ongoing || tx.status == tx_status::empty) {
        r = co_await do_abort_tm_tx(term, stm, tx, timeout);
    }
    if (!r.has_value()) {
        vlog(
          txlog.warn,
          "[tx_id={}] error aborting transaction - {}",
          tx.id,
          r.error());
        co_return r.error();
    }

    // it's ok not to check ec because if the expiration isn't passed
    // it will be retried and it's an idempotent operation
    auto ec = co_await stm->expire_tx(term, tx.id);
    if (ec != tm_stm::op_status::success) {
        vlog(
          txlog.warn, "[tx_id={}] error expiring transaction - {}", tx.id, ec);
        co_return tx::errc::not_coordinator;
    }

    co_return tx::errc::none;
}

ss::future<tx_gateway_frontend::return_all_txs_res>
tx_gateway_frontend::get_all_transactions_for_one_tx_partition(
  model::ntp tx_manager_ntp) {
    auto shard = _shard_table.local().shard_for(tx_manager_ntp);

    if (!shard.has_value()) {
        vlog(txlog.warn, "can't find a shard for {}", tx_manager_ntp);
        co_return tx::errc::shard_not_found;
    }

    co_return co_await container().invoke_on(
      *shard,
      _ssg,
      [tx_partition = tx_manager_ntp.tp.partition](tx_gateway_frontend& self)
        -> ss::future<tx_gateway_frontend::return_all_txs_res> {
          auto gate_lock = self._gate.hold();
          return self
            .with_stm(
              tx_partition,
              [](checked<ss::shared_ptr<tm_stm>, tx::errc> r) {
                  if (!r) {
                      return ssx::now(return_all_txs_res{r.error()});
                  }
                  auto stm = r.value();
                  return stm->read_lock().then([stm](
                                                 ss::basic_rwlock<>::holder
                                                   unit) {
                      return stm->get_all_transactions()
                        .then(
                          [](tm_stm::get_txs_result res)
                            -> ss::future<return_all_txs_res> {
                              if (!res.has_value()) {
                                  if (
                                    res.error()
                                    == tm_stm::op_status::not_leader) {
                                      return ss::make_ready_future<
                                        return_all_txs_res>(return_all_txs_res{
                                        tx::errc::not_coordinator});
                                  }
                                  return ss::make_ready_future<
                                    return_all_txs_res>(return_all_txs_res{
                                    tx::errc::unknown_server_error});
                              }
                              return ss::make_ready_future<return_all_txs_res>(
                                std::move(res).value());
                          })
                        .finally([u = std::move(unit)] {});
                  });
              })
            .finally([l = std::move(gate_lock)] {});
      });
}

ss::future<tx_gateway_frontend::return_all_txs_res>
tx_gateway_frontend::get_all_transactions() {
    auto ntp_meta = _metadata_cache.local().get_topic_metadata(
      model::tx_manager_nt);
    if (!ntp_meta) {
        auto ec = co_await _tx_topic_manager.invoke_on(
          cluster::tx_topic_manager::shard, [](tx_topic_manager& mgr) {
              return mgr.create_and_wait_for_coordinator_topic();
          });
        if (ec != errc::success) {
            co_return tx::errc::partition_not_exists;
        }

        ntp_meta = _metadata_cache.local().get_topic_metadata(
          model::tx_manager_nt);
        if (!ntp_meta) {
            vlog(
              txlog.error,
              "Transaction manager topic {} not found",
              model::tx_manager_nt);
            co_return tx::errc::partition_not_exists;
        }
    }

    tx_gateway_frontend::return_all_txs_res res{{}};
    for (const auto& [_, pa] : ntp_meta->get_assignments()) {
        auto tx_manager_ntp = model::ntp(
          model::tx_manager_nt.ns, model::tx_manager_nt.tp, pa.id);
        auto ntp_res = co_await get_all_transactions_for_one_tx_partition(
          tx_manager_ntp);
        if (
          ntp_res.has_error() && ntp_res.error() == tx::errc::not_coordinator) {
            continue;
        }
        if (ntp_res.has_error()) {
            co_return std::move(ntp_res);
        }
        for (const auto& v : ntp_res.value()) {
            res.value().push_back(v);
        }
    }
    co_return std::move(res);
}

ss::future<result<tx_metadata, tx::errc>>
tx_gateway_frontend::describe_tx(kafka::transactional_id tid) {
    auto tm_ntp_opt = ntp_for_tx_id(tid);
    if (!tm_ntp_opt) {
        co_return tx::errc::coordinator_not_available;
    }
    auto tm_ntp = std::move(tm_ntp_opt.value());
    auto leader = co_await wait_for_leader(tm_ntp);
    if (leader != _self) {
        vlog(
          txlog.trace,
          "[tx_id={}] current node is not a leader for {}, current leader: {}",
          tid,
          tm_ntp,
          leader);
        co_return tx::errc::not_coordinator;
    }

    auto shard = _shard_table.local().shard_for(tm_ntp);

    if (!shard.has_value()) {
        vlog(
          txlog.warn,
          "[tx_id={}] transaction manager {} partition shard not found",
          tid,
          tm_ntp);
        co_return tx::errc::shard_not_found;
    }

    co_return co_await container().invoke_on(
      *shard,
      _ssg,
      [tid, tm_ntp = std::move(tm_ntp)](tx_gateway_frontend& self)
        -> ss::future<result<tx_metadata, tx::errc>> {
          return self.with_stm(
            tm_ntp.tp.partition,
            [&self, tid](checked<ss::shared_ptr<tm_stm>, tx::errc> r) {
                if (!r) {
                    return ssx::now<result<tx_metadata, tx::errc>>(r.error());
                }
                auto stm = r.value();
                return stm->read_lock().then(
                  [&self, stm, tid](ss::basic_rwlock<>::holder unit) {
                      return with(
                               stm,
                               tid,
                               "get_tx",
                               [&self, stm, tid]() {
                                   return self.describe_tx(stm, tid);
                               })
                        .finally([u = std::move(unit)] {});
                  });
            });
      });
}

ss::future<result<tx_metadata, tx::errc>> tx_gateway_frontend::describe_tx(
  ss::shared_ptr<tm_stm> stm, kafka::transactional_id tid) {
    auto sync_result = co_await sync_stm(stm);
    if (!sync_result.has_value()) {
        co_return sync_result.error();
    }
    auto term = sync_result.value();
    const auto timeout
      = config::shard_local_cfg().internal_rpc_request_timeout_ms();
    co_return co_await find_and_try_progressing_transaction(
      term, stm, tid, timeout);
}

ss::future<try_abort_reply>
tx_gateway_frontend::route_globally(try_abort_request&& r) {
    auto ntp = model::ntp(
      model::tx_manager_nt.ns, model::tx_manager_nt.tp, r.tm);
    return do_route_globally(ntp, std::move(r));
}

ss::future<try_abort_reply>
tx_gateway_frontend::route_locally(try_abort_request&& r) {
    auto ntp = model::ntp(
      model::tx_manager_nt.ns, model::tx_manager_nt.tp, r.tm);
    return do_route_locally(ntp, std::move(r));
}

ss::future<tx::errc> tx_gateway_frontend::delete_partition_from_tx(
  kafka::transactional_id tid, tx_metadata::tx_partition ntp) {
    auto tm_ntp = ntp_for_tx_id(tid);
    if (!tm_ntp) {
        co_return tx::errc::coordinator_not_available;
    }

    auto holder = _gate.hold();

    auto leader = co_await wait_for_leader(tm_ntp.value());
    if (leader != _self) {
        vlog(
          txlog.trace,
          "[tx_id={}] current node is not a leader for {}, current leader: {}",
          tid,
          tm_ntp.value(),
          leader);
        co_return tx::errc::not_coordinator;
    }

    auto shard = _shard_table.local().shard_for(tm_ntp.value());
    if (shard == std::nullopt) {
        vlog(
          txlog.warn,
          "[tx_id={}] transaction manager {} partition shard not found",
          tid,
          tm_ntp);
        co_return tx::errc::shard_not_found;
    }

    co_return co_await container().invoke_on(
      *shard, _ssg, [tid, ntp, tm_ntp](tx_gateway_frontend& self) {
          return self.with_stm(
            tm_ntp.value().tp.partition,
            [&self, tid, ntp](
              checked<ss::shared_ptr<tm_stm>, tx::errc> r) mutable {
                if (!r) {
                    auto e = r.error();
                    if (
                      e == tx::errc::partition_not_found
                      || e == tx::errc::stm_not_found) {
                        return ssx::now(tx::errc::invalid_txn_state);
                    }
                    return ssx::now(e);
                }
                auto stm = r.value();
                return stm->read_lock().then(
                  [&self, stm, tid, ntp](ss::basic_rwlock<>::holder unit) {
                      return with(
                               stm,
                               tid,
                               "delete_partition_from_tx",
                               [&self, stm, tid, ntp]() {
                                   return self.do_delete_partition_from_tx(
                                     stm, tid, ntp);
                               })
                        .finally([u = std::move(unit)] {});
                  });
            });
      });
}

ss::future<tx::errc> tx_gateway_frontend::do_delete_partition_from_tx(
  ss::shared_ptr<tm_stm> stm,
  kafka::transactional_id tid,
  tx_metadata::tx_partition ntp) {
    auto sync_result = co_await sync_stm(stm);
    if (!sync_result.has_value()) {
        co_return sync_result.error();
    }
    auto term = sync_result.value();

    auto res = co_await stm->delete_partition_from_tx(term, tid, ntp);

    if (res.has_error()) {
        switch (res.error()) {
        case tm_stm::op_status::not_leader:
            co_return tx::errc::leader_not_found;
        case tm_stm::op_status::partition_not_found:
            co_return tx::errc::partition_not_found;
        case tm_stm::op_status::conflict:
            co_return tx::errc::conflict;
        case cluster::tm_stm::op_status::not_found:
            co_return tx::errc::tx_id_not_found;
        default:
            co_return tx::errc::unknown_server_error;
        }
    }

    co_return tx::errc::none;
}

ss::future<tx::errc> tx_gateway_frontend::unsafe_abort_group_transaction(
  kafka::group_id group,
  model::producer_identity pid,
  model::tx_seq tx_seq,
  model::timeout_clock::duration timeout) {
    auto holder = _gate.hold();
    vlog(
      txlog.warn,
      "Issuing an unsafe abort of group transaction, group: {}, pid: {}, seq: "
      "{}, timeout: {}",
      group,
      pid,
      tx_seq,
      timeout);
    auto result = co_await _rm_group_proxy->abort_group_tx(
      std::move(group), pid, tx_seq, timeout);
    co_return result.ec;
}

ss::future<get_producers_reply>
tx_gateway_frontend::get_producers(get_producers_request request) {
    auto holder = _gate.hold();
    const auto& ntp = request.ntp;
    if (!_metadata_cache.local().contains(ntp)) {
        co_return get_producers_reply{
          .error_code = tx::errc::partition_not_exists};
    }

    auto leader_opt = _leaders.local().get_leader(ntp);
    if (!leader_opt) {
        co_return get_producers_reply{.error_code = tx::errc::leader_not_found};
    }
    auto leader = leader_opt.value();
    if (leader == _self) {
        auto shard = _shard_table.local().shard_for(ntp);
        if (!shard.has_value()) {
            co_return get_producers_reply{
              .error_code = tx::errc::shard_not_found};
        }
        co_return co_await container().invoke_on(
          shard.value(),
          _ssg,
          [request = std::move(request)](tx_gateway_frontend& local) mutable {
              return local.get_producers_locally(std::move(request));
          });
    }
    auto timeout = request.timeout;
    auto result = co_await _connection_cache.local()
                    .with_node_client<tx_gateway_client_protocol>(
                      _self,
                      ss::this_shard_id(),
                      leader,
                      model::timeout_clock::now() + timeout,
                      [request = std::move(request),
                       timeout](tx_gateway_client_protocol cp) mutable {
                          return cp.get_producers(
                            std::move(request),
                            rpc::client_opts(
                              model::timeout_clock::now() + timeout));
                      });
    if (result.has_error()) {
        co_return get_producers_reply{.error_code = tx::errc::not_coordinator};
    }
    co_return std::move(result.value().data);
}

ss::future<get_producers_reply>
tx_gateway_frontend::get_producers_locally(get_producers_request request) {
    auto& ntp = request.ntp;
    bool is_consumer_offsets_ntp = ntp.ns()
                                     == model::kafka_consumer_offsets_nt.ns()
                                   && ntp.tp.topic
                                        == model::kafka_consumer_offsets_nt.tp;

    if (is_consumer_offsets_ntp) {
        co_return co_await _rm_group_proxy->get_group_producers_locally(
          std::move(request));
    }
    co_return co_await _rm_partition_frontend.local().get_producers_locally(
      std::move(request));
}

} // namespace cluster

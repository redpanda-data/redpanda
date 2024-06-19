// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cluster/tx_gateway_frontend.h"

#include "cluster/controller.h"
#include "cluster/controller_api.h"
#include "cluster/id_allocator_frontend.h"
#include "cluster/logger.h"
#include "cluster/members_table.h"
#include "cluster/metadata_cache.h"
#include "cluster/partition_leaders_table.h"
#include "cluster/partition_manager.h"
#include "cluster/rm_group_proxy.h"
#include "cluster/rm_partition_frontend.h"
#include "cluster/shard_table.h"
#include "cluster/tm_stm.h"
#include "cluster/tm_stm_cache.h"
#include "cluster/tm_stm_cache_manager.h"
#include "cluster/topics_frontend.h"
#include "cluster/tx_coordinator_mapper.h"
#include "cluster/tx_gateway.h"
#include "cluster/tx_gateway_service.h"
#include "cluster/tx_helpers.h"
#include "cluster/tx_topic_manager.h"
#include "config/configuration.h"
#include "errc.h"
#include "model/fundamental.h"
#include "model/namespace.h"
#include "model/record.h"
#include "rpc/connection_cache.h"
#include "types.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/future.hh>
#include <seastar/core/gate.hh>
#include <seastar/util/log.hh>

#include <algorithm>
#include <optional>

namespace cluster {
using namespace std::chrono_literals;

template<typename Func>
static auto with(
  ss::shared_ptr<tm_stm> stm,
  kafka::transactional_id tx_id,
  const std::string_view name,
  Func&& func) noexcept {
    return stm->lock_tx(tx_id, name)
      .then([stm, tx_id, func = std::forward<Func>(func)](auto units) mutable {
          return ss::futurize_invoke(std::forward<Func>(func))
            .finally([units = std::move(units)] {});
      });
}

template<typename Func>
static auto with_free(
  ss::shared_ptr<tm_stm> stm,
  kafka::transactional_id tx_id,
  const std::string_view name,
  Func&& func) noexcept {
    auto units = stm->try_lock_tx(tx_id, name);
    auto f = ss::now();

    if (!units) {
        f = ss::make_exception_future(ss::semaphore_timed_out());
    }

    return f.then(
      [units = std::move(units), func = std::forward<Func>(func)]() mutable {
          return ss::futurize_invoke(std::forward<Func>(func))
            .finally([units = std::move(units)] {});
      });
}

static tx_metadata as_tx(
  kafka::transactional_id tx_id, model::term_id term, fetch_tx_reply reply) {
    vassert(
      reply.ec == tx::errc::none,
      "can't extract a tx from a failed (ec: {}) reply",
      reply.ec);
    tx_metadata tx;
    tx.id = tx_id;
    tx.pid = reply.pid;
    tx.last_pid = reply.last_pid;
    tx.tx_seq = reply.tx_seq;
    tx.etag = term;
    tx.timeout_ms = reply.timeout_ms;
    switch (reply.status) {
    case fetch_tx_reply::tx_status::ongoing:
        tx.status = tx_status::ongoing;
        break;
    case fetch_tx_reply::tx_status::preparing:
        tx.status = tx_status::preparing;
        break;
    case fetch_tx_reply::tx_status::prepared:
        tx.status = tx_status::prepared;
        break;
    case fetch_tx_reply::tx_status::aborting:
        tx.status = tx_status::aborting;
        break;
    case fetch_tx_reply::tx_status::killed:
        tx.status = tx_status::killed;
        break;
    case fetch_tx_reply::tx_status::ready:
        tx.status = tx_status::ready;
        break;
    case fetch_tx_reply::tx_status::tombstone:
        tx.status = tx_status::tombstone;
        break;
    }
    tx.partitions.reserve(reply.partitions.size());
    for (auto& p : reply.partitions) {
        tx.partitions.push_back(
          tx_metadata::tx_partition{p.ntp, p.etag, p.topic_revision});
    }
    tx.groups.reserve(reply.groups.size());
    for (auto& g : reply.groups) {
        tx.groups.push_back(tx_metadata::tx_group{g.group_id, g.etag});
    }
    return tx;
}

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

    auto _self = _controller->self();
    if (leader == _self) {
        co_return co_await do_route_locally(tx_ntp, std::move(request));
    }

    co_return co_await do_dispatch(leader, std::move(request));
}

template<typename T>
ss::future<typename T::reply>
tx_gateway_frontend::do_dispatch(model::node_id target, T&& request) {
    vlog(
      txlog.trace,
      "dispatching name: {}, from: {}, to: {}, request: {}",
      T::name,
      _controller->self(),
      target,
      request);
    auto timeout = request.timeout;
    return _connection_cache.local()
      .with_node_client<tx_gateway_client_protocol>(
        _controller->self(),
        ss::this_shard_id(),
        target,
        timeout,
        [request = std::move(request)](tx_gateway_client_protocol cp) mutable {
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
      [tm = tx_ntp.tp.partition, request = std::move(request)](
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

static add_paritions_tx_reply make_add_partitions_error_response(
  add_paritions_tx_request request, tx::errc ec) {
    add_paritions_tx_reply response;
    response.results.reserve(request.topics.size());
    for (auto& req_topic : request.topics) {
        add_paritions_tx_reply::topic_result res_topic;
        res_topic.name = req_topic.name;
        res_topic.results.reserve(req_topic.partitions.size());
        for (model::partition_id req_partition : req_topic.partitions) {
            add_paritions_tx_reply::partition_result res_partition;
            res_partition.partition_index = req_partition;
            res_partition.error_code = ec;
            res_topic.results.push_back(res_partition);
        }
        response.results.push_back(res_topic);
    }
    return response;
}

tx_gateway_frontend::tx_gateway_frontend(
  ss::smp_service_group ssg,
  ss::sharded<cluster::partition_manager>& partition_manager,
  ss::sharded<cluster::shard_table>& shard_table,
  ss::sharded<cluster::metadata_cache>& metadata_cache,
  ss::sharded<rpc::connection_cache>& connection_cache,
  ss::sharded<partition_leaders_table>& leaders,
  cluster::controller* controller,
  ss::sharded<cluster::id_allocator_frontend>& id_allocator_frontend,
  rm_group_proxy* group_proxy,
  ss::sharded<cluster::rm_partition_frontend>& rm_partition_frontend,
  ss::sharded<features::feature_table>& feature_table,
  ss::sharded<cluster::tm_stm_cache_manager>& tm_stm_cache_manager,
  ss::sharded<cluster::tx_topic_manager>& tx_topic_manager,
  config::binding<uint64_t> max_transactions_per_coordinator)
  : _ssg(ssg)
  , _partition_manager(partition_manager)
  , _shard_table(shard_table)
  , _metadata_cache(metadata_cache)
  , _connection_cache(connection_cache)
  , _leaders(leaders)
  , _controller(controller)
  , _id_allocator_frontend(id_allocator_frontend)
  , _rm_group_proxy(group_proxy)
  , _rm_partition_frontend(rm_partition_frontend)
  , _feature_table(feature_table)
  , _tm_stm_cache_manager(tm_stm_cache_manager)
  , _tx_topic_manager(tx_topic_manager)
  , _metadata_dissemination_retries(
      config::shard_local_cfg().metadata_dissemination_retries.value())
  , _metadata_dissemination_retry_delay_ms(
      config::shard_local_cfg().metadata_dissemination_retry_delay_ms.value())
  , _transactional_id_expiration(
      config::shard_local_cfg().transactional_id_expiration_ms.bind())
  , _transactions_enabled(config::shard_local_cfg().enable_transactions.value())
  , _max_transactions_per_coordinator(max_transactions_per_coordinator) {
    /**
     * do not start expriry timer when transactions are disabled
     */
    if (_transactions_enabled) {
        start_expire_timer();
    }
    _transactional_id_expiration.watch(
      [this]() { rearm_expire_timer(/*force=*/true); });
}

void tx_gateway_frontend::start_expire_timer() {
    if (ss::this_shard_id() != 0) {
        // tx_gateway_frontend is intented to be used only as a sharded
        // service (run on all cores) so constraining it to a core will
        // guarantee that there is only one active gc process.
        //
        // the gc part (expire_old_txs) does the shard managment and
        // relays the execution to the right core so it's enough to
        // have only one timer/loop
        return;
    }
    _expire_timer.set_callback([this] { expire_old_txs(); });
    rearm_expire_timer();
}

void tx_gateway_frontend::rearm_expire_timer(bool force) {
    if (ss::this_shard_id() != 0 || _gate.is_closed()) {
        return;
    }
    if (force) {
        _expire_timer.cancel();
    }
    if (!_expire_timer.armed()) {
        // we need to expire transactional ids which were inactive more than
        // transactional_id_expiration period. if we check for the expired
        // transactions twice during the period then in the worst case an
        // expired id lives at most 1.5 x transactional_id_expiration
        auto delay = _transactional_id_expiration() / 2;
        _expire_timer.arm(model::timeout_clock::now() + delay);
    }
}

ss::future<> tx_gateway_frontend::stop() {
    vlog(txlog.debug, "Asked to stop tx coordinator");
    _expire_timer.cancel();
    _as.request_abort();
    return _gate.close().then(
      [] { vlog(txlog.debug, "Tx coordinator is stopped"); });
}

ss::future<find_coordinator_reply>
tx_gateway_frontend::find_coordinator(kafka::transactional_id tid) {
    auto tp_md = _metadata_cache.local().get_topic_metadata_ref(
      model::tx_manager_nt);

    if (unlikely(!tp_md)) {
        vlog(
          txlog.warn,
          "[tx_id={}] transactional manager topic {} doesn't exists in "
          "metadata cache",
          tid,
          model::tx_manager_nt);

        auto ec = co_await _tx_topic_manager.invoke_on(
          cluster::tx_topic_manager::shard, [](tx_topic_manager& mgr) {
              return mgr.create_and_wait_for_coordinator_topic();
          });

        if (ec != errc::success) {
            co_return find_coordinator_reply(
              std::nullopt, std::nullopt, errc::topic_not_exists);
        }
        // query for metadata once again
        tp_md = _metadata_cache.local().get_topic_metadata_ref(
          model::tx_manager_nt);
        if (unlikely(!tp_md)) {
            vlog(
              txlog.error,
              "[tx_id={}] unable to create tx_manager topic",
              tid,
              model::tx_manager_nt);
            co_return find_coordinator_reply(
              std::nullopt, std::nullopt, errc::topic_not_exists);
        }
    }

    auto ntp = get_tx_coordinator_ntp(
      tid, tp_md.value().get().get_configuration().partition_count);

    auto leader = co_await wait_for_leader(ntp);
    vlog(
      txlog.trace,
      "[tx_id={}] found coordinator: {}, leader: {}",
      tid,
      ntp,
      leader);
    co_return find_coordinator_reply{leader, std::move(ntp), errc::success};
}

std::optional<model::ntp>
tx_gateway_frontend::ntp_for_tx_id(const kafka::transactional_id& id) {
    vlog(txlog.trace, "[tx_id={}] getting coordinator ntp", id);
    auto tp_md = _metadata_cache.local().get_topic_metadata_ref(
      model::tx_manager_nt);
    if (!tp_md) {
        vlog(
          txlog.trace,
          "[tx_id={}] get_ntp request failed due to lack of coordinator topic "
          "metadata for: {}",
          id,
          model::tx_manager_nt);
        // Transaction coordinator topic not exist in cache
        // should be catched by caller (find_coordinator)
        // It must wait for topic in cache or init topic
        return std::nullopt;
    }

    return get_tx_coordinator_ntp(
      id, tp_md.value().get().get_configuration().partition_count);
}

ss::future<std::optional<model::node_id>>
tx_gateway_frontend::wait_for_leader(const model::ntp& ntp) {
    const auto timeout
      = model::timeout_clock::now()
        + (_metadata_dissemination_retry_delay_ms * _metadata_dissemination_retries);
    try {
        co_return co_await _leaders.local().wait_for_leader(ntp, timeout, _as);
    } catch (const ss::timed_out_error&) {
        co_return std::nullopt;
    }
}

ss::future<fetch_tx_reply> tx_gateway_frontend::fetch_tx_locally(
  kafka::transactional_id tx_id, model::term_id term, model::partition_id tm) {
    vlog(
      txlog.trace,
      "[tx_id={}] fetching transaction locally term: {}, partition: {}",
      tx_id,
      term,
      tm);
    auto map =
      [tx_id, term, tm](
        tm_stm_cache_manager& cache_manager) -> std::optional<tx_metadata> {
        auto cache = cache_manager.get(tm);
        return cache->find(term, tx_id);
    };
    auto reduce = [](
                    std::optional<tx_metadata> a,
                    std::optional<tx_metadata> b) { return a ? a : b; };
    auto tx_opt = co_await _tm_stm_cache_manager.map_reduce0(
      map, std::optional<tx_metadata>{}, reduce);

    vlog(
      txlog.trace, "[tx_id={}] fetching transaction result: {}", tx_id, tx_opt);

    if (!tx_opt) {
        co_return fetch_tx_reply(tx::errc::tx_not_found);
    }
    auto tx = tx_opt.value();

    fetch_tx_reply reply;
    reply.ec = tx::errc::none;
    reply.pid = tx.pid;
    reply.last_pid = tx.last_pid;
    reply.tx_seq = tx.tx_seq;
    reply.timeout_ms = tx.timeout_ms;

    switch (tx.status) {
    case tx_status::ongoing:
        reply.status = fetch_tx_reply::tx_status::ongoing;
        break;
    case tx_status::preparing:
        reply.status = fetch_tx_reply::tx_status::preparing;
        break;
    case tx_status::prepared:
        reply.status = fetch_tx_reply::tx_status::prepared;
        break;
    case tx_status::aborting:
        reply.status = fetch_tx_reply::tx_status::aborting;
        break;
    case tx_status::killed:
        reply.status = fetch_tx_reply::tx_status::killed;
        break;
    case tx_status::ready:
        reply.status = fetch_tx_reply::tx_status::ready;
        break;
    case tx_status::tombstone:
        reply.status = fetch_tx_reply::tx_status::tombstone;
        break;
    }

    reply.partitions.reserve(tx.partitions.size());
    for (auto& p : tx.partitions) {
        reply.partitions.emplace_back(p.ntp, p.etag, p.topic_revision);
    }
    reply.groups.reserve(tx.groups.size());
    for (auto& g : tx.groups) {
        reply.groups.emplace_back(g.group_id, g.etag);
    }
    co_return reply;
}

ss::future<checked<tx_metadata, tx::errc>> tx_gateway_frontend::fetch_tx(
  kafka::transactional_id tx_id, model::term_id term, model::partition_id tm) {
    vlog(
      txlog.trace,
      "[tx_id={}] fetching transactions from partition: {} in term: {}",
      tx_id,
      tm,
      term);

    auto outcome
      = ss::make_lw_shared<available_promise<checked<tx_metadata, tx::errc>>>();
    ssx::spawn_with_gate(_gate, [this, tx_id, term, outcome, tm] {
        return dispatch_fetch_tx(
                 tx_id,
                 term,
                 tm,
                 _metadata_dissemination_retry_delay_ms,
                 outcome)
          .finally([outcome]() {
              if (!outcome->available()) {
                  outcome->set_value(tx::errc::tx_not_found);
              }
          });
    });
    return outcome->get_future();
}

ss::future<> tx_gateway_frontend::dispatch_fetch_tx(
  kafka::transactional_id tx_id,
  model::term_id term,
  model::partition_id tm,
  model::timeout_clock::duration timeout,
  ss::lw_shared_ptr<available_promise<checked<tx_metadata, tx::errc>>>
    outcome) {
    auto& members_table = _controller->get_members_table();
    auto node_ids = members_table.local().node_ids();

    std::vector<ss::future<fetch_tx_reply>> inflight;
    inflight.reserve(node_ids.size());
    auto self = _controller->self();
    for (auto node_id : node_ids) {
        if (node_id == self) {
            vlog(
              txlog.trace,
              "[tx_id={}] fetching tx in term: {} partition_id: {} from:  {}",
              tx_id,
              term,
              tm,
              node_id);
            inflight.push_back(
              fetch_tx_locally(tx_id, term, tm)
                .then(
                  [tx_id, term, outcome, node_id, tm](fetch_tx_reply reply) {
                      vlog(
                        txlog.trace,
                        "[tx_id={}] error fetching tx in term: {} on "
                        "partition_id {} from {} - {}",
                        tx_id,
                        term,
                        tm,
                        node_id,
                        reply.ec);
                      if (reply.ec == tx::errc::none) {
                          if (!outcome->available()) {
                              auto tx = as_tx(tx_id, term, reply);
                              outcome->set_value(tx);
                          }
                      }
                      return reply;
                  }));
        } else {
            inflight.push_back(
              dispatch_fetch_tx(node_id, tx_id, term, tm, timeout, outcome));
        }
    }
    co_await when_all_succeed(inflight.begin(), inflight.end());
}

ss::future<fetch_tx_reply> tx_gateway_frontend::dispatch_fetch_tx(
  model::node_id target,
  kafka::transactional_id tx_id,
  model::term_id term,
  model::partition_id tm,
  model::timeout_clock::duration timeout,
  ss::lw_shared_ptr<available_promise<checked<tx_metadata, tx::errc>>>
    outcome) {
    vlog(
      txlog.trace,
      "[tx_id={}] dispatching fetch_tx request in term: {} for partition: {} "
      "to {}",
      tx_id,
      term,
      tm,
      target);
    return _connection_cache.local()
      .with_node_client<tx_gateway_client_protocol>(
        _controller->self(),
        ss::this_shard_id(),
        target,
        timeout,
        [tx_id, term, tm, timeout](tx_gateway_client_protocol cp) {
            return cp.fetch_tx(
              fetch_tx_request(tx_id, term, tm),
              rpc::client_opts(model::timeout_clock::now() + timeout));
        })
      .then(&rpc::get_ctx_data<fetch_tx_reply>)
      .then([outcome, tx_id, term, target](result<fetch_tx_reply> r) {
          if (r.has_error()) {
              vlog(
                txlog.warn,
                "[tx_id={}] error fetching tx in term {} from {} - {}",
                tx_id,
                term,
                target,
                r.error());
              return fetch_tx_reply(tx::errc::unknown_server_error);
          }
          auto reply = r.value();
          vlog(
            txlog.trace,
            "[tx_id={}] fetching tx in term {} from {} result: {}",
            tx_id,
            term,
            target,
            r.value());
          if (reply.ec == tx::errc::none) {
              if (!outcome->available()) {
                  auto tx = as_tx(tx_id, term, reply);
                  outcome->set_value(tx);
              }
          }
          return r.value();
      });
}

ss::future<try_abort_reply> tx_gateway_frontend::process_locally(
  ss::shared_ptr<tm_stm> stm, try_abort_request&& request) {
    vlog(txlog.trace, "processing request: {}", request);

    return stm->read_lock().then([this, stm, request = std::move(request)](
                                   ss::basic_rwlock<>::holder unit) {
        return stm->barrier()
          .then([this, stm, request = std::move(request)](
                  checked<model::term_id, tm_stm::op_status> term_opt) {
              auto pid = request.pid;
              auto tx_seq = request.tx_seq;
              auto timeout = request.timeout;

              if (!term_opt.has_value()) {
                  vlog(txlog.debug, "stm barrier error: {}", term_opt.error());
                  if (term_opt.error() == tm_stm::op_status::not_leader) {
                      return ss::make_ready_future<try_abort_reply>(
                        try_abort_reply{tx::errc::not_coordinator});
                  }
                  return ss::make_ready_future<try_abort_reply>(
                    try_abort_reply{tx::errc::unknown_server_error});
              }
              auto term = term_opt.value();
              auto tx_id_opt = stm->get_id_by_pid(pid);
              if (!tx_id_opt) {
                  vlog(
                    txlog.trace,
                    "can't find tx by pid: {} considering it aborted",
                    pid);
                  return ss::make_ready_future<try_abort_reply>(
                    try_abort_reply::make_aborted());
              }
              auto tx_id = tx_id_opt.value();

              return with_free(
                       stm,
                       tx_id,
                       "try_abort",
                       [this, stm, term, tx_id, pid, tx_seq, timeout]() {
                           return do_try_abort(
                             term, stm, tx_id, pid, tx_seq, timeout);
                       })
                .handle_exception_type([](const ss::semaphore_timed_out&) {
                    return try_abort_reply{tx::errc::unknown_server_error};
                })
                .then([this, stm, tx_id, term, timeout](auto reply) {
                    if (reply.ec == tx::errc::none) {
                        ssx::spawn_with_gate(
                          _gate, [this, stm, tx_id, timeout, term]() mutable {
                              return stm->read_lock()
                                .then(
                                  [this, stm, tx_id, timeout, term](
                                    ss::basic_rwlock<>::holder unit) mutable {
                                      return with(
                                               stm,
                                               tx_id,
                                               "try_abort:get_tx",
                                               [this,
                                                stm,
                                                tx_id,
                                                timeout,
                                                term]() mutable {
                                                   return get_tx(
                                                     term, stm, tx_id, timeout);
                                               })
                                        .finally([u = std::move(unit)] {});
                                  })
                                .discard_result();
                          });
                    }
                    return reply;
                });
          })
          .finally([u = std::move(unit)] {});
    });
}

ss::future<try_abort_reply> tx_gateway_frontend::do_try_abort(
  model::term_id term,
  ss::shared_ptr<tm_stm> stm,
  kafka::transactional_id tx_id,
  model::producer_identity pid,
  model::tx_seq tx_seq,
  model::timeout_clock::duration) {
    auto term_opt = co_await stm->sync();

    if (!term_opt.has_value()) {
        if (term_opt.error() == tm_stm::op_status::not_leader) {
            vlog(
              txlog.trace, "[tx_id={}] this node isn't a coordinator", tx_id);
            co_return try_abort_reply{tx::errc::not_coordinator};
        }
        vlog(
          txlog.warn,
          "[tx_id={}] error syncing in memory state - {}",
          tx_id,
          term_opt.error());
        co_return try_abort_reply{tx::errc::timeout};
    }

    if (term_opt.value() != term) {
        vlog(
          txlog.info,
          "[tx_id={}] error syncing in memory state, empty term optional",
          tx_id);
        co_return try_abort_reply{tx::errc::not_coordinator};
    }

    auto tx_opt = co_await stm->get_tx(tx_id);
    if (!tx_opt.has_value()) {
        if (tx_opt.error() == tm_stm::op_status::not_found) {
            vlog(
              txlog.trace,
              "[tx_id={}] can't find a tx (pid: {} tx_seq: {}) considering it "
              "aborted",
              tx_id,
              pid,
              tx_seq);
            co_return try_abort_reply::make_aborted();
        } else {
            vlog(
              txlog.warn,
              "[tx_id={}] error looking up the transaction - {}",
              tx_id,
              tx_opt.error());
            co_return try_abort_reply{tx::errc::unknown_server_error};
        }
    }

    auto tx = tx_opt.value();

    if (tx.transferring) {
        tx_opt = co_await stm->reset_transferring(term, tx_id);
        if (!tx_opt.has_value()) {
            vlog(
              txlog.warn,
              "[tx_id={}] error rehydrating transaction - {}",
              tx_id,
              tx_opt.error());
            // any error on lookin up a tx is a retriable error
            co_return try_abort_reply{tx::errc::not_coordinator};
        }
        tx = tx_opt.value();
    }

    if (tx.etag > term) {
        vlog(
          txlog.trace,
          "[tx_id={}] fenced aborting transaction, etag: {} is greater than "
          "current term: {}",
          tx_id,
          tx.etag,
          term);
        // tx was written by a future leader meaning current
        // node can't be a leader
        co_return try_abort_reply{tx::errc::not_coordinator};
    }

    if (tx.etag < term) {
        // old tx indicates that we need to use fetch_tx to discover an
        // in-memory state of the ongoing tx from previous leader and
        // adopt it but this process may require finishing a tx (recommit
        // or reabort) which may cause a deadlock:
        //   1) data partition takes tx.id lock and invokes try_abort on
        //      txn coordinator
        //   2) txn coordinator takes tx.id lock and invokes recommit
        //   3) recommit reaches out to data partition and attempts to
        //      take tx.id lock (deadlock!)
        //
        // to solve the problem we:
        //   - return unknown status to data partition (it will release to
        //     lock and retry the request later within tx_timeout_delay_ms)
        //   - run a background fiber executing get_tx to fetch in-mem
        //     state
        //   - by the time try_abort is retried the tx state should be
        //     up-to-date
        co_return try_abort_reply{tx::errc::none};
    }

    if (tx.pid != pid || tx.tx_seq != tx_seq) {
        vlog(
          txlog.trace,
          "[tx_id={}] found tx has pid: {} tx_seq: {} (expecting pid: {} "
          "tx_seq: {}) considering it aborted",
          tx_id,
          tx.pid,
          tx.tx_seq,
          pid,
          tx_seq);
        co_return try_abort_reply::make_aborted();
    }

    if (tx.status == tx_status::prepared) {
        vlog(
          txlog.trace,
          "[tx_id={}] pid: {} tx_seq: {} is prepared => considering it "
          "committed",
          tx_id,
          tx.pid,
          tx.tx_seq);
        co_return try_abort_reply::make_committed();
    } else if (
      tx.status == tx_status::aborting || tx.status == tx_status::killed
      || tx.status == tx_status::ready) {
        vlog(
          txlog.trace,
          "[tx_id={}] pid: {} tx_seq: {} has status: {} => considering it "
          "aborted",
          tx_id,
          tx.pid,
          tx.tx_seq,
          tx.status);
        // when it's ready it means in-memory state was lost
        // so can't be comitted and it's save to aborted
        co_return try_abort_reply::make_aborted();
    } else if (
      tx.status == tx_status::preparing || tx.status == tx_status::ongoing) {
        vlog(
          txlog.trace,
          "[tx_id={}] pid: {} tx_seq: {} is ongoing => forcing it to be "
          "aborted",
          tx_id,
          tx.pid,
          tx.tx_seq);
        auto killed_tx = co_await stm->mark_tx_killed(term, tx.id);
        if (!killed_tx.has_value()) {
            if (killed_tx.error() == tm_stm::op_status::not_leader) {
                co_return try_abort_reply{tx::errc::not_coordinator};
            }
            co_return try_abort_reply{tx::errc::unknown_server_error};
        }
        co_return try_abort_reply::make_aborted();
    } else {
        vlog(txlog.error, "[tx_id={}] unknown status: {}", tx_id, tx.status);
        co_return try_abort_reply{tx::errc::unknown_server_error};
    }
}

ss::future<cluster::init_tm_tx_reply> tx_gateway_frontend::init_tm_tx(
  kafka::transactional_id tx_id,
  std::chrono::milliseconds transaction_timeout_ms,
  model::timeout_clock::duration timeout,
  model::producer_identity expected_pid) {
    vlog(
      txlog.trace,
      "[tx_id={}] init_tm_tx request begin, expected_pid: {}",
      tx_id,
      expected_pid);
    auto retries = _metadata_dissemination_retries;
    auto delay_ms = _metadata_dissemination_retry_delay_ms;
    /**
     * If transactional manager metadata is missing, wait for it
     */
    if (unlikely(!_metadata_cache.local().contains(model::tx_manager_nt))) {
        while (!_as.abort_requested() && retries-- > 0) {
            vlog(
              txlog.trace,
              "[tx_id: {}] waiting for {} topic to apper in metadata cache, "
              "retries left: {}",
              tx_id,
              model::tx_manager_nt,
              retries);
            if (_metadata_cache.local().contains(model::tx_manager_nt)) {
                break;
            }
            co_await sleep_abortable(delay_ms, _as);
        }
        if (!_metadata_cache.local().contains(model::tx_manager_nt)) {
            vlog(
              txlog.warn,
              "[{}] transaction coordinator topic {} not found in "
              "metadata_cache",
              tx_id,
              model::tx_manager_nt);
            co_return cluster::init_tm_tx_reply{tx::errc::partition_not_exists};
        }
    }

    auto coordinator_ntp = ntp_for_tx_id(tx_id);
    if (!coordinator_ntp) {
        co_return cluster::init_tm_tx_reply{tx::errc::not_coordinator};
    }
    retries = _metadata_dissemination_retries;

    auto leader_opt = co_await wait_for_leader(*coordinator_ntp);
    if (!leader_opt) {
        vlog(
          txlog.warn,
          "[tx_id={}] init_tm_tx request failed, can't find {} in the leaders "
          "cache",
          tx_id,
          *coordinator_ntp);
        co_return cluster::init_tm_tx_reply{tx::errc::leader_not_found};
    }

    auto leader = leader_opt.value();
    auto _self = _controller->self();

    if (leader != _self) {
        vlog(
          txlog.trace,
          "[tx_id={}] init_tm_tx request failed, this node {} is not the "
          "leader for {}, found leader: {}",
          tx_id,
          _self,
          *coordinator_ntp,
          leader);
        co_return cluster::init_tm_tx_reply{tx::errc::not_coordinator};
    }

    co_return co_await init_tm_tx_locally(
      tx_id,
      transaction_timeout_ms,
      timeout,
      expected_pid,
      coordinator_ntp->tp.partition);
}

ss::future<cluster::init_tm_tx_reply> tx_gateway_frontend::init_tm_tx_locally(
  kafka::transactional_id tx_id,
  std::chrono::milliseconds transaction_timeout_ms,
  model::timeout_clock::duration timeout,
  model::producer_identity expected_pid,
  model::partition_id tm) {
    vlog(
      txlog.trace,
      "[tx_id={}] processing name:init_tm_tx, timeout: {}",
      tx_id,
      transaction_timeout_ms);

    model::ntp tx_ntp(model::tx_manager_nt.ns, model::tx_manager_nt.tp, tm);
    auto shard = _shard_table.local().shard_for(tx_ntp);

    auto retries = _metadata_dissemination_retries;
    auto delay_ms = _metadata_dissemination_retry_delay_ms;
    auto aborted = false;
    while (!aborted && !shard && 0 < retries--) {
        aborted = !co_await sleep_abortable(delay_ms, _as);
        shard = _shard_table.local().shard_for(tx_ntp);
    }

    if (!shard) {
        vlog(
          txlog.trace,
          "[tx_id={}] init_tm_tx failed, ec: {}, no shard found for {}",
          tx_id,
          tx::errc::shard_not_found,
          tx_ntp);
        co_return cluster::init_tm_tx_reply{tx::errc::shard_not_found};
    }

    auto reply = co_await container().invoke_on(
      shard.value(),
      _ssg,
      [tx_id, transaction_timeout_ms, timeout, expected_pid, tm](
        tx_gateway_frontend& self) -> ss::future<cluster::init_tm_tx_reply> {
          return ss::with_gate(
            self._gate,
            [tx_id, transaction_timeout_ms, timeout, expected_pid, tm, &self]()
              -> ss::future<cluster::init_tm_tx_reply> {
                return self.with_stm(
                  tm,
                  [tx_id, transaction_timeout_ms, timeout, expected_pid, &self](
                    checked<ss::shared_ptr<tm_stm>, tx::errc> r) {
                      if (!r) {
                          return ss::make_ready_future<
                            cluster::init_tm_tx_reply>(
                            cluster::init_tm_tx_reply{r.error()});
                      }
                      auto stm = r.value();
                      return stm->read_lock().then(
                        [&self,
                         stm,
                         tx_id,
                         transaction_timeout_ms,
                         expected_pid,
                         timeout](ss::basic_rwlock<>::holder unit) {
                            return self
                              .limit_init_tm_tx(
                                stm,
                                tx_id,
                                transaction_timeout_ms,
                                timeout,
                                expected_pid)
                              .finally([u = std::move(unit)] {});
                        });
                  });
            });
      });

    vlog(
      txlog.trace,
      "[tx_id={}] sending name:init_tm_tx, pid: {}, ec: {}",
      tx_id,
      reply.pid,
      reply.ec);

    co_return reply;
}

namespace {

/*
 *"If no producerId/epoch is provided, the producer is initializing for the
 * first time:
 *
 *   If there is no current producerId/epoch, generate a new producerId and set
 *   epoch=0.
 *
 *   Otherwise, we need to bump the epoch, which could mean overflow:
 *     No overflow: Bump the epoch and return the current producerId with the
 *     bumped epoch. The last producerId/epoch will be set to empty.
 *
 *     Overflow: Initialize a new producerId with epoch=0 and return it. The
 * last producerId/epoch will be set to empty.
 *
 * If producerId/epoch is provided, then the
 * producer is re-initializing after a failure. There are three cases:
 *
 *   The provided producerId/epoch matches the existing producerId/epoch, so we
 *   need to bump the epoch.
 *
 *   As above, we may need to generate a new producerId if there would be
 * overflow bumping the epoch:
 *
 *     No overflow: bump the epoch and return the current producerId with the
 *     bumped epoch. The last producerId/epoch will be set to the previous
 *     producerId/epoch.
 *
 *     Overflow: generate a new producerId with epoch=0. The last
 * producerId/epoch will be set to the old producerId/epoch.
 *
 *   The provided producerId/epoch matches the last
 *   producerId/epoch. The current producerId/epoch will be returned.
 *
 *   Else return INVALID_PRODUCER_EPOCH"
 *
 *
 *https://cwiki.apache.org/confluence/pages/viewpage.action?pageId=89068820
 */

bool is_max_epoch(int16_t epoch) {
    return epoch >= std::numeric_limits<int16_t>::max();
}

// This check returns true if current producer_id is the same for expected_pid
// from request or we had epoch overflow and expected producer id from request
// matches with last producer_id from log record
bool is_valid_producer(
  const tx_metadata& tx, const model::producer_identity& expected_pid) {
    if (expected_pid == model::no_pid) {
        return true;
    }

    return tx.pid == expected_pid || tx.last_pid == expected_pid;
}

} // namespace

ss::future<cluster::init_tm_tx_reply> tx_gateway_frontend::limit_init_tm_tx(
  ss::shared_ptr<tm_stm> stm,
  kafka::transactional_id tx_id,
  std::chrono::milliseconds transaction_timeout_ms,
  model::timeout_clock::duration timeout,
  model::producer_identity expected_pid) {
    auto term_opt = co_await stm->sync();
    if (!term_opt.has_value()) {
        if (term_opt.error() == tm_stm::op_status::not_leader) {
            vlog(txlog.trace, "[tx_id={}] node is not a leader", tx_id);
            co_return init_tm_tx_reply{tx::errc::not_coordinator};
        }
        vlog(
          txlog.warn,
          "[tx_id={}] error syncing stm - {}",
          tx_id,
          term_opt.error());
        co_return init_tm_tx_reply{tx::errc::not_coordinator};
    }
    auto term = term_opt.value();

    auto units = co_await stm->lock_tx(tx_id, "init_tm_tx");
    if ((co_await stm->get_tx(tx_id)).has_value()) {
        co_return co_await do_init_tm_tx(
          stm, term, tx_id, transaction_timeout_ms, timeout, expected_pid);
    }
    units.return_all();

    if (stm->tx_cache_size() > _max_transactions_per_coordinator()) {
        // lock is sloppy and doesn't guarantee that tx_cache_size
        // never exceeds _max_transactions_per_coordinator. init_tm_tx
        // request may pass limit_init_tm_tx but not yet increase
        // tx_cache_size so there is a small window of time when the
        // next init tx request may pass too even if the first request
        // eventually tip tx cache over max transactions per coordinator.
        // it isn't the problem, the next request will correct it
        auto init_units = co_await stm->get_tx_thrashing_lock().get_units();

        // similar to double-checked locking pattern
        // it protects concurrent access to oldest_tx
        while (stm->tx_cache_size() > _max_transactions_per_coordinator()) {
            auto old_tx_opt = stm->oldest_tx();
            if (!old_tx_opt) {
                vlog(
                  txlog.warn,
                  "oldest_tx should return oldest tx when the tx cache size "
                  "({}) is beyond capacity ({})",
                  stm->tx_cache_size(),
                  _max_transactions_per_coordinator());
                co_return init_tm_tx_reply{tx::errc::not_coordinator};
            }

            auto old_tx = old_tx_opt.value();
            vlog(
              txlog.info,
              "tx cache size ({}) is beyond capacity ({}); expiring oldest tx "
              "(tx.id={})",
              stm->tx_cache_size(),
              _max_transactions_per_coordinator(),
              old_tx.id);
            auto tx_units = co_await stm->lock_tx(old_tx.id, "init_tm_tx");

            auto timeout = config::shard_local_cfg().create_topic_timeout_ms();
            auto tx_maybe = co_await get_tx(term, stm, old_tx.id, timeout);
            if (tx_maybe.has_value()) {
                old_tx = tx_maybe.value();
                auto ec = co_await do_expire_old_tx(
                  stm, term, old_tx, timeout, true);
                if (ec != tx::errc::none) {
                    vlog(
                      txlog.warn,
                      "expiring old tx (tx.id={}) failed with ec={}",
                      old_tx.id,
                      ec);
                    co_return init_tm_tx_reply{tx::errc::not_coordinator};
                }
            } else if (tx_maybe.error() != tx::errc::tx_not_found) {
                vlog(
                  txlog.warn,
                  "can't look up a tx (tx.id={}): ec={}",
                  old_tx.id,
                  tx_maybe.error());
                co_return init_tm_tx_reply{tx::errc::not_coordinator};
            }
            tx_units.return_all();
        }
        vlog(txlog.info, "tx cache size is reduced");
        init_units.return_all();
    }

    units = co_await stm->lock_tx(tx_id, "init_tm_tx");

    co_return co_await do_init_tm_tx(
      stm, term, tx_id, transaction_timeout_ms, timeout, expected_pid);
}

ss::future<cluster::init_tm_tx_reply> tx_gateway_frontend::do_init_tm_tx(
  ss::shared_ptr<tm_stm> stm,
  model::term_id term,
  kafka::transactional_id tx_id,
  std::chrono::milliseconds transaction_timeout_ms,
  model::timeout_clock::duration timeout,
  model::producer_identity expected_pid) {
    auto r0 = co_await get_tx(term, stm, tx_id, timeout);

    if (!r0.has_value()) {
        if (r0.error() != tx::errc::tx_not_found) {
            vlog(
              txlog.warn,
              "[tx_id={}] error getting transaction: {}",
              tx_id,
              r0.error());
            co_return init_tm_tx_reply{tx::errc::not_coordinator};
        }

        // tx is missing
        allocate_id_reply pid_reply
          = co_await _id_allocator_frontend.local().allocate_id(timeout);
        if (pid_reply.ec != errc::success) {
            vlog(
              txlog.warn,
              "[tx_id={}] allocate_id failed - {}",
              tx_id,
              pid_reply.ec);
            co_return init_tm_tx_reply{tx::errc::not_coordinator};
        }

        model::producer_identity pid{pid_reply.id, 0};
        tm_stm::op_status op_status = co_await stm->register_new_producer(
          term, tx_id, transaction_timeout_ms, pid);
        init_tm_tx_reply reply;
        reply.pid = pid;
        if (op_status == tm_stm::op_status::success) {
            reply.ec = tx::errc::none;
        } else if (op_status == tm_stm::op_status::conflict) {
            vlog(
              txlog.warn,
              "[tx_id={}] got conflict on registering new producer {}",
              tx_id,
              pid);
            reply.ec = tx::errc::conflict;
        } else if (op_status == tm_stm::op_status::not_leader) {
            vlog(txlog.warn, "[tx_id={}] this node is not coordinator", tx_id);
            reply.ec = tx::errc::not_coordinator;
        } else {
            vlog(
              txlog.warn,
              "[tx_id={}] error registering producer with pid: {} - {}",
              tx_id,
              pid,
              op_status);
            reply.ec = tx::errc::invalid_txn_state;
        }
        co_return reply;
    }
    auto tx = r0.value();

    if (!is_valid_producer(tx, expected_pid)) {
        vlog(
          txlog.info,
          "[tx_id={}] producer with pid: {} for {} is invalid",
          tx_id,
          expected_pid,
          tx);
        co_return init_tm_tx_reply{tx::errc::invalid_producer_epoch};
    }

    checked<tx_metadata, tx::errc> r(tx);

    if (tx.status == tx_status::ongoing) {
        vlog(txlog.info, "[tx_id={}] tx is ongoing, aborting", tx_id);
        r = co_await do_abort_tm_tx(term, stm, tx, timeout);
    } else if (tx.status == tx_status::preparing) {
        vlog(txlog.info, "[tx_id={}] tx is preparing, aborting", tx_id);
        // preparing is obsolete, also it isn't acked until
        // it's prepared si it's safe to abort it
        r = co_await do_abort_tm_tx(term, stm, tx, timeout);
    }

    if (!r.has_value()) {
        vlog(
          txlog.warn,
          "[tx_id={}] error rolling previous with status: {} - {}",
          tx_id,
          tx.status,
          r.error());
        co_return init_tm_tx_reply{r.error()};
    }

    tx = r.value();
    init_tm_tx_reply reply;
    // note: while rolled_pid and last_pid look very similar in intent which is
    // to track previous incarnation of this transaction_id, it doesn't seem to
    // work like that in practice. last_pid is not set in all cases (refer to
    // kip-360 for details) whereas we want to cleanup older epochs state in all
    // cases, hence a separate rolled_pid was added. This is definitely not
    // ideal, probably needs a closer look.
    model::producer_identity rolled_pid = tx.pid;
    model::producer_identity last_pid = model::no_pid;

    if (expected_pid == model::no_pid) {
        if (is_max_epoch(tx.pid.epoch)) {
            vlog(txlog.trace, "[tx_id={}] allocating new producer id", tx_id);
            allocate_id_reply pid_reply
              = co_await _id_allocator_frontend.local().allocate_id(timeout);
            reply.pid = model::producer_identity{pid_reply.id, 0};
        } else {
            reply.pid = model::producer_identity::with_next_epoch(tx.pid);
        }
    } else {
        if (tx.last_pid == expected_pid) {
            last_pid = tx.last_pid;
        } else if (tx.pid == expected_pid) {
            if (is_max_epoch(tx.pid.epoch)) {
                allocate_id_reply pid_reply
                  = co_await _id_allocator_frontend.local().allocate_id(
                    timeout);
                reply.pid = model::producer_identity{pid_reply.id, 0};
            } else {
                reply.pid = model::producer_identity::with_next_epoch(tx.pid);
            }
            last_pid = tx.pid;
        } else {
            co_return init_tm_tx_reply{tx::errc::invalid_producer_epoch};
        }
    }
    vlog(
      txlog.trace,
      "[tx_id={}] re-registering producer with new pid: {}, last_pid: {}",
      tx.id,
      reply.pid,
      last_pid);
    auto op_status = co_await stm->re_register_producer(
      term, tx.id, transaction_timeout_ms, reply.pid, last_pid, rolled_pid);
    if (op_status == tm_stm::op_status::success) {
        reply.ec = tx::errc::none;
    } else if (op_status == tm_stm::op_status::conflict) {
        reply.ec = tx::errc::conflict;
    } else if (op_status == tm_stm::op_status::timeout) {
        reply.ec = tx::errc::timeout;
    } else {
        vlog(
          txlog.warn,
          "[tx_id={}] error re-registering a producer {} - {}",
          tx.id,
          reply.pid,
          op_status);
        reply.ec = tx::errc::invalid_txn_state;
    }
    co_return reply;
}

ss::future<add_paritions_tx_reply> tx_gateway_frontend::add_partition_to_tx(
  add_paritions_tx_request request, model::timeout_clock::duration timeout) {
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
    if (leader != _controller->self()) {
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
      -> ss::future<add_paritions_tx_reply> {
          return ss::with_gate(
            self._gate,
            [request = std::move(request), timeout, tm, &self]()
              -> ss::future<add_paritions_tx_reply> {
                return self.with_stm(
                  tm,
                  [request = std::move(request), timeout, &self](
                    checked<ss::shared_ptr<tm_stm>, tx::errc> r) {
                      if (!r) {
                          return ss::make_ready_future<add_paritions_tx_reply>(
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

ss::future<add_paritions_tx_reply> tx_gateway_frontend::do_add_partition_to_tx(
  ss::shared_ptr<tm_stm> stm,
  add_paritions_tx_request request,
  model::timeout_clock::duration timeout) {
    model::producer_identity pid{request.producer_id, request.producer_epoch};

    auto term_opt = co_await stm->sync();
    if (!term_opt.has_value()) {
        if (term_opt.error() == tm_stm::op_status::not_leader) {
            co_return make_add_partitions_error_response(
              request, tx::errc::not_coordinator);
        }
        co_return make_add_partitions_error_response(
          request, tx::errc::coordinator_not_available);
    }
    auto term = term_opt.value();

    auto r = co_await get_ongoing_tx(
      term, stm, pid, request.transactional_id, timeout);

    if (!r.has_value()) {
        vlog(
          txlog.trace,
          "[tx_id={}] error getting ongoing transaction for pid: {} - {}",
          request.transactional_id,
          pid,
          r.error());
        co_return make_add_partitions_error_response(request, r.error());
    }
    auto tx = r.value();

    add_paritions_tx_reply response;

    std::vector<model::ntp> new_partitions;

    for (auto& req_topic : request.topics) {
        add_paritions_tx_reply::topic_result res_topic;
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
                add_paritions_tx_reply::partition_result res_partition;
                res_partition.partition_index = req_partition;
                res_partition.error_code = tx::errc::none;
                res_topic.results.push_back(res_partition);
            } else if (
              disabled_set && disabled_set->is_disabled(req_partition)) {
                add_paritions_tx_reply::partition_result res_partition;
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
    std::vector<begin_tx_reply> brs;
    auto retries = _metadata_dissemination_retries;
    auto delay_ms = _metadata_dissemination_retry_delay_ms;

    while (0 < retries--) {
        partitions.clear();
        brs.clear();
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
        brs = co_await when_all_succeed(bfs.begin(), bfs.end());
        for (auto& br : brs) {
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
                               || br.ec == tx::errc::partition_not_exists;
            should_abort = should_abort
                           || (br.ec != tx::errc::none && !expected_ec);
            should_retry = should_retry || expected_ec;

            if (br.ec == tx::errc::none) {
                partitions.push_back(tx_metadata::tx_partition{
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

    auto status = co_await stm->add_partitions(term, tx.id, partitions);
    auto has_added = status == tm_stm::op_status::success;
    if (!has_added) {
        vlog(
          txlog.warn,
          "[tx_id={}] adding partitions failed pid: {} - {}",
          request.transactional_id,
          pid,
          status);
    }
    for (auto& br : brs) {
        auto topic_it = std::find_if(
          response.results.begin(),
          response.results.end(),
          [&br](const auto& r) { return r.name == br.ntp.tp.topic(); });

        add_paritions_tx_reply::partition_result res_partition;
        res_partition.partition_index = br.ntp.tp.partition;
        if (has_added && br.ec == tx::errc::none) {
            res_partition.error_code = tx::errc::none;
        } else {
            if (br.ec != tx::errc::none) {
                vlog(
                  txlog.warn,
                  "[tx_id={}] begin_tx request for pid: {} at ntp: {} failed - "
                  "{}",
                  request.transactional_id,
                  pid,
                  br.ntp,
                  br.ec);
            }
            res_partition.error_code = tx::errc::invalid_txn_state;
        }
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
    if (leader != _controller->self()) {
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
            [request = std::move(request), timeout, tm, &self]()
              -> ss::future<add_offsets_tx_reply> {
                return self.with_stm(
                  tm,
                  [request = std::move(request), timeout, &self](
                    checked<ss::shared_ptr<tm_stm>, tx::errc> r) {
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

    auto term_opt = co_await stm->sync();
    if (!term_opt.has_value()) {
        if (term_opt.error() == tm_stm::op_status::not_leader) {
            co_return add_offsets_tx_reply{
              .error_code = tx::errc::not_coordinator};
        }
        co_return add_offsets_tx_reply{
          .error_code = tx::errc::coordinator_not_available};
    }
    auto term = term_opt.value();

    auto r = co_await get_ongoing_tx(
      term, stm, pid, request.transactional_id, timeout);
    if (!r.has_value()) {
        vlog(
          txlog.warn,
          "[tx_id={}] error getting ongoing transaction for pid: {} - {}",
          request.transactional_id,
          pid,
          r.error());
        co_return add_offsets_tx_reply{.error_code = r.error()};
    }
    auto tx = r.value();

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
      term, tx.id, request.group_id, group_info.etag);
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
    if (leader != _controller->self()) {
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
            [request = std::move(request), timeout, tm, &self]()
              -> ss::future<end_tx_reply> {
                return self.with_stm(
                  tm,
                  [request = std::move(request), timeout, &self](
                    checked<ss::shared_ptr<tm_stm>, tx::errc> r) {
                      return self.do_end_txn(r, std::move(request), timeout);
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

ss::future<checked<cluster::tx_metadata, tx::errc>>
tx_gateway_frontend::do_end_txn(
  end_tx_request request,
  ss::shared_ptr<cluster::tm_stm> stm,
  model::timeout_clock::duration timeout,
  ss::lw_shared_ptr<available_promise<tx::errc>> outcome) {
    model::producer_identity pid{request.producer_id, request.producer_epoch};
    checked<model::term_id, tm_stm::op_status> term_opt
      = tm_stm::op_status::unknown;
    try {
        term_opt = co_await stm->sync();
    } catch (...) {
        vlog(
          txlog.warn,
          "[tx_id={}] sync on end_txn failed pid: {} - {}",
          request.transactional_id,
          pid,
          std::current_exception());
        outcome->set_value(tx::errc::invalid_txn_state);
        throw;
    }
    if (!term_opt.has_value()) {
        if (term_opt.error() == tm_stm::op_status::not_leader) {
            outcome->set_value(tx::errc::not_coordinator);
            co_return tx::errc::not_coordinator;
        }
        vlog(
          txlog.warn,
          "[tx_id={}] sync on end_txn failed pid: {} - {}",
          request.transactional_id,
          pid,
          std::current_exception());
        outcome->set_value(tx::errc::invalid_txn_state);
        co_return tx::errc::invalid_txn_state;
    }
    auto term = term_opt.value();

    auto r0 = co_await get_latest_tx(
      term, stm, pid, request.transactional_id, timeout);
    if (!r0.has_value()) {
        auto err = r0.error();
        if (err == tx::errc::tx_not_found) {
            vlog(
              txlog.warn,
              "[tx_id={}] can't find an ongoing transaction pid: {} to commit "
              "/ "
              "abort",
              request.transactional_id,
              pid);
            err = tx::errc::unknown_server_error;
        }
        outcome->set_value(err);
        co_return err;
    }
    auto tx = r0.value();

    checked<cluster::tx_metadata, tx::errc> r(tx::errc::unknown_server_error);
    if (request.committed) {
        if (tx.status == tx_status::killed) {
            vlog(
              txlog.warn,
              "[tx_id={}] can't commit an expired transaction pid: {} etag: {} "
              "tx_seq: {} in term: {}",
              tx.id,
              tx.pid,
              tx.etag,
              tx.tx_seq,
              term);
            outcome->set_value(tx::errc::fenced);
            co_return tx::errc::fenced;
        }
        bool is_status_ok = tx.status == tx_status::ongoing
                            || tx.status == tx_status::prepared;
        if (is_status_ok) {
            try {
                r = co_await do_commit_tm_tx(term, stm, tx, timeout, outcome);
            } catch (...) {
                vlog(
                  txlog.error,
                  "[tx_id={}] error committing transaction pid: {} etag: {} "
                  "tx_seq: {} - {}",
                  tx.id,
                  tx.pid,
                  tx.etag,
                  tx.tx_seq,
                  std::current_exception());
                if (!outcome->available()) {
                    outcome->set_value(tx::errc::unknown_server_error);
                    throw;
                }
            }
        } else {
            vlog(
              txlog.warn,
              "[tx_id={}] an ongoing transaction pid: {} etag: {} tx_seq: {} "
              "in term: {} has an unexpected status: {}",
              tx.id,
              tx.pid,
              tx.etag,
              tx.tx_seq,
              term,
              tx.status);
            outcome->set_value(tx::errc::invalid_txn_state);
            co_return tx::errc::invalid_txn_state;
        }
    } else {
        if (tx.status == tx_status::killed) {
            vlog(
              txlog.warn,
              "[tx_id={}] can't abort an expired transaction pid: {} etag: {} "
              "tx_seq: {} in term: {}",
              tx.id,
              tx.pid,
              tx.etag,
              tx.tx_seq,
              term);
            outcome->set_value(tx::errc::fenced);
            co_return tx::errc::fenced;
        }
        try {
            r = co_await do_abort_tm_tx(term, stm, tx, timeout);
        } catch (...) {
            vlog(
              txlog.error,
              "[tx_id={}] error aborting transaction pid: {} etag: {} tx_seq: "
              "{} - {}",
              tx.id,
              tx.pid,
              tx.etag,
              tx.tx_seq,
              std::current_exception());
            outcome->set_value(tx::errc::unknown_server_error);
            throw;
        }
        if (r.has_value()) {
            outcome->set_value(tx::errc::none);
        } else {
            auto ec = r.error();
            vlog(
              txlog.error,
              "[tx_id={}] error aborting transaction pid: {} etag: {} tx_seq: "
              "{} - {}",
              tx.id,
              tx.pid,
              tx.etag,
              tx.tx_seq,
              ec);
            outcome->set_value(ec);
        }
    }
    // starting from this point we don't need to set outcome on return because
    // we shifted this responsibility to do_commit_tm_tx
    if (!r.has_value()) {
        co_return r;
    }
    tx = r.value();

    auto ongoing_tx = co_await stm->mark_tx_ongoing(term, tx.id);
    if (!ongoing_tx.has_value()) {
        vlog(
          txlog.warn,
          "[tx_id={}] can't mark transaction as ongoing - {}",
          tx.id,
          ongoing_tx.error());
        co_return tx::errc::unknown_server_error;
    }
    co_return ongoing_tx.value();
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

ss::future<checked<cluster::tx_metadata, tx::errc>>
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

    if (tx.status == tx_status::aborting) {
        // retry of the abort
        co_return co_await reabort_tm_tx(stm, expected_term, tx, timeout);
    }

    if (tx.status != tx_status::ongoing && tx.status != tx_status::preparing) {
        vlog(
          txlog.warn,
          "[tx_id={}] abort encountered a transaction with unexpected status: "
          "{},  pid: {}, etag: {} tx_seq: {} in term: {}",
          tx.id,
          tx.status,
          tx.pid,
          tx.etag,
          tx.tx_seq,
          expected_term);
        co_return tx::errc::invalid_txn_state;
    }

    auto changed_tx = co_await stm->mark_tx_aborting(expected_term, tx.id);
    if (!changed_tx.has_value()) {
        if (is_fetch_tx_supported()) {
            vlog(
              txlog.trace,
              "[tx_id={}] aborting transaction pid: {} etag: {} tx_seq: {} "
              "status: {} in term: {} failed - {}",
              tx.id,
              tx.pid,
              tx.etag,
              tx.tx_seq,
              tx.status,
              expected_term,
              changed_tx.error());
            co_return tx::errc::coordinator_not_available;
        } else {
            if (changed_tx.error() == tm_stm::op_status::not_leader) {
                co_return tx::errc::not_coordinator;
            }
            if (changed_tx.error() == tm_stm::op_status::timeout) {
                co_return tx::errc::timeout;
            }
            co_return tx::errc::unknown_server_error;
        }
    }
    tx = changed_tx.value();

    co_return co_await reabort_tm_tx(stm, expected_term, tx, timeout);
}

ss::future<checked<cluster::tx_metadata, tx::errc>>
tx_gateway_frontend::do_commit_tm_tx(
  model::term_id expected_term,
  ss::shared_ptr<cluster::tm_stm> stm,
  cluster::tx_metadata tx,
  model::timeout_clock::duration timeout,
  ss::lw_shared_ptr<available_promise<tx::errc>> outcome) {
    if (!stm->is_actual_term(expected_term)) {
        outcome->set_value(tx::errc::not_coordinator);
        co_return tx::errc::not_coordinator;
    }

    if (tx.status == tx_status::prepared) {
        outcome->set_value(tx::errc::none);
        co_return co_await recommit_tm_tx(stm, expected_term, tx, timeout);
    }

    vlog(
      txlog.trace,
      "[tx_id={}] marking transaction pid: {} tx_seq: {} etag: {} as prepared "
      "in term: {}",
      tx.id,
      tx.pid,
      tx.tx_seq,
      tx.etag,
      expected_term);
    auto changed_tx = co_await stm->mark_tx_prepared(expected_term, tx.id);
    if (!changed_tx.has_value()) {
        vlog(
          txlog.trace,
          "[tx_id={}] error committing transaction pid: {} etag: {} tx_seq: {} "
          "- {}",
          tx.id,
          tx.pid,
          tx.etag,
          tx.tx_seq,
          changed_tx.error());

        if (!is_fetch_tx_supported()) {
            if (changed_tx.error() == tm_stm::op_status::not_leader) {
                outcome->set_value(tx::errc::not_coordinator);
                co_return tx::errc::not_coordinator;
            }

            if (changed_tx.error() == tm_stm::op_status::timeout) {
                outcome->set_value(tx::errc::timeout);
                co_return tx::errc::timeout;
            }
            outcome->set_value(tx::errc::unknown_server_error);
            co_return tx::errc::unknown_server_error;
        }

        outcome->set_value(tx::errc::not_coordinator);
        co_return tx::errc::not_coordinator;
    }
    tx = changed_tx.value();

    // We can reduce the number of disk operation if we will not write
    // preparing state on disk. But after it we should ans to client when we
    // sure that tx will be recommited after fail. We can guarantee it only
    // if we ans after marking tx prepared. Becase after fail tx will be
    // recommited again and client will see expected bechavior.
    // Also we do not need to support old bechavior with feature flag, because
    // now we will ans client later than in old versions. So we do not break
    // anything
    outcome->set_value(tx::errc::none);
    co_return co_await recommit_tm_tx(stm, expected_term, tx, timeout);
}

ss::future<checked<cluster::tx_metadata, tx::errc>>
tx_gateway_frontend::recommit_tm_tx(
  ss::shared_ptr<tm_stm> stm,
  model::term_id expected_term,
  tx_metadata tx,
  model::timeout_clock::duration timeout) {
    auto retries = _metadata_dissemination_retries;
    auto delay_ms = _metadata_dissemination_retry_delay_ms;
    auto done = false;

    while (0 < retries--) {
        std::vector<ss::future<commit_group_tx_reply>> gfs;
        gfs.reserve(tx.groups.size());
        for (auto group : tx.groups) {
            gfs.push_back(_rm_group_proxy->commit_group_tx(
              group.group_id, tx.pid, tx.tx_seq, timeout));
        }
        std::vector<ss::future<commit_tx_reply>> cfs;
        cfs.reserve(tx.partitions.size());
        for (auto rm : tx.partitions) {
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

ss::future<checked<cluster::tx_metadata, tx::errc>>
tx_gateway_frontend::reabort_tm_tx(
  ss::shared_ptr<tm_stm> stm,
  model::term_id expected_term,
  tx_metadata tx,
  model::timeout_clock::duration timeout) {
    auto retries = _metadata_dissemination_retries;
    auto delay_ms = _metadata_dissemination_retry_delay_ms;
    auto done = false;
    while (0 < retries--) {
        std::vector<ss::future<abort_tx_reply>> pfs;
        pfs.reserve(tx.partitions.size());
        for (auto rm : tx.partitions) {
            pfs.push_back(_rm_partition_frontend.local().abort_tx(
              rm.ntp, tx.pid, tx.tx_seq, timeout));
        }
        std::vector<ss::future<abort_group_tx_reply>> gfs;
        gfs.reserve(tx.groups.size());
        for (auto group : tx.groups) {
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

ss::future<checked<tx_metadata, tx::errc>> tx_gateway_frontend::bump_etag(
  model::term_id term,
  ss::shared_ptr<cluster::tm_stm> stm,
  cluster::tx_metadata tx,
  model::timeout_clock::duration timeout) {
    checked<tx_metadata, tx::errc> r1(tx::errc::unknown_server_error);
    if (tx.status == tx_status::prepared) {
        r1 = co_await recommit_tm_tx(stm, term, tx, timeout);
    } else if (
      tx.status == tx_status::aborting || tx.status == tx_status::killed) {
        r1 = co_await reabort_tm_tx(stm, term, tx, timeout);
    } else {
        r1 = tx;
    }
    if (!r1.has_value()) {
        // we invoke bump when we have the most up-to-date info
        // so reject isn't possible
        vlog(
          txlog.error,
          "[tx_id={}] error rolling previous transaction with status: {} - "
          "{}",
          tx.id,
          tx.status,
          r1.error());
        if (r1.error() == tx::errc::request_rejected) {
            co_return tx::errc::invalid_txn_state;
        }

        // until any decision is made it's ok to ask user retry
        co_return tx::errc::not_coordinator;
    }

    if (tx.etag != term) {
        tx.etag = term;
        auto r2 = co_await stm->update_tx(tx, term);
        if (!r2) {
            vlog(
              txlog.error,
              "[tx_id={}] error bumping etag - {}",
              tx.id,
              r2.error());
            co_return tx::errc::not_coordinator;
        }
        co_return r2.value();
    }

    co_return tx;
}

ss::future<checked<tx_metadata, tx::errc>> tx_gateway_frontend::forget_tx(
  model::term_id term,
  ss::shared_ptr<cluster::tm_stm> stm,
  cluster::tx_metadata tx,
  model::timeout_clock::duration timeout) {
    checked<tx_metadata, tx::errc> r1(tx::errc::unknown_server_error);
    if (tx.status == tx_status::prepared) {
        r1 = co_await recommit_tm_tx(stm, term, tx, timeout);
    } else if (tx.status == tx_status::aborting) {
        r1 = co_await reabort_tm_tx(stm, term, tx, timeout);
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

ss::future<checked<tx_metadata, tx::errc>> tx_gateway_frontend::get_tx(
  model::term_id term,
  ss::shared_ptr<tm_stm> stm,
  kafka::transactional_id tid,
  model::timeout_clock::duration timeout) {
    auto tx_opt = co_await stm->get_tx(tid);
    if (!tx_opt.has_value()) {
        if (tx_opt.error() == tm_stm::op_status::not_found) {
            co_return tx::errc::tx_not_found;
        } else {
            vlog(
              txlog.warn,
              "[tx_id={}] error getting transaction - {}",
              tid,
              tx_opt.error());
            // any error on lookin up a tx is a retriable error
            co_return tx::errc::not_coordinator;
        }
    }

    auto tx = tx_opt.value();

    if (tx.transferring) {
        tx_opt = co_await stm->reset_transferring(term, tid);
        if (!tx_opt.has_value()) {
            vlog(
              txlog.warn,
              "[tx_id={}] error rehydrating transaction info - {}",
              tid,
              tx_opt.error());
            // any error on lookin up a tx is a retriable error
            co_return tx::errc::not_coordinator;
        }
        tx = tx_opt.value();
    }

    if (term == tx.etag) {
        // we have the most up-to-date info => reject can't happen
        co_return co_await bump_etag(term, stm, tx, timeout);
    }

    if (tx.etag > term) {
        // tx was written by a future leader meaning current
        // node can't be a leader
        co_return tx::errc::not_coordinator;
    }

    // tombstone & killed are terminal tx states
    // preparing isn't supported since ga
    if (
      tx.status == tx_status::tombstone || tx.status == tx_status::preparing
      || tx.status == tx_status::killed) {
        // tombstone & killed are terminal so we know that we're
        // we have the most up-to-date info => reject can't happen
        co_return co_await bump_etag(term, stm, tx, timeout);
    }

    if (!is_fetch_tx_supported()) {
        checked<tx_metadata, tx::errc> r1(tx);
        if (tx.status == tx_status::prepared) {
            r1 = co_await recommit_tm_tx(stm, term, tx, timeout);
        } else if (tx.status == tx_status::aborting) {
            r1 = co_await reabort_tm_tx(stm, term, tx, timeout);
        } else if (tx.status == tx_status::killed) {
            r1 = co_await reabort_tm_tx(stm, term, tx, timeout);
        }
        if (!r1.has_value()) {
            vlog(
              txlog.warn,
              "[tx_id={}] error rolling previous transaction id with status: "
              "{} - {}",
              tid,
              tx.status,
              r1.error());

            // until any decision is made it's ok to ask user retry
            co_return tx::errc::not_coordinator;
        }
        co_return r1.value();
    }

    auto r1 = co_await fetch_tx(tx.id, tx.etag, stm->get_partition());
    if (!r1) {
        vlog(
          txlog.warn,
          "[tx_id={}] Can't fetch cached state of the persisted tx: {}, true "
          "state is unknown, terminating session to avoid data loss",
          tx.id,
          tx);
        co_return co_await forget_tx(term, stm, tx, timeout);
    }

    auto old_tx = r1.value();

    if (tx.status == tx_status::ready) {
        if (old_tx.tx_seq < tx.tx_seq) {
            // previous leader attempted to write ready;
            // the write erred but passed; tx is the freshest
            // state
            old_tx = tx;
        } else {
            if (old_tx.pid != tx.pid) {
                vlog(
                  txlog.warn,
                  "[tx_id={}] A cached tx (tx: {} pid: {} etag: {} tx_seq: {} "
                  "status: {}) of the persisted (pid: {} etag:  {} tx_seq: {} "
                  "status: {}) should have the same pid, terminating session",
                  tx.id,
                  old_tx.id,
                  old_tx.pid,
                  old_tx.etag,
                  old_tx.tx_seq,
                  old_tx.status,
                  tx.pid,
                  tx.etag,
                  tx.tx_seq,
                  tx.status);
                co_return co_await forget_tx(term, stm, tx, timeout);
            }
            if (old_tx.tx_seq > tx.tx_seq) {
                // old leader has fresher data which aren't in the log
                // we don't save ongoing to log so it must be it
                if (old_tx.status != tx_status::ongoing) {
                    vlog(
                      txlog.warn,
                      "[tx_id={}] A cached tx (tx: {} pid: {} etag: {} tx_seq: "
                      "{} status: {}) of the persisted (pid: {} etag:  {} "
                      "tx_seq: {} status: {}) may be in the tx seq future only "
                      "if it's ongoing, terminating session",
                      tx.id,
                      old_tx.id,
                      old_tx.pid,
                      old_tx.etag,
                      old_tx.tx_seq,
                      old_tx.status,
                      tx.pid,
                      tx.etag,
                      tx.tx_seq,
                      tx.status);
                    co_return co_await forget_tx(term, stm, tx, timeout);
                }
            } else {
                if (old_tx.status != tx.status) {
                    vlog(
                      txlog.warn,
                      "[tx_id={}] A cached tx (tx: {} pid: {} etag: {} tx_seq: "
                      "{} status: {}) of the persisted (pid: {} etag:  {} "
                      "tx_seq: {} status: {}) with the same tx seq must have "
                      "same status, terminating session",
                      tx.id,
                      old_tx.id,
                      old_tx.pid,
                      old_tx.etag,
                      old_tx.tx_seq,
                      old_tx.status,
                      tx.pid,
                      tx.etag,
                      tx.tx_seq,
                      tx.status);

                    co_return co_await forget_tx(term, stm, tx, timeout);
                }
            }
        }
        // retry & ongoing don't retry => reject isn't expected
        co_return co_await bump_etag(term, stm, old_tx, timeout);
    }

    if (old_tx.pid != tx.pid) {
        vlog(
          txlog.warn,
          "[tx_id={}] A cached tx (tx: {} pid: {} etag: {} tx_seq: "
          "{} status: {}) of the persisted (pid: {} etag:  {} "
          "tx_seq: {} status: {}) should have same pid, terminating session",
          tx.id,
          old_tx.id,
          old_tx.pid,
          old_tx.etag,
          old_tx.tx_seq,
          old_tx.status,
          tx.pid,
          tx.etag,
          tx.tx_seq,
          tx.status);

        co_return co_await forget_tx(term, stm, tx, timeout);
    }

    if (tx.status == tx_status::aborting) {
        if (old_tx.tx_seq < tx.tx_seq) {
            // previous leader attempted to write abort;
            // the write erred but passed; tx is the freshest
            // state
            old_tx = tx;
        } else {
            if (old_tx.tx_seq > tx.tx_seq) {
                // old leader has fresher data which aren't in the log
                // we don't save ongoing to log so it must be it
                if (old_tx.status != tx_status::ongoing) {
                    vlog(
                      txlog.warn,
                      "[tx_id={}] A cached tx (tx: {} pid: {} etag: {} tx_seq: "
                      "{} status: {}) of the persisted (pid: {} etag:  {} "
                      "tx_seq: {} status: {}) may be in the tx seq future only "
                      "if it's ongoing, terminating session",
                      tx.id,
                      old_tx.id,
                      old_tx.pid,
                      old_tx.etag,
                      old_tx.tx_seq,
                      old_tx.status,
                      tx.pid,
                      tx.etag,
                      tx.tx_seq,
                      tx.status);
                    co_return co_await forget_tx(term, stm, tx, timeout);
                }
            } else {
                // either writing aborting erred but passed (same txseq, status
                // is ongoing) or it passed (same txseq, status is aborting)
                if (
                  old_tx.status != tx_status::ongoing
                  && old_tx.status != tx_status::aborting) {
                    vlog(
                      txlog.warn,
                      "[tx_id={}] A cached tx (tx: {} pid: {} etag: {} tx_seq: "
                      "{} status: {}) of the persisted (pid: {} etag:  {} "
                      "tx_seq: {} status: {}) with the same tx seq must have "
                      "either ongoing or aborting status; terminating session",
                      tx.id,
                      old_tx.id,
                      old_tx.pid,
                      old_tx.etag,
                      old_tx.tx_seq,
                      old_tx.status,
                      tx.pid,
                      tx.etag,
                      tx.tx_seq,
                      tx.status);
                    co_return co_await forget_tx(term, stm, tx, timeout);
                }
                old_tx = tx;
            }
        }
        // we have the most up-to-date info => reject can't happen
        co_return co_await bump_etag(term, stm, old_tx, timeout);
    }

    if (tx.status == tx_status::prepared) {
        if (old_tx.tx_seq < tx.tx_seq) {
            // previous leader attempted to write prepared;
            // the write erred but passed; tx is the freshest
            // state
            old_tx = tx;
        } else {
            if (old_tx.tx_seq > tx.tx_seq) {
                // old leader has fresher data which aren't in the log
                // we don't save ongoing to log so it must be it
                if (old_tx.status != tx_status::ongoing) {
                    vlog(
                      txlog.warn,
                      "[tx_id={}] A cached tx (tx: {} pid: {} etag: {} tx_seq: "
                      "{} status: {}) of the persisted (pid: {} etag:  {} "
                      "tx_seq: {} status: {}) may be in the tx seq future only "
                      "if it's ongoing, terminating session",
                      tx.id,
                      old_tx.id,
                      old_tx.pid,
                      old_tx.etag,
                      old_tx.tx_seq,
                      old_tx.status,
                      tx.pid,
                      tx.etag,
                      tx.tx_seq,
                      tx.status);
                    co_return co_await forget_tx(term, stm, tx, timeout);
                }
            } else {
                // either writing prepared erred but passed (same txseq, status
                // is ongoing) or it passed (same txseq, status is prepared)
                if (
                  old_tx.status != tx_status::ongoing
                  && old_tx.status != tx_status::prepared) {
                    vlog(
                      txlog.warn,
                      "[tx_id={}] A cached tx (tx: {} pid: {} etag: {} tx_seq: "
                      "{} status: {}) of the persisted (pid: {} etag:  {} "
                      "tx_seq: {} status: {}) with the same tx seq must have "
                      "either ongoing or prepared status, terminating session",
                      tx.id,
                      old_tx.id,
                      old_tx.pid,
                      old_tx.etag,
                      old_tx.tx_seq,
                      old_tx.status,
                      tx.pid,
                      tx.etag,
                      tx.tx_seq,
                      tx.status);
                    co_return co_await forget_tx(term, stm, tx, timeout);
                }
                old_tx = tx;
            }
        }
        // we have the most up-to-date info => reject can't happen
        co_return co_await bump_etag(term, stm, old_tx, timeout);
    }

    if (tx.status == tx_status::ongoing) {
        if (old_tx.tx_seq != tx.tx_seq || old_tx.status != tx.status) {
            // when redpanda saves ongoing to disk it also
            // keeps it in memory so fetching should return
            // the same
            vlog(
              txlog.warn,
              "[tx_id={}] A cached tx (tx: {} pid: {} etag: {} tx_seq: "
              "{} status: {}) of the persisted (pid: {} etag:  {} "
              "tx_seq: {} status: {}) should have the same tx_seq and status, "
              "terminating session",
              tx.id,
              old_tx.id,
              old_tx.pid,
              old_tx.etag,
              old_tx.tx_seq,
              old_tx.status,
              tx.pid,
              tx.etag,
              tx.tx_seq,
              tx.status);
            co_return co_await forget_tx(term, stm, tx, timeout);
        }
        // an old leader could have updated a tx after saving it so picking
        // its version;
        // we have the most up-to-date info => reject can't happen
        co_return co_await bump_etag(term, stm, old_tx, timeout);
    }

    vlog(
      txlog.warn,
      "[tx_id={}] A persisted tx (pid: {} etag: {} tx_seq: {} status: {}) has "
      "unexpected status",
      tx.id,
      tx.pid,
      tx.etag,
      tx.tx_seq,
      tx.status);

    co_return tx::errc::unknown_server_error;
}

ss::future<checked<tx_metadata, tx::errc>> tx_gateway_frontend::get_latest_tx(
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
    auto r0 = co_await get_tx(term, stm, tx_id, timeout);
    if (!r0.has_value()) {
        co_return r0.error();
    }

    auto tx = r0.value();

    if (term != tx.etag) {
        // very unlikely situation happens only when !is_fetch_tx_supported()
        // all we can do is to give up
        co_return tx::errc::unknown_server_error;
    }

    if (tx.pid != pid) {
        if (tx.pid.id == pid.id && tx.pid.epoch > pid.epoch) {
            vlog(
              txlog.info,
              "[tx_id={}] producer {} is fenced of by {}",
              tx_id,
              pid,
              tx.pid);
            co_return tx::errc::fenced;
        }
        vlog(
          txlog.info,
          "[tx_id={}] transaction is mapped to {} not {}",
          tx_id,
          tx.pid,
          pid);

        co_return tx::errc::invalid_producer_id_mapping;
    }

    co_return tx;
}

ss::future<checked<tx_metadata, tx::errc>> tx_gateway_frontend::get_ongoing_tx(
  model::term_id term,
  ss::shared_ptr<tm_stm> stm,
  model::producer_identity pid,
  kafka::transactional_id tx_id,
  model::timeout_clock::duration timeout) {
    vlog(
      txlog.trace,
      "[tx_id={}] Getting ongoing transactions for pid: {}",
      tx_id,
      pid);
    auto r0 = co_await get_latest_tx(term, stm, pid, tx_id, timeout);
    if (!r0.has_value()) {
        if (r0.error() == tx::errc::tx_not_found) {
            // tx doesn't exist when it was expected
            // if tx doesn't exist then the tx.id -> pid mapping doesn't
            // exist either meaning any provided mapping is wrong
            co_return tx::errc::invalid_producer_id_mapping;
        }
        co_return r0.error();
    }
    auto tx = r0.value();

    if (tx.status == tx_status::ongoing) {
        co_return tx;
    } else if (tx.status == tx_status::preparing) {
        // a producer can see a transaction with the same pid and in a
        // preparing state only if it attempted a commit, the commit
        // failed and then the producer ignored it and tried to start
        // another transaction.
        //
        // it violates the docs, the producer is expected to call abort
        // https://kafka.apache.org/23/javadoc/org/apache/kafka/clients/producer/KafkaProducer.html
        co_return tx::errc::invalid_txn_state;
    } else if (tx.status == tx_status::killed) {
        // a tx was timed out, can't treat it as ::aborting because
        // from the client perspective it will look like a tx wasn't
        // failed at all but in fact the second part of the tx will
        // start a new transactions
        vlog(
          txlog.warn,
          "[tx_id={} can't modify an expired transaction pid: {} etag: {} "
          "tx_seq: {} in term: {}",
          tx.id,
          tx.pid,
          tx.etag,
          tx.tx_seq,
          term);
        co_return tx::errc::fenced;
    } else {
        if (
          tx.status == tx_status::prepared
          || tx.status == tx_status::aborting) {
            // previous commit was acked to the client
            // the the commit / abort failed on recommit or the node crushed
            // client retries new implicit tx on the new leader
            // and encounters half committed / aborted tx
            // get_tx is correcting so it was already rolled forward
            // we just need to implicitly start new ongoing tx
        } else if (tx.status != tx_status::ready) {
            vassert(false, "unexpected tx status {}", tx.status);
        }
        auto ongoing_tx = co_await stm->mark_tx_ongoing(term, tx.id);
        if (!ongoing_tx.has_value()) {
            vlog(
              txlog.warn,
              "[tx_id={}] failed resetting ongoing transaction pid: {} etag: "
              "{} tx_seq: {} term: {}",
              tx.id,
              tx.pid,
              tx.etag,
              tx.tx_seq,
              term);

            co_return tx::errc::invalid_txn_state;
        }
        co_return ongoing_tx.value();
    }
}

void tx_gateway_frontend::expire_old_txs() {
    ssx::spawn_with_gate(_gate, [this] {
        auto ntp_meta = _metadata_cache.local().get_topic_metadata(
          model::tx_manager_nt);
        if (!ntp_meta) {
            vlog(
              txlog.error,
              "Topic {} doesn't exist in metadata cache,",
              model::tx_manager_nt);
            return ss::now();
        }

        std::vector<model::partition_id> partitions;
        partitions.reserve(ntp_meta->get_assignments().size());
        for (auto& pa : ntp_meta->get_assignments()) {
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

ss::future<> tx_gateway_frontend::expire_old_txs(model::ntp tx_ntp) {
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
    for (auto tx_id : tx_ids) {
        co_await expire_old_tx(stm, tx_id);
    }
}

ss::future<> tx_gateway_frontend::expire_old_tx(
  ss::shared_ptr<tm_stm> stm, kafka::transactional_id tx_id) {
    auto units = co_await stm->lock_tx(tx_id, "expire_old_tx");

    auto term_opt = co_await stm->sync();
    if (!term_opt.has_value()) {
        if (term_opt.error() == tm_stm::op_status::not_leader) {
            vlog(
              txlog.trace,
              "[tx_id={}] this node is not current coordinator",
              tx_id);
        }
        vlog(
          txlog.warn,
          "[tx_id={}] error syncing state machine - {}",
          tx_id,
          term_opt.error());
        co_return;
    }

    auto term = term_opt.value();
    auto timeout = config::shard_local_cfg().create_topic_timeout_ms();

    auto tx_maybe = co_await get_tx(term, stm, tx_id, timeout);
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

    checked<tx_metadata, tx::errc> r(tx);

    vlog(
      txlog.trace,
      "[tx_id={}] attempting to expire transaction pid: {} tx_seq: {} status: "
      "{}",
      tx.id,
      tx.pid,
      tx.tx_seq,
      tx.status);

    if (tx.status == tx_status::ongoing) {
        r = co_await do_abort_tm_tx(term, stm, tx, timeout);
    } else if (tx.status == tx_status::preparing) {
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
          model::ntp tx_manager_ntp{
            model::tx_manager_nt.ns, model::tx_manager_nt.tp, tx_partition};
          auto partition = self._partition_manager.local().get(tx_manager_ntp);
          if (!partition) {
              vlog(txlog.warn, "can't get partition by {} ntp", tx_manager_ntp);
              return ss::make_ready_future<return_all_txs_res>(
                return_all_txs_res{tx::errc::partition_not_found});
          }

          auto stm = partition->tm_stm();

          if (!stm) {
              vlog(
                txlog.error,
                "can't get tm stm of the {}' partition",
                tx_manager_ntp);
              return ss::make_ready_future<return_all_txs_res>(
                return_all_txs_res{tx::errc::unknown_server_error});
          }

          auto gate_lock = self._gate.hold();
          return stm->read_lock()
            .then([stm](ss::basic_rwlock<>::holder unit) {
                return stm->get_all_transactions()
                  .then(
                    [](tm_stm::get_txs_result res)
                      -> ss::future<return_all_txs_res> {
                        if (!res.has_value()) {
                            if (res.error() == tm_stm::op_status::not_leader) {
                                return ss::make_ready_future<
                                  return_all_txs_res>(return_all_txs_res{
                                  tx::errc::not_coordinator});
                            }
                            return ss::make_ready_future<return_all_txs_res>(
                              return_all_txs_res{
                                tx::errc::unknown_server_error});
                        }
                        return ss::make_ready_future<return_all_txs_res>(
                          std::move(res).value());
                    })
                  .finally([u = std::move(unit)] {});
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
    for (const auto& pa : ntp_meta->get_assignments()) {
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
    if (leader != _controller->self()) {
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
          auto partition = self._partition_manager.local().get(tm_ntp);
          if (!partition) {
              vlog(
                txlog.warn,
                "[tx_id={}] transaction manager {} partition not found",
                tid,
                tm_ntp);

              return ss::make_ready_future<result<tx_metadata, tx::errc>>(
                tx::errc::partition_not_found);
          }

          auto stm = partition->tm_stm();

          if (!stm) {
              vlog(
                txlog.warn,
                "[tx_id={}] can not get transactional manager stm for {}",
                tid,
                tm_ntp);
              return ss::make_ready_future<result<tx_metadata, tx::errc>>(
                tx::errc::stm_not_found);
          }

          return ss::with_gate(self._gate, [&stm, &self, tid] {
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
    auto term_opt = co_await stm->sync();
    if (!term_opt.has_value()) {
        if (term_opt.error() == tm_stm::op_status::not_leader) {
            co_return tx::errc::not_coordinator;
        }
        co_return tx::errc::coordinator_not_available;
    }
    auto term = term_opt.value();

    // create_topic_timeout_ms isn't the right timeout here but this change
    // is intendent to be a backport so we're not at will to introduce new
    // configuration; what we need there is a timeout which acts as an upper
    // boundary for happy case replication and create_topic_timeout_ms is a
    // good approximation, we already use it for that purpose in other api:
    // init_producer_id, add_offsets_to_txn etc
    auto timeout = config::shard_local_cfg().create_topic_timeout_ms();
    co_return co_await get_tx(term, stm, tid, timeout);
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

    auto leader = co_await wait_for_leader(tm_ntp.value());
    if (leader != _controller->self()) {
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
          auto partition = self._partition_manager.local().get(tm_ntp.value());
          if (!partition) {
              vlog(
                txlog.warn,
                "[tx_id={}] transaction manager {} partition not found",
                tid,
                tm_ntp);
              return ss::make_ready_future<tx::errc>(
                tx::errc::invalid_txn_state);
          }

          auto stm = partition->tm_stm();

          if (!stm) {
              vlog(
                txlog.warn,
                "[tx_id={}] can not get transactional manager stm for {}",
                tid,
                tm_ntp);
              return ss::make_ready_future<tx::errc>(
                tx::errc::invalid_txn_state);
          }

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
}

ss::future<tx::errc> tx_gateway_frontend::do_delete_partition_from_tx(
  ss::shared_ptr<tm_stm> stm,
  kafka::transactional_id tid,
  tx_metadata::tx_partition ntp) {
    checked<model::term_id, tm_stm::op_status> term_opt
      = tm_stm::op_status::unknown;
    try {
        term_opt = co_await stm->sync();
    } catch (...) {
        co_return tx::errc::invalid_txn_state;
    }
    if (!term_opt.has_value()) {
        if (term_opt.error() == tm_stm::op_status::not_leader) {
            co_return tx::errc::not_coordinator;
        }
        co_return tx::errc::invalid_txn_state;
    }
    auto term = term_opt.value();

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

} // namespace cluster

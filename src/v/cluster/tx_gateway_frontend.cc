// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cluster/tx_gateway_frontend.h"

#include "cluster/id_allocator_frontend.h"
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
#include "cluster/tx_gateway_service.h"
#include "cluster/tx_helpers.h"
#include "cluster/tx_topic_manager.h"
#include "config/configuration.h"
#include "kafka/protocol/types.h"
#include "model/fundamental.h"
#include "model/namespace.h"
#include "model/record.h"
#include "rpc/connection_cache.h"
#include "types.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/future.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/util/log.hh>

#include <algorithm>
#include <optional>
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

template<typename Func>
static auto with_free(
  ss::shared_ptr<tm_stm> stm,
  const kafka::transactional_id& tx_id,
  const std::string_view name,
  Func&& func) {
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

tx_gateway_frontend::tx_gateway_frontend(
  ss::smp_service_group ssg,
  ss::sharded<cluster::partition_manager>& partition_manager,
  ss::sharded<cluster::shard_table>& shard_table,
  ss::sharded<cluster::metadata_cache>& metadata_cache,
  ss::sharded<rpc::connection_cache>& connection_cache,
  ss::sharded<partition_leaders_table>& leaders,
  model::node_id self,
  ss::sharded<cluster::id_allocator_frontend>& id_allocator_frontend,
  rm_group_proxy* group_proxy,
  ss::sharded<cluster::rm_partition_frontend>& rm_partition_frontend,
  ss::sharded<features::feature_table>& feature_table,
  ss::sharded<cluster::tx_topic_manager>& tx_topic_manager,
  config::binding<uint64_t> max_transactions_per_coordinator)
  : _ssg(ssg)
  , _partition_manager(partition_manager)
  , _shard_table(shard_table)
  , _metadata_cache(metadata_cache)
  , _connection_cache(connection_cache)
  , _leaders(leaders)
  , _self(self)
  , _id_allocator_frontend(id_allocator_frontend)
  , _rm_group_proxy(group_proxy)
  , _rm_partition_frontend(rm_partition_frontend)
  , _feature_table(feature_table)
  , _tx_topic_manager(tx_topic_manager)
  , _metadata_dissemination_retries(
      config::shard_local_cfg().metadata_dissemination_retries.value())
  , _metadata_dissemination_retry_delay_ms(
      config::shard_local_cfg().metadata_dissemination_retry_delay_ms.value())
  , _transactional_id_expiration(
      config::shard_local_cfg().transactional_id_expiration_ms.bind())
  , _transactions_enabled(config::shard_local_cfg().enable_transactions.value())
  , _max_transactions_per_coordinator(
      std::move(max_transactions_per_coordinator)) {
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
    vlog(txlog.debug, "stopping transaction coordinator gateway");
    _expire_timer.cancel();
    _as.request_abort();
    co_await _gate.close();
    vlog(txlog.debug, "stopped transaction coordinator gateway");
}
namespace {
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

ss::future<try_abort_reply> tx_gateway_frontend::process_locally(
  ss::shared_ptr<tm_stm> stm, try_abort_request request) {
    vlog(txlog.trace, "processing try abort request: {}", request);
    auto read_units = co_await stm->read_lock();

    auto b_result = co_await stm->barrier();

    auto pid = request.pid;
    auto tx_seq = request.tx_seq;
    auto timeout = request.timeout;

    if (!b_result.has_value()) {
        vlog(txlog.debug, "stm barrier error: {}", b_result.error());
        if (b_result.error() == tm_stm::op_status::not_leader) {
            co_return try_abort_reply{tx::errc::not_coordinator};
        }
        co_return try_abort_reply{tx::errc::unknown_server_error};
    }
    auto synced_term = b_result.value();
    auto tx_id_opt = stm->get_id_by_pid(pid);
    if (!tx_id_opt) {
        vlog(
          txlog.trace,
          "can not find transactional id mapped to producer: {}, considering "
          "it aborted",
          pid);
        co_return try_abort_reply::make_aborted();
    }
    auto tx_id = tx_id_opt.value();

    auto reply = co_await with_free(
                   stm,
                   tx_id,
                   "try_abort",
                   [this, stm, synced_term, tx_id, pid, tx_seq, timeout]() {
                       return do_try_abort(
                         synced_term, stm, tx_id, pid, tx_seq, timeout);
                   })
                   .handle_exception_type([](const ss::semaphore_timed_out&) {
                       return try_abort_reply{tx::errc::unknown_server_error};
                   });

    if (reply.ec == tx::errc::none) {
        ssx::spawn_with_gate(
          _gate,
          [this,
           stm,
           tx_id,
           timeout,
           synced_term,
           read_units = std::move(read_units)]() mutable {
              return with(
                       stm,
                       tx_id,
                       "try_abort:get_tx",
                       [this, stm, tx_id, timeout, synced_term]() mutable {
                           return find_and_try_progressing_transaction(
                             synced_term, stm, tx_id, timeout);
                       })
                .finally([u = std::move(read_units)] {})
                .discard_result();
          });
    }
    co_return reply;
}

ss::future<try_abort_reply> tx_gateway_frontend::do_try_abort(
  model::term_id term,
  ss::shared_ptr<tm_stm> stm,
  kafka::transactional_id tx_id,
  model::producer_identity pid,
  model::tx_seq tx_seq,
  model::timeout_clock::duration) {
    auto sync_result = co_await sync_stm(stm);
    vlog(
      txlog.info,
      "[tx_id={}] trying to abort transaction. pid: {} sequence: {}",
      tx_id,
      pid,
      tx_seq);
    if (!sync_result.has_value()) {
        vlog(
          txlog.warn,
          "[tx_id={}] error syncing state machine - {}",
          tx_id,
          sync_result.error());
        co_return try_abort_reply{sync_result.error()};
    }

    if (sync_result.value() != term) {
        vlog(
          txlog.info,
          "[tx_id={}] leadership changed, when processing request",
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
        }
        vlog(
          txlog.warn,
          "[tx_id={}] error looking up the transaction - {}",
          tx_id,
          tx_opt.error());
        co_return try_abort_reply{tx::errc::unknown_server_error};
    }

    auto tx = std::move(tx_opt.value());

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
    vlog(txlog.info, "[tx_id={}] found transaction {} to abort", tx_id, tx);
    switch (tx.status) {
    case tx_status::empty:
        [[fallthrough]];
    case tx_status::ongoing: {
        vlog(txlog.trace, "[tx_id={}] aborting transaction: {}", tx_id, tx);
        auto killed_tx = co_await stm->update_transaction_status(
          term, tx.id, tx_status::preparing_internal_abort);
        if (!killed_tx.has_value()) {
            vlog(
              txlog.warn,
              "[tx_id={}] error aborting transaction - {}",
              tx.id,
              killed_tx.error());

            co_return try_abort_reply{
              map_state_update_outcome(killed_tx.error())};
        }
        co_return try_abort_reply::make_aborted();
    }
    case tx_status::preparing_commit:
        [[fallthrough]];
    case tx_status::completed_commit:
        vlog(
          txlog.trace,
          "[tx_id={}] transaction: {} is already committed",
          tx_id,
          tx);
        co_return try_abort_reply::make_committed();
    case tx_status::preparing_abort:
        [[fallthrough]];
    case tx_status::preparing_internal_abort:
        [[fallthrough]];
    case tx_status::completed_abort:
        [[fallthrough]];
    case tx_status::tombstone:
        vlog(
          txlog.trace,
          "[tx_id={}] transaction: {} is already aborted",
          tx_id,
          tx);
        co_return try_abort_reply::make_aborted();
    }
}

ss::future<cluster::init_tm_tx_reply> tx_gateway_frontend::init_tm_tx(
  kafka::transactional_id tx_id,
  std::chrono::milliseconds transaction_timeout_ms,
  model::timeout_clock::duration timeout,
  std::optional<model::producer_identity> expected_pid) {
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
  std::optional<model::producer_identity> expected_pid,
  model::partition_id tm) {
    vlog(
      txlog.trace,
      "[tx_id={}] processing name:init_tm_tx, timeout: {}",
      tx_id,
      transaction_timeout_ms);

    if (unlikely(
          transaction_timeout_ms
          > config::shard_local_cfg().transaction_max_timeout_ms())) {
        vlog(
          txlog.warn,
          "[tx_id={}] Transactional timeout requested {}ms exceeds configured "
          "maximum timeout {}ms",
          tx_id,
          transaction_timeout_ms,
          config::shard_local_cfg().transaction_max_timeout_ms());
        co_return init_tm_tx_reply{tx::errc::invalid_timeout};
    }

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

// This check returns true if current producer_id is the same for expected_pid
// from request or we had epoch overflow and expected producer id from request
// matches with last producer_id from log record
bool is_valid_producer(
  const tx_metadata& tx,
  const std::optional<model::producer_identity>& expected_pid) {
    if (!expected_pid) {
        return true;
    }

    return expected_pid->get_epoch() == model::no_producer_epoch
           || tx.pid.get_id() == expected_pid->get_id()
           || (tx.last_pid.get_id() == expected_pid->get_id() && expected_pid->has_exhausted_epoch());
}

} // namespace

ss::future<cluster::init_tm_tx_reply> tx_gateway_frontend::limit_init_tm_tx(
  ss::shared_ptr<tm_stm> stm,
  kafka::transactional_id tx_id,
  std::chrono::milliseconds transaction_timeout_ms,
  model::timeout_clock::duration timeout,
  std::optional<model::producer_identity> expected_pid) {
    auto sync_result = co_await sync_stm(stm);
    if (!sync_result.has_value()) {
        vlog(
          txlog.warn,
          "[tx_id={}] error syncing stm - {}",
          tx_id,
          sync_result.error());
        co_return init_tm_tx_reply{sync_result.error()};
    }
    auto term = sync_result.value();

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

            auto timeout
              = config::shard_local_cfg().internal_rpc_request_timeout_ms();
            auto tx_maybe = co_await find_and_try_progressing_transaction(
              term, stm, old_tx.id, timeout);
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
ss::future<cluster::init_tm_tx_reply>
tx_gateway_frontend::allocate_new_producer_id(
  ss::shared_ptr<tm_stm> stm,
  model::term_id term,
  kafka::transactional_id tx_id,
  std::chrono::milliseconds transaction_timeout_ms,
  model::timeout_clock::duration timeout) {
    allocate_id_reply pid_reply
      = co_await _id_allocator_frontend.local().allocate_id(timeout);

    if (pid_reply.ec != errc::success) {
        vlog(
          txlog.warn,
          "[tx_id={}] failed allocating producer id - {}",
          tx_id,
          pid_reply.ec);
        co_return init_tm_tx_reply{tx::errc::not_coordinator};
    }

    model::producer_identity new_pid(pid_reply.id, 0);

    tm_stm::op_status op_status = co_await stm->register_new_producer(
      term, tx_id, transaction_timeout_ms, new_pid);
    init_tm_tx_reply reply;
    reply.pid = new_pid;
    reply.ec = tx::errc::none;

    if (op_status != tm_stm::op_status::success) {
        reply.ec = tx::errc::none;
        vlog(
          txlog.warn,
          "[tx_id={}] error registering new producer {} - {}",
          tx_id,
          new_pid,
          op_status);
        switch (op_status) {
        case tm_stm::success:
            reply.ec = tx::errc::none;
            break;
        case tm_stm::conflict:
            reply.ec = tx::errc::conflict;
            break;
        case tm_stm::not_found:
        case tm_stm::unknown:
            reply.ec = tx::errc::unknown_server_error;
            break;
        case tm_stm::not_leader:
        case tm_stm::partition_not_found:
            reply.ec = tx::errc::not_coordinator;
            break;
        case tm_stm::timeout:
            reply.ec = tx::errc::timeout;
            break;
        }
    }
    co_return reply;
}

ss::future<cluster::init_tm_tx_reply> tx_gateway_frontend::do_init_tm_tx(
  ss::shared_ptr<tm_stm> stm,
  model::term_id term,
  kafka::transactional_id tx_id,
  std::chrono::milliseconds transaction_timeout_ms,
  model::timeout_clock::duration timeout,
  std::optional<model::producer_identity> expected_pid) {
    auto tx_result = co_await find_and_try_progressing_transaction(
      term, stm, tx_id, timeout);

    if (tx_result.has_error()) {
        if (tx_result.error() == tx::errc::tx_not_found) {
            co_return co_await allocate_new_producer_id(
              stm, term, std::move(tx_id), transaction_timeout_ms, timeout);
        }
        vlog(
          txlog.warn,
          "[tx_id={}] error getting transaction metadata: {}",
          tx_id,
          tx_result.error());
        co_return init_tm_tx_reply{tx_result.error()};
    }
    // mapping for current transactional id already exists
    auto tx = std::move(tx_result.value());

    if (!is_valid_producer(tx, expected_pid)) {
        vlog(
          txlog.info,
          "[tx_id={}] producer with expected pid: {} for {} is invalid",
          tx_id,
          expected_pid,
          tx);
        co_return init_tm_tx_reply{tx::errc::invalid_producer_epoch};
    }

    switch (tx.status) {
    case tx_status::ongoing: {
        vlog(txlog.info, "[tx_id={}] tx is ongoing, aborting", tx_id);
        auto abort_result = co_await do_abort_tm_tx(term, stm, tx, timeout);
        if (!abort_result) {
            vlog(
              txlog.warn,
              "[tx_id={}] error rolling previous transaction",
              tx_id,
              tx.status,
              abort_result.error());
            co_return init_tm_tx_reply{abort_result.error()};
        }
        co_return init_tm_tx_reply{tx::errc::concurrent_transactions};
    }
    case tx_status::empty:
    case tx_status::tombstone:
    case tx_status::completed_commit:
    case tx_status::completed_abort: {
        co_return co_await increase_producer_epoch(
          tx.id,
          tx.pid,
          tx.last_pid,
          expected_pid,
          stm,
          term,
          transaction_timeout_ms,
          timeout);
    }
    case tx_status::preparing_abort:
    case tx_status::preparing_internal_abort:
    case tx_status::preparing_commit:
        co_return init_tm_tx_reply{tx::errc::concurrent_transactions};
    }
}

ss::future<cluster::init_tm_tx_reply>
tx_gateway_frontend::increase_producer_epoch(
  kafka::transactional_id tx_id,
  model::producer_identity tx_pid,
  model::producer_identity last_tx_pid,
  std::optional<model::producer_identity> expected_pid,
  ss::shared_ptr<tm_stm> stm,
  model::term_id term,
  std::chrono::milliseconds transaction_timeout_ms,
  model::timeout_clock::duration timeout) {
    // the expected epoch can be empty then it matches everything
    const bool expected_epoch_matches = expected_pid
                                          ? expected_pid->epoch == tx_pid.epoch
                                          : true;
    // exhausted epoch, allocate new producer id
    if (tx_pid.has_exhausted_epoch() && expected_epoch_matches) {
        allocate_id_reply pid_reply
          = co_await _id_allocator_frontend.local().allocate_id(timeout);

        if (pid_reply.ec != errc::success) {
            vlog(
              txlog.warn,
              "[tx_id={}] failed allocating producer id - {}",
              tx_id,
              pid_reply.ec);
            co_return init_tm_tx_reply{tx::errc::not_coordinator};
        }
        tx_pid = model::producer_identity(
          pid_reply.id, model::no_producer_epoch);
    }

    init_tm_tx_reply reply;
    if (tx_pid.has_exhausted_epoch()) {
        reply.ec = tx::errc::invalid_producer_epoch;
        co_return reply;
    }
    // expected producer id wasn't provided,
    if (!expected_pid) {
        tx_pid = model::producer_identity::with_next_epoch(tx_pid);
        last_tx_pid = model::no_pid;
    } else if (
      tx_pid.epoch == model::no_producer_epoch || expected_pid == tx_pid) {
        // If the expected epoch matches the current epoch, or if there is no
        // current epoch, the producer is attempting
        // to continue after an error and no other producer has been
        // initialized. Bump the current and last epochs. The no current epoch
        // case means this is a new producer; producerEpoch will be -1 and
        // bumpedEpoch will be 0
        last_tx_pid = tx_pid;
        tx_pid = model::producer_identity::with_next_epoch(tx_pid);
    } else if (last_tx_pid == expected_pid) {
        // If the expected epoch matches the previous epoch, it is a retry of a
        // successful call, so just return the current epoch without bumping.
        // There is no danger of this producer being fenced, because a new
        // producer calling InitProducerId would have caused the last epoch to
        // be set to -1. Note that if the IBP is prior to 2.4.IV1, the
        // lastProducerId and lastProducerEpoch will not be written to the
        // transaction log, so a retry that spans a coordinator change will
        // fail. We expect this to be a rare case.

    } else {
        vlog(
          txlog.trace,
          "[tx_id={}] producer fenced current pid: {}, expected pid: {}, "
          "last_pid: {}",
          tx_id,
          tx_pid,
          expected_pid,
          last_tx_pid);
        co_return init_tm_tx_reply{tx::errc::fenced};
    }
    reply.pid = tx_pid;

    auto op_status = co_await stm->update_tx_producer(
      term, tx_id, transaction_timeout_ms, tx_pid, last_tx_pid);
    if (op_status == tm_stm::op_status::success) {
        reply.ec = tx::errc::none;
    } else if (op_status == tm_stm::op_status::conflict) {
        reply.ec = tx::errc::conflict;
    } else if (op_status == tm_stm::op_status::timeout) {
        reply.ec = tx::errc::timeout;
    } else {
        vlog(
          txlog.warn,
          "[tx_id={}] error updating transaction metadata producer {} - {}",
          tx_id,
          tx_pid,
          op_status);
        reply.ec = tx::errc::invalid_txn_state;
    }
    co_return reply;
}

} // namespace cluster

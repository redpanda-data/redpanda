// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cluster/rm_stm.h"

#include "bytes/iostream.h"
#include "cluster/logger.h"
#include "cluster/producer_state_manager.h"
#include "cluster/rm_stm_types.h"
#include "cluster/tx_gateway_frontend.h"
#include "cluster/types.h"
#include "kafka/protocol/wire.h"
#include "metrics/metrics.h"
#include "metrics/prometheus_sanitize.h"
#include "model/fundamental.h"
#include "model/record.h"
#include "model/timestamp.h"
#include "raft/consensus_utils.h"
#include "raft/errc.h"
#include "raft/fundamental.h"
#include "raft/persisted_stm.h"
#include "raft/state_machine_base.h"
#include "ssx/future-util.h"
#include "storage/parser_utils.h"
#include "storage/record_batch_builder.h"
#include "utils/human.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/future.hh>
#include <seastar/core/loop.hh>
#include <seastar/core/sstring.hh>
#include <seastar/coroutine/as_future.hh>

#include <filesystem>
#include <optional>
#include <ranges>
#include <utility>

namespace cluster {
using namespace tx;
using namespace std::chrono_literals;

namespace {
ss::sstring abort_idx_name(model::offset first, model::offset last) {
    return fmt::format("abort.idx.{}.{}", first, last);
}

raft::replicate_options make_replicate_options() {
    auto opts = raft::replicate_options(raft::consistency_level::quorum_ack);
    opts.set_force_flush();
    return opts;
}

} // namespace

model::control_record_type
rm_stm::parse_tx_control_batch(const model::record_batch& b) {
    return parse_control_batch(b);
}

rm_stm::rm_stm(
  ss::logger& logger,
  raft::consensus* c,
  ss::sharded<cluster::tx_gateway_frontend>& tx_gateway_frontend,
  ss::sharded<features::feature_table>& feature_table,
  ss::sharded<tx::producer_state_manager>& producer_state_manager,
  std::optional<model::vcluster_id> vcluster_id)
  : raft::persisted_stm<>(rm_stm_snapshot, logger, c)
  , _sync_timeout(config::shard_local_cfg().rm_sync_timeout_ms.value())
  , _tx_timeout_delay(config::shard_local_cfg().tx_timeout_delay_ms.value())
  , _abort_interval_ms(config::shard_local_cfg()
                         .abort_timed_out_transactions_interval_ms.value())
  , _abort_index_segment_size(
      config::shard_local_cfg().abort_index_segment_size.value())
  , _is_tx_enabled(config::shard_local_cfg().enable_transactions.value())
  , _tx_gateway_frontend(tx_gateway_frontend)
  , _abort_snapshot_mgr(
      "abort.idx",
      std::filesystem::path(c->log_config().work_directory()),
      ss::default_priority_class())
  , _feature_table(feature_table)
  , _ctx_log(txlog, ssx::sformat("[{}]", c->ntp()))
  , _producer_state_manager(producer_state_manager)
  , _vcluster_id(vcluster_id) {
    vassert(
      _feature_table.local().is_active(features::feature::transaction_ga),
      "unexpected state for transactions support. skipped a few "
      "versions during upgrade?");
    setup_metrics();
    if (!_is_tx_enabled) {
        _is_autoabort_enabled = false;
    }
    auto_abort_timer.set_callback([this] { abort_old_txes(); });
    /// Wait to determine when the raft committed offset has been set at
    /// startup.
    ssx::spawn_with_gate(_gate, [this]() {
        return bootstrap_committed_offset()
          .then([this](model::offset o) {
              vlog(
                _ctx_log.info, "Setting bootstrap committed offset to: {}", o);
              _bootstrap_committed_offset = o;
          })
          .handle_exception_type([](const ss::abort_requested_exception&) {})
          .handle_exception_type([](const ss::gate_closed_exception&) {})
          .handle_exception([this](const std::exception_ptr& e) {
              vlog(
                _ctx_log.warn,
                "Failed to set bootstrap committed index due to exception: {}",
                e);
          });
    });
}

ss::future<model::offset> rm_stm::bootstrap_committed_offset() {
    /// It is useful for some STMs to know what the committed offset is so they
    /// may do things like block until they have consumed all known committed
    /// records. To achieve this, this method waits on offset 0, so on the first
    /// call to `event_manager::notify_commit_index`, it is known that the
    /// committed offset is in an initialized state.
    return _raft->events()
      .wait(model::offset(0), model::no_timeout, _as)
      .then([this] { return _raft->committed_offset(); });
}

producer_ptr rm_stm::maybe_create_producer(model::producer_identity pid) {
    // Double lookup because of two reasons
    // 1. we are forced to use a ptr as map value_type because producer_state is
    // not movable
    // 2. btree_map does not support lazy_emplace and we are stuck to btree_map
    // as it is memory friendly.
    auto it = _producers.find(pid.get_id());
    if (it != _producers.end()) {
        return it->second;
    }
    auto producer = ss::make_lw_shared<producer_state>(
      _ctx_log, pid, _raft->group(), [pid, this] {
          cleanup_producer_state(pid);
      });
    _producer_state_manager.local().register_producer(*producer, _vcluster_id);
    _producers.emplace(pid.get_id(), producer);

    return producer;
}

void rm_stm::cleanup_producer_state(model::producer_identity pid) {
    auto it = _producers.find(pid.get_id());
    if (it != _producers.end() && it->second->id() == pid) {
        vassert(
          !it->second->_active_transaction_hook.is_linked(),
          "Cleaning up {} while active transaction is in progress",
          *(it->second));
        _producers.erase(it);
    }
};

ss::future<> rm_stm::reset_producers() {
    co_await ss::max_concurrent_for_each(
      _producers.begin(), _producers.end(), 32, [this](auto& it) {
          auto& producer = it.second;
          producer->shutdown_input();
          _producer_state_manager.local().deregister_producer(
            *producer, _vcluster_id);
          return ss::now();
      });
    _active_tx_producers.clear();
    _producers.clear();
}

ss::future<checked<model::term_id, tx::errc>> rm_stm::begin_tx(
  model::producer_identity new_pid,
  model::tx_seq tx_seq,
  std::chrono::milliseconds transaction_timeout_ms,
  model::partition_id tm) {
    auto state_lock = co_await _state_lock.hold_read_lock();
    if (!co_await sync(_sync_timeout)) {
        vlog(
          _ctx_log.trace,
          "processing name:begin_tx pid:{}, tx_seq:{}, "
          "timeout:{}, coordinator:{}, error: stale leader",
          new_pid,
          tx_seq,
          transaction_timeout_ms,
          tm);
        co_return tx::errc::stale;
    }
    auto synced_term = _insync_term;
    auto producer = maybe_create_producer(new_pid);
    co_return co_await producer->run_with_lock(
      [this,
       synced_term,
       new_pid,
       tx_seq,
       transaction_timeout_ms,
       tm,
       producer](ssx::semaphore_units units) {
          return do_begin_tx(
                   synced_term,
                   new_pid,
                   producer,
                   tx_seq,
                   transaction_timeout_ms,
                   tm)
            .finally([units = std::move(units)] {});
      });
}

ss::future<checked<model::term_id, tx::errc>> rm_stm::do_begin_tx(
  model::term_id synced_term,
  model::producer_identity pid,
  producer_ptr producer,
  model::tx_seq tx_seq,
  std::chrono::milliseconds transaction_timeout_ms,
  model::partition_id tm) {
    if (!check_tx_permitted()) {
        co_return tx::errc::request_rejected;
    }

    if (!_raft->is_leader()) {
        vlog(
          _ctx_log.trace,
          "processing name:begin_tx pid:{}, tx_seq:{}, "
          "timeout:{}, coordinator:{} => not "
          "a leader",
          pid,
          tx_seq,
          transaction_timeout_ms,
          tm);
        co_return tx::errc::leader_not_found;
    }

    vlog(
      _ctx_log.trace,
      "processing name:begin_tx pid:{}, tx_seq:{}, timeout:{}, coordinator:{}  "
      "in term:{} with producer: {}",
      pid,
      tx_seq,
      transaction_timeout_ms,
      tm,
      synced_term,
      *producer);

    auto current_pid = producer->id();
    if (pid.epoch < current_pid.epoch) {
        // request from an older instance of the producer
        co_return tx::errc::fenced;
    }
    if (pid.epoch > current_pid.epoch) {
        // abort any transactions from the older instance of the producer.
        auto ar = co_await do_abort_tx(
          synced_term, producer, std::nullopt, _sync_timeout);
        if (ar != tx::errc::none) {
            vlog(
              _ctx_log.trace,
              "can't begin tx {} because abort of a prev tx {} failed with {}",
              pid,
              current_pid,
              ar);
            co_return tx::errc::stale;
        }
    } else if (producer->has_transaction_in_progress()) {
        // check for duplicate request
        if (
          producer->transaction_state()->status
          == tx::partition_transaction_status::initialized) {
            vlog(
              _ctx_log.debug,
              "Ignoring duplicate begin_tx request with producer: {}",
              *producer);
            // no data yet, in the transaction ignore the duplicate
            // request by returning success
            co_return synced_term;
        }
        vlog(
          _ctx_log.warn,
          "duplicate begin request with producer after the transaction already "
          "began: {}",
          *producer);
        co_return tx::errc::unknown_server_error;
    }
    // By now any in progresss transactions are aborted and the resulting
    // state and the resulting changes are reflected in the producer state.
    vassert(
      !producer->has_transaction_in_progress(),
      "There is an in progress transaction for producer, invalid state: {}",
      producer);

    model::record_batch batch = make_fence_batch(
      pid, tx_seq, transaction_timeout_ms, tm);

    auto reader = model::make_memory_record_batch_reader(std::move(batch));
    auto r = co_await _raft->replicate(
      synced_term, std::move(reader), make_replicate_options());

    if (!r) {
        vlog(
          _ctx_log.trace,
          "Error \"{}\" on replicating pid:{} tx_seq:{} fencing batch",
          r.error(),
          pid,
          tx_seq);
        if (_raft->is_leader() && _raft->term() == synced_term) {
            co_await _raft->step_down("begin_tx replication error");
        }
        // begin is idempotent so it's ok to return a retryable error
        co_return tx::errc::timeout;
    }

    if (!co_await wait_no_throw(
          model::offset(r.value().last_offset()),
          model::timeout_clock::now() + _sync_timeout)) {
        vlog(
          _ctx_log.trace,
          "timeout on waiting until {} is applied (begin_tx pid:{} tx_seq:{})",
          r.value().last_offset(),
          pid,
          tx_seq);
        if (_raft->is_leader() && _raft->term() == synced_term) {
            co_await _raft->step_down("begin_tx apply error");
        }
        co_return tx::errc::timeout;
    }

    if (_raft->term() != synced_term) {
        vlog(
          _ctx_log.trace,
          "term changed from {} to {} during fencing pid:{} tx_seq:{}",
          synced_term,
          _raft->term(),
          pid,
          tx_seq);
        co_return tx::errc::leader_not_found;
    }

    const auto& tx_state = producer->transaction_state();
    vassert(
      tx_state && tx_state->sequence == tx_seq,
      "Inconsistent state detected after tx begin, expected sequence: {}, from "
      "producer: {}",
      tx_seq,
      *producer);

    co_return synced_term;
}

ss::future<tx::errc> rm_stm::commit_tx(
  model::producer_identity pid,
  model::tx_seq tx_seq,
  model::timeout_clock::duration timeout) {
    auto state_lock_holder = co_await _state_lock.hold_read_lock();
    if (!co_await sync(timeout)) {
        co_return tx::errc::stale;
    }
    auto synced_term = _insync_term;
    auto producer = maybe_create_producer(pid);
    if (pid != producer->id()) {
        co_return tx::errc::fenced;
    }
    co_return co_await producer->run_with_lock(
      [this, synced_term, tx_seq, timeout, producer](
        ssx::semaphore_units units) {
          return do_commit_tx(synced_term, producer, tx_seq, timeout)
            .finally([units = std::move(units)] {});
      });
}

ss::future<tx::errc> rm_stm::do_commit_tx(
  model::term_id synced_term,
  tx::producer_ptr producer,
  model::tx_seq expected_tx_seq,
  model::timeout_clock::duration timeout) {
    auto pid = producer->id();
    vlog(
      _ctx_log.trace,
      "commit tx pid: {}, tx sequence: {}",
      pid,
      expected_tx_seq);
    if (!check_tx_permitted()) {
        co_return tx::errc::request_rejected;
    }

    if (!producer->has_transaction_in_progress()) {
        // check if this a replay commit request on coordinator recovery.
        const auto& tx_state = producer->transaction_state();
        if (
          tx_state
          && tx_state->status == tx::partition_transaction_status::committed
          && tx_state->sequence == expected_tx_seq) {
            // Already committed request
            co_return tx::errc::none;
        }
        // At this point we do not have any trace of the transaction with
        // required sequence. This is possible only when the transaction has
        // been sealed (committed/aborted) as we do not evict in progress
        // transaction state. The state could've been GC-ed along with the
        // producer or there are edge cases like upgrading from snapshot
        // v5 -> v6 that does not preserve sealed transactions (rare edge
        // case only relevant during upgrades).
        //
        // It can only be committed because the commit_tx request from the
        // coordinator originates if the transaction has been marked prepared
        // in the coordinator log, so the only way it would have been sealed
        // already is with another commit request and this is a replay.
        vlog(
          _ctx_log.info,
          "No in progress transaction {} while attempting to (replay) commit "
          "sequence: {}, assuming committed.",
          *producer,
          expected_tx_seq);
        co_return tx::errc::none;
    }
    auto transaction_sequence = producer->transaction_state()->sequence;
    if (transaction_sequence > expected_tx_seq) {
        vlog(
          _ctx_log.trace,
          "Already commited pid:{} tx_seq:{} - a higher tx_seq:{} was "
          "observed",
          *producer,
          expected_tx_seq,
          transaction_sequence);
        co_return tx::errc::none;
    } else if (transaction_sequence != expected_tx_seq) {
        vlog(
          _ctx_log.trace,
          "can't commit pid:{} tx: passed txseq {} doesn't match local {}",
          *producer,
          expected_tx_seq,
          transaction_sequence);
        co_return tx::errc::request_rejected;
    }

    auto batch = make_tx_control_batch(
      pid, model::control_record_type::tx_commit);
    auto reader = model::make_memory_record_batch_reader(std::move(batch));
    auto r = co_await _raft->replicate(
      synced_term, std::move(reader), make_replicate_options());

    if (!r) {
        vlog(
          _ctx_log.warn,
          "Error \"{}\" on replicating pid:{} commit batch",
          r.error(),
          pid);
        if (_raft->is_leader() && _raft->term() == synced_term) {
            co_await _raft->step_down("do_commit_tx replication error");
        }
        co_return tx::errc::timeout;
    }
    if (!co_await wait_no_throw(
          r.value().last_offset, model::timeout_clock::now() + timeout)) {
        if (_raft->is_leader() && _raft->term() == synced_term) {
            co_await _raft->step_down("do_commit_tx wait error");
        }
        co_return tx::errc::timeout;
    }

    co_return tx::errc::none;
}

abort_origin rm_stm::get_abort_origin(
  tx::producer_ptr producer, model::tx_seq expected_tx_seq) const {
    auto current_tx_seq = producer->get_transaction_sequence();
    if (!current_tx_seq) {
        return abort_origin::present;
    }

    if (expected_tx_seq < *current_tx_seq) {
        return abort_origin::past;
    }
    if (*current_tx_seq < expected_tx_seq) {
        return abort_origin::future;
    }
    return abort_origin::present;
}

ss::future<tx::errc> rm_stm::abort_tx(
  model::producer_identity pid,
  model::tx_seq tx_seq,
  model::timeout_clock::duration timeout) {
    auto state_lock_holder = co_await _state_lock.hold_read_lock();
    if (!co_await sync(timeout)) {
        vlog(
          _ctx_log.trace,
          "processing name:abort_tx pid:{} tx_seq:{} => stale leader",
          pid,
          tx_seq);
        co_return tx::errc::stale;
    }
    auto synced_term = _insync_term;
    auto producer = maybe_create_producer(pid);
    if (pid != producer->id()) {
        co_return cluster::errc::invalid_producer_epoch;
    }
    co_return co_await producer->run_with_lock(
      [this, synced_term, tx_seq, timeout, producer](
        ssx::semaphore_units units) {
          return do_abort_tx(synced_term, producer, tx_seq, timeout)
            .finally([units = std::move(units)] {});
      });
}

// abort_tx is invoked strictly after a tx is canceled on the tm_stm
// the purpose of abort is to put tx_range into the list of aborted txes
// and to fence off the old epoch.
// we need to check tx_seq to filter out stale requests
ss::future<tx::errc> rm_stm::do_abort_tx(
  model::term_id synced_term,
  producer_ptr producer,
  std::optional<model::tx_seq> expected_tx_seq,
  model::timeout_clock::duration timeout) {
    if (!check_tx_permitted()) {
        co_return tx::errc::request_rejected;
    }

    auto pid = producer->id();
    vlog(
      _ctx_log.trace,
      "processing name:abort_tx producer:{} tx_seq:{} in term:{}",
      *producer,
      expected_tx_seq.value_or(model::tx_seq(-1)),
      synced_term);

    if (!producer->has_transaction_in_progress()) {
        vlog(
          _ctx_log.trace,
          "No in-progress transaction for producer: {}, ignoring abort_tx, "
          "seq: {}",
          *producer,
          expected_tx_seq);
        co_return tx::errc::none;
    }

    if (expected_tx_seq) {
        auto origin = get_abort_origin(producer, expected_tx_seq.value());
        if (origin == abort_origin::past) {
            // An abort request has older tx_seq. It may mean than the request
            // was duplicated, delayed and retried later.
            //
            // Or it may mean that a tx coordinator
            //   - lost its state
            //   - rolled back to previous op
            //   - the previous op happend to be an abort
            //   - the coordinator retried it
            //
            // In the first case the least impactful way to reject the request.
            // But in the second case rejection will only cause the retry loop
            // which blocks a transaction until the hanging tx expires (by
            // default within one minute).
            //
            // It's more unlikely to receiving a message from the past so we
            // improving the second case by aborting the ongoing tx before it's
            // expired.
            //
            // If it happens to be the first case then Redpanda rejects a
            // client's tx.
            vlog(
              _ctx_log.info,
              "abort_tx request producer:{} tx_seq:{} came from the past => "
              "rejecting",
              *producer,
              expected_tx_seq.value_or(model::tx_seq(-1)));
            co_return tx::errc::request_rejected;
        }
        if (origin == abort_origin::future) {
            // impossible situation: before transactional coordinator may issue
            // abort of the current transaction it should begin it and abort all
            // previous transactions with the same pid
            vlog(
              _ctx_log.error,
              "Rejecting abort (pid:{}, tx_seq: {}) because it isn't "
              "consistent with the current ongoing transaction",
              pid,
              expected_tx_seq.value());
            co_return tx::errc::request_rejected;
        }
    }

    auto batch = make_tx_control_batch(
      pid, model::control_record_type::tx_abort);
    auto reader = model::make_memory_record_batch_reader(std::move(batch));
    auto r = co_await _raft->replicate(
      synced_term, std::move(reader), make_replicate_options());

    if (!r) {
        vlog(
          _ctx_log.info,
          "Error \"{}\" on replicating pid:{} tx_seq:{} abort batch",
          r.error(),
          pid,
          expected_tx_seq.value_or(model::tx_seq(-1)));
        if (_raft->is_leader() && _raft->term() == synced_term) {
            co_await _raft->step_down("abort_tx replication error");
        }
        co_return tx::errc::timeout;
    }

    if (!co_await wait_no_throw(
          r.value().last_offset, model::timeout_clock::now() + timeout)) {
        vlog(
          _ctx_log.trace,
          "timeout on waiting until {} is applied (abort_tx pid:{} tx_seq:{})",
          r.value().last_offset,
          pid,
          expected_tx_seq.value_or(model::tx_seq(-1)));
        if (_raft->is_leader() && _raft->term() == synced_term) {
            co_await _raft->step_down("abort_tx apply error");
        }
        co_return tx::errc::timeout;
    }

    co_return tx::errc::none;
}

kafka_stages rm_stm::replicate_in_stages(
  model::batch_identity bid,
  model::record_batch_reader r,
  raft::replicate_options opts) {
    auto enqueued = ss::make_lw_shared<available_promise<>>();
    auto f = enqueued->get_future();
    auto replicate_finished
      = do_replicate(bid, std::move(r), opts, enqueued).finally([enqueued] {
            // we should avoid situations when replicate_finished is set while
            // enqueued isn't because it leads to hanging produce requests and
            // the resource leaks. since staged replication is an optimization
            // and setting enqueued only after replicate_finished is already
            // set doesn't have sematic implications adding this post
            // replicate_finished as a safety measure in case enqueued isn't
            // set explicitly
            if (!enqueued->available()) {
                enqueued->set_value();
            }
        });
    return kafka_stages(std::move(f), std::move(replicate_finished));
}

ss::future<result<kafka_result>> rm_stm::replicate(
  model::batch_identity bid,
  model::record_batch_reader r,
  raft::replicate_options opts) {
    auto enqueued = ss::make_lw_shared<available_promise<>>();
    return do_replicate(bid, std::move(r), opts, enqueued);
}

ss::future<ss::basic_rwlock<>::holder> rm_stm::prepare_transfer_leadership() {
    co_return co_await _state_lock.hold_write_lock();
}

ss::future<result<kafka_result>> rm_stm::do_replicate(
  model::batch_identity bid,
  model::record_batch_reader b,
  raft::replicate_options opts,
  ss::lw_shared_ptr<available_promise<>> enqueued) {
    auto holder = _gate.hold();
    auto unit = co_await _state_lock.hold_read_lock();
    if (bid.is_transactional) {
        co_return co_await transactional_replicate(bid, std::move(b));
    } else if (bid.is_idempotent()) {
        co_return co_await idempotent_replicate(
          bid, std::move(b), opts, enqueued);
    }
    co_return co_await replicate_msg(std::move(b), opts, enqueued);
}

ss::future<> rm_stm::stop() {
    _as.request_abort();
    auto_abort_timer.cancel();
    co_await _gate.close();
    co_await reset_producers();
    _metrics.clear();
    co_await raft::persisted_stm<>::stop();
}

ss::future<> rm_stm::start() { return persisted_stm::start(); }

std::optional<int32_t>
rm_stm::get_seq_number(model::producer_identity pid) const {
    auto it = _producers.find(pid.get_id());
    if (it == _producers.end()) {
        return std::nullopt;
    }
    return it->second->last_sequence_number();
}

ss::future<result<partition_transactions>> rm_stm::get_transactions() {
    if (!co_await sync(_sync_timeout)) {
        co_return cluster::errc::not_leader;
    }
    partition_transactions ans;
    for (auto& producer : _active_tx_producers) {
        const auto& tx_state = producer.transaction_state();
        vassert(
          tx_state,
          "Invalid transactional state_tracking for producer: {}",
          producer);
        partition_transaction_info tx_info;
        tx_info.lso_bound = tx_state->first;
        tx_info.status = tx_state->status;
        tx_info.info = producer.get_expiration_info();
        tx_info.seq = producer.last_sequence_number();
        ans.emplace(producer.id(), tx_info);
    }
    co_return ans;
}

ss::future<std::error_code> rm_stm::mark_expired(model::producer_identity pid) {
    if (!co_await sync(_sync_timeout)) {
        co_return std::error_code(tx::errc::leader_not_found);
    }
    auto holder = co_await _state_lock.hold_read_lock();
    auto producer_it = _producers.find(pid.get_id());
    if (
      producer_it == _producers.end()
      || !producer_it->second->has_transaction_in_progress()) {
        co_return std::error_code{tx::errc::none};
    }
    auto producer = producer_it->second;
    co_return co_await producer->run_with_lock(
      [this, producer](ssx::semaphore_units units) {
          producer->force_transaction_expiry();
          return do_try_abort_old_tx(producer)
            .then([](tx::errc result) { return std::error_code(result); })
            .finally([units = std::move(units)] {});
      });
}

ss::future<result<kafka_result>> rm_stm::transactional_replicate(
  model::term_id synced_term,
  producer_ptr producer,
  model::batch_identity bid,
  model::record_batch_reader rdr) {
    auto result = co_await do_transactional_replicate(
      synced_term, producer, bid, std::move(rdr));
    if (!result) {
        vlog(
          _ctx_log.trace,
          "error from do_transactional_replicate: {}, pid: {}, range: [{}, {}]",
          result.error(),
          bid.pid,
          bid.first_seq,
          bid.last_seq);
        if (result.error() == cluster::errc::sequence_out_of_order) {
            auto barrier = co_await _raft->linearizable_barrier();
            if (!barrier) {
                co_return cluster::errc::not_leader;
            }
        } else if (_raft->is_leader() && _raft->term() == synced_term) {
            co_await _raft->step_down(
              "Failed replication during transactional replicate.");
        }
        co_return result.error();
    }
    co_return result;
}

ss::future<result<kafka_result>> rm_stm::do_transactional_replicate(
  model::term_id synced_term,
  producer_ptr producer,
  model::batch_identity bid,
  model::record_batch_reader rdr) {
    if (producer->id().epoch != bid.pid.epoch) {
        vlog(
          _ctx_log.warn,
          "producer {} by newer instance: {}, failed replication",
          bid.pid,
          *producer);
        co_return cluster::errc::invalid_producer_epoch;
    }
    if (!producer->has_transaction_in_progress()) {
        vlog(
          _ctx_log.warn,
          "No transaction found for producer: {}, request: {}",
          *producer,
          bid);
        co_return cluster::errc::invalid_producer_epoch;
    }

    vlog(
      _ctx_log.trace,
      "attempt to transactionally replicate batch: {} using producer: {}",
      bid,
      *producer);

    // For the first batch of a transaction, reset sequence tracking to handle
    // an edge case where client reuses sequence number after an aborted
    // transaction see https://github.com/redpanda-data/redpanda/pull/5026
    // for details
    const auto& tx_state = producer->transaction_state();
    bool reset_sequence_tracking
      = tx_state->status == tx::partition_transaction_status::initialized;
    auto request = producer->try_emplace_request(
      bid, synced_term, reset_sequence_tracking);

    if (!request) {
        co_return request.error();
    }
    auto req_ptr = request.value();
    if (req_ptr->state() != request_state::initialized) {
        co_return co_await req_ptr->result();
    }
    req_ptr->mark_request_in_progress();

    auto r = co_await _raft->replicate(
      synced_term, std::move(rdr), make_replicate_options());
    if (!r) {
        vlog(
          _ctx_log.info,
          "got {} on replicating tx data batch for pid:{}",
          r.error(),
          bid.pid);
        req_ptr->set_error(r.error());
        co_return r.error();
    }
    if (!co_await wait_no_throw(
          model::offset(r.value().last_offset()),
          model::timeout_clock::now() + _sync_timeout)) {
        vlog(
          _ctx_log.warn,
          "application of the replicated tx batch has timed out pid:{}",
          bid.pid);
        req_ptr->set_error(cluster::errc::timeout);
        co_return tx::errc::timeout;
    }
    auto result = kafka_result{
      .last_offset = from_log_offset(r.value().last_offset)};
    req_ptr->set_value(result);
    co_return result;
}

ss::future<result<kafka_result>> rm_stm::transactional_replicate(
  model::batch_identity bid, model::record_batch_reader rdr) {
    if (!check_tx_permitted()) {
        co_return cluster::errc::generic_tx_error;
    }
    if (!co_await sync(_sync_timeout)) {
        vlog(
          _ctx_log.trace,
          "processing name:replicate_tx pid:{} => stale leader",
          bid.pid);
        co_return cluster::errc::not_leader;
    }
    auto synced_term = _insync_term;
    auto producer = maybe_create_producer(bid.pid);
    co_return co_await producer->run_with_lock(
      [&, synced_term](ssx::semaphore_units units) {
          return do_transactional_replicate(
                   synced_term, producer, bid, std::move(rdr))
            .finally([units = std::move(units)] {});
      });
}

ss::future<result<kafka_result>> rm_stm::idempotent_replicate(
  model::term_id synced_term,
  producer_ptr producer,
  model::batch_identity bid,
  model::record_batch_reader br,
  raft::replicate_options opts,
  ss::lw_shared_ptr<available_promise<>> enqueued,
  ssx::semaphore_units units) {
    auto result = co_await do_idempotent_replicate(
      synced_term,
      producer,
      bid,
      std::move(br),
      opts,
      std::move(enqueued),
      units);

    if (!result) {
        vlog(
          _ctx_log.trace,
          "error from do_idempotent_replicate: {}, pid: {}, range: [{}, {}]",
          result.error(),
          bid.pid,
          bid.first_seq,
          bid.last_seq);
        if (result.error() == cluster::errc::sequence_out_of_order) {
            // release the lock so it is not held for the duration of the
            // barrier, other requests can make progress if they are
            // in the right sequence.
            units.return_all();
            // Ensure we are actually the leader and request didn't
            // ooosn on a stale state. If we are not the leader return
            // a retryable error code to the client.
            auto barrier = co_await _raft->linearizable_barrier();
            if (!barrier) {
                co_return cluster::errc::not_leader;
            }
        } else {
            // if the request enqueue failed, its important to step down
            // under units scope so that the next request in the pipeline
            // fails on sync.
            if (_raft->is_leader() && _raft->term() == synced_term) {
                co_await _raft->step_down(
                  "Failed replication during idempotent replicate.");
            }
        }
        co_return result.error();
    }
    co_return result.value();
}

ss::future<result<kafka_result>> rm_stm::do_idempotent_replicate(
  model::term_id synced_term,
  producer_ptr producer,
  model::batch_identity bid,
  model::record_batch_reader br,
  raft::replicate_options opts,
  ss::lw_shared_ptr<available_promise<>> enqueued,
  ssx::semaphore_units& units) {
    // Check if the producer bumped the epoch and reset accordingly.
    if (bid.pid.epoch > producer->id().epoch()) {
        producer->reset_with_new_epoch(bid.pid.epoch);
    }
    auto request = producer->try_emplace_request(bid, synced_term);
    if (!request) {
        co_return request.error();
    }
    auto req_ptr = request.value();
    if (req_ptr->state() != request_state::initialized) {
        // request already in progress/ completed.
        co_return co_await req_ptr->result();
    }

    req_ptr->mark_request_in_progress();
    auto stages = _raft->replicate_in_stages(synced_term, std::move(br), opts);
    auto req_enqueued = co_await ss::coroutine::as_future(
      std::move(stages.request_enqueued));
    if (req_enqueued.failed()) {
        vlog(
          _ctx_log.warn,
          "replication failed, request enqueue returned error: {}",
          req_enqueued.get_exception());
        req_ptr->set_error(cluster::errc::replication_error);
        co_return cluster::errc::replication_error;
    }
    units.return_all();
    enqueued->set_value();
    auto replicated = co_await ss::coroutine::as_future(
      std::move(stages.replicate_finished));
    if (replicated.failed()) {
        vlog(
          _ctx_log.warn, "replication failed: {}", replicated.get_exception());
        req_ptr->set_error(cluster::errc::replication_error);
        co_return cluster::errc::replication_error;
    }
    auto result = replicated.get0();
    if (result.has_error()) {
        vlog(_ctx_log.warn, "replication failed: {}", result.error());
        req_ptr->set_error(result.error());
        co_return result.error();
    }
    // translate to kafka offset.
    auto kafka_offset = from_log_offset(result.value().last_offset);
    auto final_result = kafka_result{.last_offset = kafka_offset};
    req_ptr->set_value(final_result);
    co_return final_result;
}

ss::future<result<kafka_result>> rm_stm::idempotent_replicate(
  model::batch_identity bid,
  model::record_batch_reader br,
  raft::replicate_options opts,
  ss::lw_shared_ptr<available_promise<>> enqueued) {
    if (!co_await sync(_sync_timeout)) {
        // it's ok not to set enqueued on early return because
        // the safety check in replicate_in_stages sets it automatically
        co_return cluster::errc::not_leader;
    }
    try {
        auto synced_term = _insync_term;
        auto producer = maybe_create_producer(bid.pid);
        co_return co_await producer->run_with_lock(
          [&](ssx::semaphore_units units) {
              return idempotent_replicate(
                synced_term,
                producer,
                bid,
                std::move(br),
                opts,
                std::move(enqueued),
                std::move(units));
          });
    } catch (const cache_full_error& e) {
        vlog(
          _ctx_log.warn,
          "unable to register producer {} with vcluster: {} - {}",
          bid.pid,
          _vcluster_id,
          e.what());
        enqueued->set_value();
        co_return cluster::errc::producer_ids_vcluster_limit_exceeded;
    }
}

ss::future<result<kafka_result>> rm_stm::replicate_msg(
  model::record_batch_reader br,
  raft::replicate_options opts,
  ss::lw_shared_ptr<available_promise<>> enqueued) {
    using ret_t = result<kafka_result>;

    if (!co_await sync(_sync_timeout)) {
        co_return cluster::errc::not_leader;
    }

    auto ss = _raft->replicate_in_stages(_insync_term, std::move(br), opts);
    co_await std::move(ss.request_enqueued);
    enqueued->set_value();
    auto r = co_await std::move(ss.replicate_finished);

    if (!r) {
        co_return ret_t(r.error());
    }
    auto old_offset = r.value().last_offset;
    auto new_offset = from_log_offset(old_offset);
    co_return ret_t(kafka_result{new_offset});
}

model::offset rm_stm::last_stable_offset() {
    // There are two main scenarios we deal with here.
    // 1. stm is still bootstrapping
    // 2. stm is past bootstrapping.
    //
    // We distinguish between (1) and (2) based on the offset
    // we save during first apply (_bootstrap_committed_offset).

    // We always want to return only the `applied` state as it
    // contains aborted transactions metadata that is consumed by
    // the client to distinguish aborted data batches.
    //
    // We optimize for the case where there are no inflight transactional
    // batches to return the high water mark.
    auto last_applied = last_applied_offset();
    if (unlikely(
          !_bootstrap_committed_offset
          || last_applied < _bootstrap_committed_offset.value())) {
        // To preserve the monotonicity of LSO from a client perspective,
        // we return this unknown offset marker that is translated to
        // an appropriate retry-able Kafka error code for clients.
        return model::invalid_lso;
    }

    // Check for any in-flight transactions.
    auto first_tx_start = model::offset::max();
    if (_is_tx_enabled && !_active_tx_producers.empty()) {
        const auto& earliest_open_tx_producer = _active_tx_producers.begin();
        const auto& tx_state = earliest_open_tx_producer->transaction_state();
        vassert(
          tx_state,
          "invalid transaction state tracking: {}",
          *earliest_open_tx_producer);
        first_tx_start = tx_state->first;
    }

    auto synced_leader = _raft->is_leader() && _raft->term() == _insync_term;
    model::offset lso{-1};
    auto last_visible_index = _raft->last_visible_index();
    auto next_to_apply = model::next_offset(last_applied);
    if (first_tx_start <= last_visible_index) {
        // There are in flight transactions < high water mark that may
        // not be applied yet. We still need to consider only applied
        // transactions.
        lso = std::min(first_tx_start, next_to_apply);
    } else if (synced_leader) {
        // no inflight transactions in (last_applied, last_visible_index]
        lso = model::next_offset(last_visible_index);
    } else {
        // a follower or hasn't synced yet leader doesn't know about the
        // txes in the (last_applied, last_visible_index] range so we
        // should not advance lso beyond last_applied
        lso = next_to_apply;
    }
    _last_known_lso = std::max(_last_known_lso, lso);
    return _last_known_lso;
}

static void filter_intersecting(
  fragmented_vector<tx_range>& target,
  const fragmented_vector<tx_range>& source,
  model::offset from,
  model::offset to) {
    for (auto& range : source) {
        if (range.last < from) {
            continue;
        }
        if (range.first > to) {
            continue;
        }
        target.push_back(range);
    }
}

ss::future<fragmented_vector<tx_range>>
rm_stm::aborted_transactions(model::offset from, model::offset to) {
    return _state_lock.hold_read_lock().then(
      [from, to, this](ss::basic_rwlock<>::holder unit) mutable {
          return do_aborted_transactions(from, to).finally(
            [u = std::move(unit)] {});
      });
}

model::producer_id rm_stm::highest_producer_id() const {
    return _highest_producer_id;
}

ss::future<fragmented_vector<tx_range>>
rm_stm::do_aborted_transactions(model::offset from, model::offset to) {
    fragmented_vector<tx_range> result;
    if (!_is_tx_enabled) {
        co_return result;
    }
    fragmented_vector<abort_index> intersecting_idxes;
    for (const auto& idx : _aborted_tx_state.abort_indexes) {
        if (idx.last < from) {
            continue;
        }
        if (idx.first > to) {
            continue;
        }
        if (_aborted_tx_state.last_abort_snapshot.match(idx)) {
            const auto& opt = _aborted_tx_state.last_abort_snapshot;
            filter_intersecting(result, opt.aborted, from, to);
        } else {
            intersecting_idxes.push_back(idx);
        }
    }

    filter_intersecting(result, _aborted_tx_state.aborted, from, to);

    for (const auto& idx : intersecting_idxes) {
        auto opt = co_await load_abort_snapshot(idx);
        if (opt) {
            filter_intersecting(result, opt->aborted, from, to);
        }
    }
    vlog(
      _ctx_log.trace,
      "aborted transactions from: {}, to: {}, result : {}",
      from,
      to,
      result);
    co_return result;
}

ss::future<bool> rm_stm::sync(model::timeout_clock::duration timeout) {
    auto current_insync_term = _insync_term;
    auto ready = co_await persisted_stm::sync(timeout);
    if (ready) {
        if (current_insync_term != _insync_term) {
            _last_known_lso = model::offset{-1};
            vlog(
              _ctx_log.trace,
              "garbage collecting requests from terms < {}",
              _insync_term);
            for (auto& [_, producer] : _producers) {
                producer->gc_requests_from_older_terms(_insync_term);
            }
        }
    }
    co_return ready;
}

void rm_stm::abort_old_txes() {
    ssx::spawn_with_gate(_gate, [this] {
        return _state_lock.hold_read_lock().then(
          [this](ss::basic_rwlock<>::holder unit) mutable {
              return do_abort_old_txes().then(
                [this, u = std::move(unit)](auto next_expiration_ms) {
                    next_expiration_ms = std::min(
                      next_expiration_ms, _abort_interval_ms);
                    maybe_rearm_autoabort_timer(
                      clock_type::now() + next_expiration_ms);
                });
          });
    });
}

std::pair<chunked_vector<tx::producer_ptr>, std::chrono::milliseconds>
rm_stm::get_expired_producers() const {
    chunked_vector<tx::producer_ptr> result;
    std::chrono::milliseconds min_pending_expiration
      = std::chrono::milliseconds::max();
    for (const auto& producer : _active_tx_producers) {
        auto it = _producers.find(producer.id().get_id());
        vassert(
          it != _producers.end(),
          "No entry for {} in _producers, incorrect state tracking",
          producer);
        if (producer.has_transaction_expired()) {
            result.push_back(it->second);
        } else {
            const auto& tx_state = producer.transaction_state();
            vassert(
              tx_state,
              "No transaction state for in progress transaction: {}",
              producer);
            auto ms_to_expire = tx_state->timeout_ms()
                                - producer.ms_since_last_update();
            min_pending_expiration = std::min(
              ms_to_expire, min_pending_expiration);
        }
    }
    vlog(
      _ctx_log.trace,
      "expired producers: {}, next expiration duration ms: {}",
      result.size(),
      min_pending_expiration.count());
    return std::make_pair(
      std::move(result), std::max(min_pending_expiration, 200ms));
}

ss::future<std::chrono::milliseconds> rm_stm::do_abort_old_txes() {
    if (!co_await sync(_sync_timeout)) {
        co_return std::chrono::milliseconds::max();
    }
    if (!_is_autoabort_enabled) {
        co_return std::chrono::milliseconds::max();
    }
    const auto [expired, next_expiration_ms] = get_expired_producers();
    co_await ss::max_concurrent_for_each(
      expired, 5, [this](const tx::producer_ptr& producer) {
          return try_abort_old_tx(producer);
      });
    co_return next_expiration_ms;
}

ss::future<> rm_stm::try_abort_old_tx(producer_ptr producer) {
    co_await producer->run_with_lock(
      [this, producer](ssx::semaphore_units units) {
          return do_try_abort_old_tx(producer).finally(
            [units = std::move(units)] {});
      });
}

ss::future<tx::errc> rm_stm::do_try_abort_old_tx(producer_ptr producer) {
    if (!co_await sync(_sync_timeout)) {
        co_return tx::errc::leader_not_found;
    }
    auto synced_term = _insync_term;

    const auto& tx_state = producer->transaction_state();

    if (!tx_state || !producer->has_transaction_expired()) {
        vlog(_ctx_log.debug, "[{}] no transaction to expire", *producer);
        co_return tx::errc::stale;
    }

    auto pid = producer->id();
    model::tx_seq tx_seq = tx_state->sequence;
    if (tx_seq() >= 0) {
        vlog(_ctx_log.trace, "trying to expire transaction: {}", *producer);
        auto r = co_await _tx_gateway_frontend.local().route_globally(
          cluster::try_abort_request(
            tx_state->coordinator_partition, pid, tx_seq, _sync_timeout));
        if (r.ec == tx::errc::none) {
            if (r.commited) {
                vlog(
                  _ctx_log.trace, "pid:{} tx_seq:{} is committed", pid, tx_seq);
                auto batch = make_tx_control_batch(
                  pid, model::control_record_type::tx_commit);
                auto reader = model::make_memory_record_batch_reader(
                  std::move(batch));
                auto cr = co_await _raft->replicate(
                  synced_term, std::move(reader), make_replicate_options());
                if (!cr) {
                    vlog(
                      _ctx_log.warn,
                      "Error \"{}\" on replicating pid:{} tx_seq:{} "
                      "autoabort/commit batch",
                      cr.error(),
                      pid,
                      tx_seq);
                    if (_raft->is_leader() && _raft->term() == synced_term) {
                        co_await _raft->step_down(
                          "try_abort(commit) replication error");
                    }
                    co_return tx::errc::unknown_server_error;
                }

                if (!co_await wait_no_throw(
                      cr.value().last_offset,
                      model::timeout_clock::now() + _sync_timeout)) {
                    vlog(
                      _ctx_log.warn,
                      "Timed out on waiting for the commit marker to be "
                      "applied pid:{} tx_seq:{}",
                      pid,
                      tx_seq);
                    if (_raft->is_leader() && _raft->term() == synced_term) {
                        co_await _raft->step_down(
                          "try_abort(commit) apply error");
                    }
                    co_return tx::errc::timeout;
                }
                co_return tx::errc::none;
            } else if (r.aborted) {
                vlog(
                  _ctx_log.trace, "pid:{} tx_seq:{} is aborted", pid, tx_seq);
                auto batch = make_tx_control_batch(
                  pid, model::control_record_type::tx_abort);
                auto reader = model::make_memory_record_batch_reader(
                  std::move(batch));
                auto cr = co_await _raft->replicate(
                  synced_term, std::move(reader), make_replicate_options());
                if (!cr) {
                    vlog(
                      _ctx_log.warn,
                      "Error \"{}\" on replicating pid:{} tx_seq:{} "
                      "autoabort/abort "
                      "batch",
                      cr.error(),
                      pid,
                      tx_seq);
                    if (_raft->is_leader() && _raft->term() == synced_term) {
                        co_await _raft->step_down(
                          "try_abort(abort) replication error");
                    }
                    co_return tx::errc::unknown_server_error;
                }

                if (!co_await wait_no_throw(
                      cr.value().last_offset,
                      model::timeout_clock::now() + _sync_timeout)) {
                    vlog(
                      _ctx_log.warn,
                      "Timed out on waiting for the abort marker to be applied "
                      "for transaction: {}",
                      *producer);
                    if (_raft->is_leader() && _raft->term() == synced_term) {
                        co_await _raft->step_down(
                          "try_abort(abort) apply error");
                    }
                    co_return tx::errc::timeout;
                }
                co_return tx::errc::none;
            } else {
                co_return tx::errc::timeout;
            }
        } else {
            vlog(
              _ctx_log.warn,
              "state of transaction {} is unknown:{}",
              *producer,
              r.ec);
            co_return tx::errc::timeout;
        }
    } else {
        vlog(
          _ctx_log.error,
          "Invalid sequence number for transaction: {}, force expiring",
          *producer);
        auto batch = make_tx_control_batch(
          pid, model::control_record_type::tx_abort);

        auto reader = model::make_memory_record_batch_reader(std::move(batch));
        auto cr = co_await _raft->replicate(
          _insync_term, std::move(reader), make_replicate_options());

        if (!cr) {
            vlog(
              _ctx_log.warn,
              "Error \"{}\" on replicating pid:{} autoabort/abort batch",
              cr.error(),
              pid);
            if (_raft->is_leader() && _raft->term() == synced_term) {
                co_await _raft->step_down("try_abort(abort) replication error");
            }
            co_return tx::errc::unknown_server_error;
        }

        if (!co_await wait_no_throw(
              model::offset(cr.value().last_offset()),
              model::timeout_clock::now() + _sync_timeout)) {
            vlog(
              _ctx_log.trace,
              "timeout on waiting until {} is applied (try_abort_old_tx "
              "pid:{})",
              cr.value().last_offset(),
              pid);
            if (_raft->is_leader() && _raft->term() == synced_term) {
                co_await _raft->step_down("try_abort_old_tx apply error");
            }
            co_return tx::errc::timeout;
        }
        co_return tx::errc::none;
    }
}

void rm_stm::maybe_rearm_autoabort_timer(time_point_type deadline) {
    if (auto_abort_timer.armed() && auto_abort_timer.get_timeout() > deadline) {
        auto_abort_timer.cancel();
        auto_abort_timer.arm(deadline);
    } else if (!auto_abort_timer.armed()) {
        auto_abort_timer.arm(deadline);
    }
}

void rm_stm::apply_fence(model::producer_identity pid, model::record_batch b) {
    auto producer = maybe_create_producer(pid);
    auto header = b.header();
    auto batch_data = read_fence_batch(std::move(b));
    vlog(
      _ctx_log.trace,
      "applying fence batch, offset: {}, pid: {}",
      b.base_offset(),
      batch_data.bid.pid);
    producer->apply_transaction_begin(header, batch_data);
    _highest_producer_id = std::max(
      _highest_producer_id, batch_data.bid.pid.get_id());
    if (batch_data.transaction_timeout_ms.has_value()) {
        maybe_rearm_autoabort_timer(
          clock_type::now() + batch_data.transaction_timeout_ms.value());
    }
    _active_tx_producers.push_back(*producer);
    _producer_state_manager.local().touch(*producer, _vcluster_id);
}

ss::future<> rm_stm::apply(const model::record_batch& b) {
    const auto& hdr = b.header();
    const auto bid = model::batch_identity::from(hdr);

    if (hdr.type == model::record_batch_type::tx_fence) {
        apply_fence(bid.pid, b.copy());
    } else if (hdr.type == model::record_batch_type::tx_prepare) {
        // prepare phase was used pre-transactions GA. Ideally these
        // batches should not appear anymore and should not be a part
        // of Redpanda deployments from the recent past. Still logging
        // it at warn for debugging.
        vlog(
          _ctx_log.warn,
          "Ignored prepare batch at offset: {} from producer: {}",
          b.base_offset(),
          b.header().producer_id);
    } else if (hdr.type == model::record_batch_type::raft_data) {
        if (hdr.attrs.is_control()) {
            apply_control(bid.pid, parse_control_batch(b));
        } else {
            apply_data(bid, hdr);
        }
    }
    co_return;
}

void rm_stm::apply_control(
  model::producer_identity pid, model::control_record_type crt) {
    vlog(
      _ctx_log.trace, "applying control batch of type {}, pid: {}", crt, pid);
    auto producer = maybe_create_producer(pid);
    auto tx_range = producer->apply_transaction_end(crt);
    if (tx_range && crt == model::control_record_type::tx_abort) {
        // Aborted transaction
        vlog(
          _ctx_log.trace,
          "Adding aborted transaction range: {}",
          tx_range.value());
        _aborted_tx_state.aborted.push_back(tx_range.value());
        if (
          _aborted_tx_state.aborted.size() > _abort_index_segment_size
          && !_is_abort_idx_reduction_requested) {
            ssx::spawn_with_gate(
              _gate, [this] { return reduce_aborted_list(); });
        }
    }
    _producer_state_manager.local().touch(*producer, _vcluster_id);
    if (producer->_active_transaction_hook.is_linked()) {
        // the producer may be unlinked if only the abort batch is retained
        // from the transaction and everything else got truncated.
        auto it = _active_tx_producers.iterator_to(*producer);
        vassert(
          it != _active_tx_producers.end(),
          "Invalid transacation state tracking, {} is not in active producers "
          "list",
          *producer);
        _active_tx_producers.erase(it);
    }
    _highest_producer_id = std::max(_highest_producer_id, pid.get_id());
}

ss::future<> rm_stm::reduce_aborted_list() {
    if (_is_abort_idx_reduction_requested) {
        return ss::now();
    }
    if (_aborted_tx_state.aborted.size() <= _abort_index_segment_size) {
        return ss::now();
    }
    _is_abort_idx_reduction_requested = true;
    return write_local_snapshot().finally(
      [this] { _is_abort_idx_reduction_requested = false; });
}

void rm_stm::apply_data(
  model::batch_identity bid, const model::record_batch_header& header) {
    if (bid.is_idempotent()) {
        _highest_producer_id = std::max(_highest_producer_id, bid.pid.get_id());
        const auto last_kafka_offset = from_log_offset(header.last_offset());
        auto producer = maybe_create_producer(bid.pid);
        producer->apply_data(header, last_kafka_offset);
        _producer_state_manager.local().touch(*producer, _vcluster_id);
        if (
          bid.is_transactional
          && !producer->_active_transaction_hook.is_linked()) {
            // This can happen if the begin batch was prefix truncated.
            _active_tx_producers.push_back(*producer);
        }
    }
}

kafka::offset rm_stm::from_log_offset(model::offset log_offset) const {
    if (log_offset > model::offset{-1}) {
        return kafka::offset(_raft->log()->from_log_offset(log_offset));
    }
    return kafka::offset(log_offset);
}

model::offset rm_stm::to_log_offset(kafka::offset k_offset) const {
    if (k_offset > kafka::offset{-1}) {
        return _raft->log()->to_log_offset(model::offset(k_offset()));
    }

    return model::offset(k_offset);
}

ss::future<>
rm_stm::apply_local_snapshot(raft::stm_snapshot_header hdr, iobuf&& tx_ss_buf) {
    vlog(
      _ctx_log.trace,
      "applying snapshot with last included offset: {}",
      hdr.offset);
    tx_snapshot_v6 data;
    iobuf_parser data_parser(std::move(tx_ss_buf));
    if (hdr.version == tx_snapshot_v4::version) {
        tx_snapshot_v4 data_v4
          = co_await reflection::async_adl<tx_snapshot_v4>{}.from(data_parser);
        data = tx_snapshot_v6(
          tx_snapshot_v5(std::move(data_v4), _raft->group()), _raft->group());
    } else if (hdr.version == tx_snapshot_v5::version) {
        data = tx_snapshot_v6(
          co_await reflection::async_adl<tx_snapshot_v5>{}.from(data_parser),
          _raft->group());
    } else if (hdr.version == tx_snapshot_v6::version) {
        data = co_await serde::read_async<tx_snapshot_v6>(data_parser);
    } else {
        vassert(
          false, "unsupported tx_snapshot_header version {}", hdr.version);
    }

    _highest_producer_id = std::max(
      data.highest_producer_id, _highest_producer_id);
    _aborted_tx_state.aborted = std::move(data.aborted);
    co_await ss::max_concurrent_for_each(
      data.abort_indexes, 32, [this](const abort_index& idx) -> ss::future<> {
          auto f_name = abort_idx_name(idx.first, idx.last);
          return _abort_snapshot_mgr.get_snapshot_size(f_name).then(
            [this, idx](uint64_t snapshot_size) {
                _abort_snapshot_sizes.emplace(
                  std::make_pair(idx.first, idx.last), snapshot_size);
            });
      });
    _aborted_tx_state.abort_indexes = std::move(data.abort_indexes);

    co_await reset_producers();

    vlog(_ctx_log.debug, "Loading snapshot: {}", data);
    chunked_vector<producer_ptr> transactional_producers;
    for (auto& entry : data.producers) {
        auto it = _producers.find(entry.id.get_id());
        if (it != _producers.end()) {
            // This is impossible because the map is keyed on producer_id,
            // when the snapshot is built.
            vlog(
              _ctx_log.error,
              "Duplicate producer state in snapshot: {}, skipping",
              *(it->second));
            continue;
        }
        auto pid = entry.id;
        auto producer = ss::make_lw_shared<producer_state>(
          _ctx_log,
          [pid, this] { cleanup_producer_state(pid); },
          std::move(entry));
        if (producer->has_transaction_in_progress()) {
            transactional_producers.push_back(producer);
        }
        try {
            _producer_state_manager.local().register_producer(
              *producer, _vcluster_id);
            _producers.emplace(pid.get_id(), producer);
        } catch (const cache_full_error& e) {
            vlog(
              _ctx_log.warn,
              "unable to register producer {} with vcluster: {} - {}",
              pid,
              _vcluster_id,
              e.what());
        }
    }

    std::sort(
      std::begin(transactional_producers),
      std::end(transactional_producers),
      [](producer_ptr a, producer_ptr b) {
          vassert(a->transaction_state(), "Invalid transaction state: {}", *a);
          vassert(b->transaction_state(), "Invalid transaction state: {}", *b);
          return a->transaction_state()->first < b->transaction_state()->first;
      });

    for (auto& producer : transactional_producers) {
        vlog(
          _ctx_log.trace,
          "adding transactional producer: {} from snapshot",
          *producer);
        _active_tx_producers.push_back(*producer);
    }
    transactional_producers.clear();

    abort_index last{model::offset{}, model::offset(-1)};
    for (auto& entry : _aborted_tx_state.abort_indexes) {
        if (entry.last > last.last) {
            last = entry;
        }
    }
    if (last.last > model::offset(0)) {
        auto snapshot_opt = co_await load_abort_snapshot(last);
        if (snapshot_opt) {
            _aborted_tx_state.last_abort_snapshot = std::move(
              snapshot_opt.value());
        }
    }
}

uint8_t rm_stm::active_snapshot_version() {
    if (_feature_table.local().is_active(features::feature::unified_tx_state)) {
        return tx_snapshot_v6::version;
    }
    return tx_snapshot_v5::version;
}

ss::future<> rm_stm::offload_aborted_txns() {
    // Note: this method requires a consistent view of aborted state
    // make sure to call this under _state_lock.write_lock()
    std::sort(
      std::begin(_aborted_tx_state.aborted),
      std::end(_aborted_tx_state.aborted),
      [](tx_range a, tx_range b) { return a.first < b.first; });

    abort_snapshot snapshot{
      .first = model::offset::max(), .last = model::offset::min()};
    for (auto const& entry : _aborted_tx_state.aborted) {
        snapshot.first = std::min(snapshot.first, entry.first);
        snapshot.last = std::max(snapshot.last, entry.last);
        snapshot.aborted.push_back(entry);
        if (snapshot.aborted.size() == _abort_index_segment_size) {
            auto idx = abort_index{snapshot.first, snapshot.last};
            _aborted_tx_state.abort_indexes.push_back(idx);
            co_await save_abort_snapshot(std::move(snapshot));
            snapshot = abort_snapshot{
              .first = model::offset::max(), .last = model::offset::min()};
        }
    }
    _aborted_tx_state.aborted = std::move(snapshot.aborted);
}

ss::future<raft::stm_snapshot> rm_stm::take_local_snapshot() {
    return do_take_local_snapshot(active_snapshot_version());
}

ss::future<raft::stm_snapshot> rm_stm::do_take_local_snapshot(uint8_t version) {
    vassert(
      version == tx_snapshot_v5::version || version == tx_snapshot_v6::version,
      "Unsupported snapshot version requested: {}",
      version);

    auto start_offset = _raft->start_offset();
    vlog(
      _ctx_log.trace,
      "taking snapshot with last included offset of: {}",
      last_applied_offset());

    // Get rid of any aborted transactions state, from the part of the log
    // that was prefix truncated.
    fragmented_vector<abort_index> abort_indexes;
    fragmented_vector<abort_index> expired_abort_indexes;

    for (const auto& idx : _aborted_tx_state.abort_indexes) {
        if (idx.last < start_offset) {
            // caching expired indexes instead of removing them as we go
            // to avoid giving control to another coroutine and managing
            // concurrent access to _log_state.abort_indexes
            expired_abort_indexes.push_back(idx);
        } else {
            abort_indexes.push_back(idx);
        }
    }
    _aborted_tx_state.abort_indexes = std::move(abort_indexes);

    vlog(
      _ctx_log.debug,
      "Removing abort indexes {} with offset < {}",
      expired_abort_indexes.size(),
      start_offset);

    co_await ss::max_concurrent_for_each(
      expired_abort_indexes.begin(),
      expired_abort_indexes.end(),
      64,
      [this](const abort_index& idx) {
          auto f_name = abort_idx_name(idx.first, idx.last);
          vlog(
            _ctx_log.debug,
            "removing aborted transactions {} snapshot file",
            f_name);
          _abort_snapshot_sizes.erase(std::make_pair(idx.first, idx.last));
          return _abort_snapshot_mgr.remove_snapshot(f_name);
      });

    fragmented_vector<tx_range> aborted;
    std::copy_if(
      _aborted_tx_state.aborted.begin(),
      _aborted_tx_state.aborted.end(),
      std::back_inserter(aborted),
      [start_offset](tx_range range) { return range.last >= start_offset; });
    _aborted_tx_state.aborted = std::move(aborted);

    if (_aborted_tx_state.aborted.size() > _abort_index_segment_size) {
        // There is nested locking here. Snapshot is done under
        // persisted_stm::_op_lock and we grab a write lock to stop all the
        // writers to aborted state to have a consistent snpahot to offload.
        // Ensure this order is not violated in other places to avoid a
        // deadlock.
        auto units = co_await _state_lock.hold_write_lock();
        co_await offload_aborted_txns();
    }
    kafka::offset start_kafka_offset = from_log_offset(start_offset);
    tx::tx_snapshot_v6 stm_snapshot;
    // aborted transactions state.
    stm_snapshot.abort_indexes = _aborted_tx_state.abort_indexes.copy();
    stm_snapshot.aborted = _aborted_tx_state.aborted.copy();
    stm_snapshot.highest_producer_id = _highest_producer_id;
    // producers state (includes idempotent and transactional producers)
    for (const auto& [_, state] : _producers) {
        auto snapshot = state->snapshot(start_kafka_offset);
        if (!snapshot.finished_requests.empty()) {
            stm_snapshot.producers.push_back(std::move(snapshot));
        }
    }

    vlog(_ctx_log.trace, "Serializing snapshot {}", stm_snapshot);

    iobuf snapshot_buf;
    if (version == tx_snapshot_v6::version) {
        co_await serde::write_async(snapshot_buf, std::move(stm_snapshot));
    } else {
        co_await reflection::async_adl<tx_snapshot_v5>{}.to(
          snapshot_buf, std::move(stm_snapshot).downgrade_to_v5());
    }

    co_return raft::stm_snapshot::create(
      version, last_applied_offset(), std::move(snapshot_buf));
}

uint64_t rm_stm::get_local_snapshot_size() const {
    uint64_t abort_snapshots_size = 0;
    for (const auto& snapshot_size : _abort_snapshot_sizes) {
        abort_snapshots_size += snapshot_size.second;
    }
    vlog(
      clusterlog.trace,
      "rm_stm: aborted snapshots size {}",
      abort_snapshots_size);
    return persisted_stm::get_local_snapshot_size() + abort_snapshots_size;
}

ss::future<> rm_stm::save_abort_snapshot(abort_snapshot snapshot) {
    auto first_offset = snapshot.first;
    auto last_offset = snapshot.last;
    auto filename = abort_idx_name(first_offset, last_offset);
    vlog(_ctx_log.debug, "saving abort snapshot {} at {}", snapshot, filename);
    iobuf snapshot_data;
    reflection::adl<abort_snapshot>{}.to(snapshot_data, std::move(snapshot));
    int32_t snapshot_size = snapshot_data.size_bytes();

    auto writer = co_await _abort_snapshot_mgr.start_snapshot(filename);

    iobuf metadata_buf;
    reflection::serialize(metadata_buf, abort_snapshot_version, snapshot_size);
    co_await writer.write_metadata(std::move(metadata_buf));
    co_await write_iobuf_to_output_stream(
      std::move(snapshot_data), writer.output());
    co_await writer.close();
    co_await _abort_snapshot_mgr.finish_snapshot(writer);
    uint64_t snapshot_disk_size
      = co_await _abort_snapshot_mgr.get_snapshot_size(filename);
    _abort_snapshot_sizes.emplace(
      std::make_pair(first_offset, last_offset), snapshot_disk_size);
}

ss::future<std::optional<abort_snapshot>>
rm_stm::load_abort_snapshot(abort_index index) {
    auto filename = abort_idx_name(index.first, index.last);

    auto reader = co_await _abort_snapshot_mgr.open_snapshot(filename);
    if (!reader) {
        co_return std::nullopt;
    }

    auto meta_buf = co_await reader->read_metadata();
    iobuf_parser meta_parser(std::move(meta_buf));

    auto version = reflection::adl<int8_t>{}.from(meta_parser);
    vassert(
      version == abort_snapshot_version,
      "Only support abort_snapshot_version {} but got {}",
      abort_snapshot_version,
      version);

    auto snapshot_size = reflection::adl<int32_t>{}.from(meta_parser);
    vassert(
      meta_parser.bytes_left() == 0,
      "Not all metadata content of {} were consumed, {} bytes left. "
      "This is an indication of the serialization save/load mismatch",
      filename,
      meta_parser.bytes_left());

    auto data_buf = co_await read_iobuf_exactly(reader->input(), snapshot_size);
    co_await reader->close();
    co_await _abort_snapshot_mgr.remove_partial_snapshots();

    iobuf_parser data_parser(std::move(data_buf));
    auto data = reflection::adl<abort_snapshot>{}.from(data_parser);

    co_return data;
}

ss::future<> rm_stm::remove_persistent_state() {
    // the write lock drains all ongoing operations and prevents
    // modification of _log_state.abort_indexes while we iterate
    // through it
    return _state_lock.hold_write_lock().then(
      [this](ss::basic_rwlock<>::holder unit) {
          return do_remove_persistent_state().finally([u = std::move(unit)] {});
      });
}

ss::future<> rm_stm::do_remove_persistent_state() {
    _abort_snapshot_sizes.clear();
    for (const auto& idx : _aborted_tx_state.abort_indexes) {
        auto filename = abort_idx_name(idx.first, idx.last);
        co_await _abort_snapshot_mgr.remove_snapshot(filename);
    }
    co_await _abort_snapshot_mgr.remove_partial_snapshots();
    co_return co_await persisted_stm::remove_persistent_state();
}

ss::future<> rm_stm::apply_raft_snapshot(const iobuf&) {
    auto units = co_await _state_lock.hold_write_lock();
    vlog(
      _ctx_log.info,
      "Resetting all state, reason: log eviction, offset: {}",
      _raft->start_offset());
    _aborted_tx_state = {};
    co_await reset_producers();
    set_next(_raft->start_offset());
    co_return;
}

void rm_stm::setup_metrics() {
    if (config::shard_local_cfg().disable_metrics()) {
        return;
    }
    namespace sm = ss::metrics;
    auto ns_label = sm::label("namespace");
    auto topic_label = sm::label("topic");
    auto partition_label = sm::label("partition");

    const auto& ntp = _raft->ntp();
    const std::vector<sm::label_instance> labels = {
      ns_label(ntp.ns()),
      topic_label(ntp.tp.topic()),
      partition_label(ntp.tp.partition()),
    };

    _metrics.add_group(
      prometheus_sanitize::metrics_name("tx:partition"),
      {
        sm::make_gauge(
          "idempotency_pid_cache_size",
          [this] { return _producers.size(); },
          sm::description(
            "Number of active producers (known producer_id seq number pairs)."),
          labels),
        sm::make_gauge(
          "tx_num_inflight_requests",
          [this] { return _active_tx_producers.size(); },
          sm::description("Number of ongoing transactional requests."),
          labels),
      },
      {},
      {sm::shard_label, partition_label});
}

rm_stm_factory::rm_stm_factory(
  bool enable_transactions,
  bool enable_idempotence,
  ss::sharded<tx_gateway_frontend>& tx_gateway_frontend,
  ss::sharded<cluster::producer_state_manager>& producer_state_manager,
  ss::sharded<features::feature_table>& feature_table,
  ss::sharded<topic_table>& topics)
  : _enable_transactions(enable_transactions)
  , _enable_idempotence(enable_idempotence)
  , _tx_gateway_frontend(tx_gateway_frontend)
  , _producer_state_manager(producer_state_manager)
  , _feature_table(feature_table)
  , _topics(topics) {}

bool rm_stm_factory::is_applicable_for(const storage::ntp_config& cfg) const {
    const auto& ntp = cfg.ntp();
    const auto enabled = _enable_transactions || _enable_idempotence;

    return enabled && cfg.ntp().ns == model::kafka_namespace
           && ntp.tp.topic != model::kafka_consumer_offsets_nt.tp;
}

void rm_stm_factory::create(

  raft::state_machine_manager_builder& builder, raft::consensus* raft) {
    auto topic_md = _topics.local().get_topic_metadata_ref(
      model::topic_namespace_view(raft->ntp()));

    auto stm = builder.create_stm<cluster::rm_stm>(
      clusterlog,
      raft,
      _tx_gateway_frontend,
      _feature_table,
      _producer_state_manager,
      topic_md.has_value()
        ? topic_md->get().get_configuration().properties.mpx_virtual_cluster_id
        : std::nullopt);

    raft->log()->stm_manager()->add_stm(stm);
}

} // namespace cluster

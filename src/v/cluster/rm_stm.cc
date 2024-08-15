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
#include "cluster/tx_gateway_frontend.h"
#include "cluster/tx_snapshot_utils.h"
#include "cluster/types.h"
#include "kafka/protocol/wire.h"
#include "metrics/metrics.h"
#include "model/fundamental.h"
#include "model/record.h"
#include "model/timestamp.h"
#include "prometheus/prometheus_sanitize.h"
#include "raft/consensus_utils.h"
#include "raft/errc.h"
#include "raft/persisted_stm.h"
#include "raft/state_machine_base.h"
#include "raft/types.h"
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
#include <utility>

namespace cluster {
using namespace std::chrono_literals;

namespace {
ss::sstring abort_idx_name(model::offset first, model::offset last) {
    return fmt::format("abort.idx.{}.{}", first, last);
}

model::record_batch make_tx_control_batch(
  model::producer_identity pid, model::control_record_type crt) {
    iobuf key;
    kafka::protocol::encoder kw(key);
    kw.write(model::current_control_record_version());
    kw.write(static_cast<int16_t>(crt));

    iobuf value;
    kafka::protocol::encoder vw(value);
    vw.write(static_cast<int16_t>(0));
    vw.write(static_cast<int32_t>(0));

    storage::record_batch_builder builder(
      model::record_batch_type::raft_data, model::offset(0));
    builder.set_producer_identity(pid.id, pid.epoch);
    builder.set_control_type();
    builder.set_transactional_type();
    builder.add_raw_kw(
      std::move(key), std::move(value), std::vector<model::record_header>());

    return std::move(builder).build();
}

model::control_record_type parse_control_batch(const model::record_batch& b) {
    const auto& hdr = b.header();

    vassert(
      hdr.type == model::record_batch_type::raft_data,
      "expect data batch type got {}",
      hdr.type);
    vassert(hdr.attrs.is_control(), "expect control attrs got {}", hdr.attrs);
    vassert(
      b.record_count() == 1, "control batch must contain a single record");

    auto r = b.copy_records();
    auto& record = *r.begin();
    auto key = record.release_key();
    kafka::protocol::decoder key_reader(std::move(key));
    auto version = model::control_record_version(key_reader.read_int16());
    vassert(
      version == model::current_control_record_version,
      "unknown control record version");
    return model::control_record_type(key_reader.read_int16());
}

raft::replicate_options make_replicate_options() {
    auto opts = raft::replicate_options(raft::consistency_level::quorum_ack);
    opts.set_force_flush();
    return opts;
}

} // namespace

fence_batch_data read_fence_batch(model::record_batch&& b) {
    const auto& hdr = b.header();
    auto bid = model::batch_identity::from(hdr);

    vassert(
      b.record_count() == 1,
      "model::record_batch_type::tx_fence batch must contain a single record");
    auto r = b.copy_records();
    auto& record = *r.begin();
    auto val_buf = record.release_value();

    iobuf_parser val_reader(std::move(val_buf));
    auto version = reflection::adl<int8_t>{}.from(val_reader);
    vassert(
      version <= rm_stm::fence_control_record_version,
      "unknown fence record version: {} expected: {}",
      version,
      rm_stm::fence_control_record_version);

    std::optional<model::tx_seq> tx_seq{};
    std::optional<std::chrono::milliseconds> transaction_timeout_ms;
    if (version >= rm_stm::fence_control_record_v1_version) {
        tx_seq = reflection::adl<model::tx_seq>{}.from(val_reader);
        transaction_timeout_ms
          = reflection::adl<std::chrono::milliseconds>{}.from(val_reader);
    }
    model::partition_id tm{model::legacy_tm_ntp.tp.partition};
    if (version >= rm_stm::fence_control_record_version) {
        tm = reflection::adl<model::partition_id>{}.from(val_reader);
    }

    auto key_buf = record.release_key();
    iobuf_parser key_reader(std::move(key_buf));
    auto batch_type = reflection::adl<model::record_batch_type>{}.from(
      key_reader);
    vassert(
      hdr.type == batch_type,
      "broken model::record_batch_type::tx_fence batch. expected batch type {} "
      "got: {}",
      hdr.type,
      batch_type);
    auto p_id = model::producer_id(reflection::adl<int64_t>{}.from(key_reader));
    vassert(
      p_id == bid.pid.id,
      "broken model::record_batch_type::tx_fence batch. expected pid {} got: "
      "{}",
      bid.pid.id,
      p_id);
    return fence_batch_data{bid, tx_seq, transaction_timeout_ms, tm};
}

model::record_batch make_fence_batch_v1(
  model::producer_identity pid,
  model::tx_seq tx_seq,
  std::chrono::milliseconds transaction_timeout_ms) {
    iobuf key;
    auto pid_id = pid.id;
    reflection::serialize(key, model::record_batch_type::tx_fence, pid_id);

    iobuf value;
    reflection::serialize(
      value,
      rm_stm::fence_control_record_v1_version,
      tx_seq,
      transaction_timeout_ms);

    storage::record_batch_builder builder(
      model::record_batch_type::tx_fence, model::offset(0));
    builder.set_producer_identity(pid.id, pid.epoch);
    builder.set_control_type();
    builder.add_raw_kv(std::move(key), std::move(value));

    return std::move(builder).build();
}

model::record_batch make_fence_batch_v2(
  model::producer_identity pid,
  model::tx_seq tx_seq,
  std::chrono::milliseconds transaction_timeout_ms,
  model::partition_id tm) {
    iobuf key;
    auto pid_id = pid.id;
    reflection::serialize(key, model::record_batch_type::tx_fence, pid_id);

    iobuf value;
    // the key byte representation must not change because it's used in
    // compaction
    reflection::serialize(
      value,
      rm_stm::fence_control_record_version,
      tx_seq,
      transaction_timeout_ms,
      tm);

    storage::record_batch_builder builder(
      model::record_batch_type::tx_fence, model::offset(0));
    builder.set_producer_identity(pid.id, pid.epoch);
    builder.set_control_type();
    builder.add_raw_kv(std::move(key), std::move(value));

    return std::move(builder).build();
}

model::record_batch rm_stm::make_fence_batch(
  model::producer_identity pid,
  model::tx_seq tx_seq,
  std::chrono::milliseconds transaction_timeout_ms,
  model::partition_id tm) {
    if (is_transaction_partitioning()) {
        return make_fence_batch_v2(pid, tx_seq, transaction_timeout_ms, tm);
    }
    return make_fence_batch_v1(pid, tx_seq, transaction_timeout_ms);
}

model::control_record_type
rm_stm::parse_tx_control_batch(const model::record_batch& b) {
    return parse_control_batch(b);
}

void rm_stm::log_state::forget(const model::producer_identity& pid) {
    auto it = fence_pid_epoch.find(pid.get_id());
    if (it != fence_pid_epoch.end() && it->second == pid.get_epoch()) {
        fence_pid_epoch.erase(pid.get_id());
    }
    ongoing_map.erase(pid);
    current_txes.erase(pid);
    expiration.erase(pid);
}

void rm_stm::log_state::reset() {
    fence_pid_epoch.clear();
    ongoing_map.clear();
    ongoing_set.clear();
    current_txes.clear();
    expiration.clear();
    aborted.clear();
    abort_indexes.clear();
    last_abort_snapshot = {model::offset(-1)};
}

rm_stm::rm_stm(
  ss::logger& logger,
  raft::consensus* c,
  ss::sharded<cluster::tx_gateway_frontend>& tx_gateway_frontend,
  ss::sharded<features::feature_table>& feature_table,
  ss::sharded<producer_state_manager>& producer_state_manager,
  std::optional<model::vcluster_id> vcluster_id)
  : raft::persisted_stm<>(rm_stm_snapshot, logger, c)
  , _tx_locks(
      mt::
        map<absl::flat_hash_map, model::producer_id, ss::lw_shared_ptr<mutex>>(
          _tx_root_tracker.create_child("tx-locks")))
  , _log_state(_tx_root_tracker)
  , _mem_state(_tx_root_tracker)
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
  , _log_stats_interval_s(
      config::shard_local_cfg().tx_log_stats_interval_s.bind())
  , _ctx_log(txlog, ssx::sformat("[{}]", c->ntp()))
  , _producer_state_manager(producer_state_manager)
  , _vcluster_id(vcluster_id)
  , _producers(
      mt::map<absl::btree_map, model::producer_identity, cluster::producer_ptr>(
        _tx_root_tracker.create_child("producers"))) {
    vassert(
      _feature_table.local().is_active(features::feature::transaction_ga),
      "unexpected state for transactions support. skipped a few "
      "versions during upgrade?");
    setup_metrics();
    if (!_is_tx_enabled) {
        _is_autoabort_enabled = false;
    }
    auto_abort_timer.set_callback([this] { abort_old_txes(); });
    _log_stats_timer.set_callback([this] { log_tx_stats(); });
    _log_stats_timer.arm(clock_type::now() + _log_stats_interval_s());

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

std::pair<producer_ptr, rm_stm::producer_previously_known>
rm_stm::maybe_create_producer(model::producer_identity pid) {
    // Double lookup because of two reasons
    // 1. we are forced to use a ptr as map value_type because producer_state is
    // not movable
    // 2. btree_map does not support lazy_emplace and we are stuck to btree_map
    // as it is memory friendly.
    auto it = _producers.find(pid);
    if (it != _producers.end()) {
        return std::make_pair(it->second, producer_previously_known::yes);
    }
    auto producer = ss::make_lw_shared<producer_state>(
      pid, _raft->group(), [pid, this] { cleanup_producer_state(pid); });
    _producer_state_manager.local().register_producer(*producer, _vcluster_id);
    _producers.emplace(pid, producer);

    return std::make_pair(producer, producer_previously_known::no);
}

void rm_stm::cleanup_producer_state(model::producer_identity pid) {
    // If this lock is not being held in the map, then we can clean it up.
    // Otherwise assume a later epoch of the same producer is using the lock.
    auto lock_it = _tx_locks.find(pid.get_id());
    if (lock_it != _tx_locks.end() && lock_it->second->ready()) {
        lock_it->second->broken();
        _tx_locks.erase(lock_it);
    }

    if (!_log_state.current_txes.contains(pid)) {
        // No active transactions for this producer.
        // note: this branch can be removed once we port tx state
        // into producer_state.
        _mem_state.forget(pid);
        _log_state.forget(pid);
    }
    _producers.erase(pid);
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
    _producers.clear();
}

ss::future<checked<model::term_id, tx_errc>> rm_stm::begin_tx(
  model::producer_identity pid,
  model::tx_seq tx_seq,
  std::chrono::milliseconds transaction_timeout_ms,
  model::partition_id tm) {
    return _state_lock.hold_read_lock().then(
      [this, pid, tx_seq, transaction_timeout_ms, tm](
        ss::basic_rwlock<>::holder unit) mutable {
          return get_tx_lock(pid.get_id())
            ->with([this, pid, tx_seq, transaction_timeout_ms, tm]() {
                return do_begin_tx(pid, tx_seq, transaction_timeout_ms, tm);
            })
            .finally([u = std::move(unit)] {});
      });
}

ss::future<checked<model::term_id, tx_errc>> rm_stm::do_begin_tx(
  model::producer_identity pid,
  model::tx_seq tx_seq,
  std::chrono::milliseconds transaction_timeout_ms,
  model::partition_id tm) {
    if (!check_tx_permitted()) {
        co_return tx_errc::request_rejected;
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
        co_return tx_errc::leader_not_found;
    }

    if (!co_await sync(_sync_timeout)) {
        vlog(
          _ctx_log.trace,
          "processing name:begin_tx pid:{}, tx_seq:{}, "
          "timeout:{}, coordinator:{} => "
          "stale leader",
          pid,
          tx_seq,
          transaction_timeout_ms,
          tm);
        co_return tx_errc::stale;
    }
    auto synced_term = _insync_term;

    vlog(
      _ctx_log.trace,
      "processing name:begin_tx pid:{}, tx_seq:{}, timeout:{}, coordinator:{}  "
      "in term:{}",
      pid,
      tx_seq,
      transaction_timeout_ms,
      tm,
      synced_term);
    // checking / setting pid fencing
    auto fence_it = _log_state.fence_pid_epoch.find(pid.get_id());
    if (fence_it == _log_state.fence_pid_epoch.end()) {
        // intentionally empty
    } else if (pid.get_epoch() > fence_it->second) {
        auto old_pid = model::producer_identity{pid.get_id(), fence_it->second};
        // there is a fence, it might be that tm_stm failed, forget about
        // an ongoing transaction, assigned next pid for the same tx.id and
        // started a new transaction without aborting the previous one.
        //
        // at the same time it's possible that it already aborted the old
        // tx before starting this. do_abort_tx is idempotent so calling it
        // just in case to proactivly abort the tx instead of waiting for
        // the timeout
        //
        // moreover do_abort_tx is co-idempotent with do_commit_tx so if a
        // tx was committed calling do_abort_tx will do nothing
        auto ar = co_await do_abort_tx(old_pid, std::nullopt, _sync_timeout);
        if (ar != tx_errc::none) {
            vlog(
              _ctx_log.trace,
              "can't begin tx {} because abort of a prev tx {} failed with {}; "
              "retrying",
              pid,
              old_pid,
              ar);
            co_return tx_errc::stale;
        }

        if (is_known_session(old_pid)) {
            vlog(
              _ctx_log.warn,
              "can't begin a tx {}: an aborted tx should have disappeared",
              pid);
            // can't begin a transaction while previous tx is in progress
            co_return tx_errc::unknown_server_error;
        }
        // we want to replicate tx_fence batch on every transaction so
        // intentionally dropping through
    } else if (pid.get_epoch() < fence_it->second) {
        vlog(
          _ctx_log.error,
          "pid {} fenced out by epoch {}",
          pid,
          fence_it->second);
        co_return tx_errc::fenced;
    }

    auto txseq_it = _log_state.current_txes.find(pid);
    if (txseq_it != _log_state.current_txes.end()) {
        if (txseq_it->second.tx_seq != tx_seq) {
            vlog(
              _ctx_log.warn,
              "can't begin a tx {} with tx_seq {}: a producer id is already "
              "involved in a tx with tx_seq {}",
              pid,
              tx_seq,
              txseq_it->second.tx_seq);
            co_return tx_errc::unknown_server_error;
        }
        if (_log_state.ongoing_map.contains(pid)) {
            vlog(
              _ctx_log.warn,
              "can't begin a tx {} with tx_seq {}: it was already begun and "
              "accepted writes",
              pid,
              tx_seq);
            co_return tx_errc::unknown_server_error;
        }
        co_return synced_term;
    }

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
        co_return tx_errc::timeout;
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
        co_return tx_errc::timeout;
    }

    if (_raft->term() != synced_term) {
        vlog(
          _ctx_log.trace,
          "term changed from {} to {} during fencing pid:{} tx_seq:{}",
          synced_term,
          _raft->term(),
          pid,
          tx_seq);
        co_return tx_errc::leader_not_found;
    }

    auto tx_seq_it = _log_state.current_txes.find(pid);
    if (tx_seq_it == _log_state.current_txes.end()) {
        vlog(
          _ctx_log.error,
          "tx_seqs should be updated after fencing pid:{} tx_seq:{}",
          pid,
          tx_seq);
        co_return tx_errc::unknown_server_error;
    }
    if (tx_seq_it->second.tx_seq != tx_seq) {
        vlog(
          _ctx_log.error,
          "expected tx_seq:{} for pid:{} got {}",
          tx_seq,
          pid,
          tx_seq_it->second.tx_seq);
        co_return tx_errc::unknown_server_error;
    }

    co_return synced_term;
}

ss::future<tx_errc> rm_stm::commit_tx(
  model::producer_identity pid,
  model::tx_seq tx_seq,
  model::timeout_clock::duration timeout) {
    return _state_lock.hold_read_lock().then(
      [this, pid, tx_seq, timeout](ss::basic_rwlock<>::holder unit) mutable {
          return get_tx_lock(pid.get_id())
            ->with([this, pid, tx_seq, timeout]() {
                return do_commit_tx(pid, tx_seq, timeout);
            })
            .finally([u = std::move(unit)] {});
      });
}

ss::future<tx_errc> rm_stm::do_commit_tx(
  model::producer_identity pid,
  model::tx_seq tx_seq,
  model::timeout_clock::duration timeout) {
    vlog(_ctx_log.trace, "commit tx pid: {}, tx sequence: {}", pid, tx_seq);
    if (!check_tx_permitted()) {
        co_return tx_errc::request_rejected;
    }

    // doesn't make sense to fence off a commit because transaction
    // manager has already decided to commit and acked to a client
    if (!co_await sync(timeout)) {
        co_return tx_errc::stale;
    }
    auto synced_term = _insync_term;
    auto fence_it = _log_state.fence_pid_epoch.find(pid.get_id());
    if (fence_it == _log_state.fence_pid_epoch.end()) {
        // begin_tx should have set a fence
        vlog(_ctx_log.warn, "can't commit a tx: unknown pid:{}", pid);
        co_return tx_errc::request_rejected;
    }
    if (pid.get_epoch() != fence_it->second) {
        vlog(
          _ctx_log.error,
          "Can't commit pid:{} - fenced out by epoch {}",
          pid,
          fence_it->second);
        co_return tx_errc::fenced;
    }

    auto tx_seqs_it = _log_state.current_txes.find(pid);
    if (tx_seqs_it != _log_state.current_txes.end()) {
        if (tx_seqs_it->second.tx_seq > tx_seq) {
            // rare situation:
            //   * tm_stm begins (tx_seq+1)
            //   * request on this rm passes but then tm_stm fails and forgets
            //   about this tx
            //   * during recovery tm_stm recommits previous tx (tx_seq)
            // existence of {pid, tx_seq+1} implies {pid, tx_seq} is committed
            vlog(
              _ctx_log.trace,
              "Already commited pid:{} tx_seq:{} - a higher tx_seq:{} was "
              "observed",
              pid,
              tx_seq,
              tx_seqs_it->second.tx_seq);
            co_return tx_errc::none;
        } else if (tx_seq != tx_seqs_it->second.tx_seq) {
            vlog(
              _ctx_log.trace,
              "can't commit pid:{} tx: passed txseq {} doesn't match local {}",
              pid,
              tx_seq,
              tx_seqs_it->second.tx_seq);
            co_return tx_errc::request_rejected;
        }
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
        co_return tx_errc::timeout;
    }
    if (!co_await wait_no_throw(
          r.value().last_offset, model::timeout_clock::now() + timeout)) {
        if (_raft->is_leader() && _raft->term() == synced_term) {
            co_await _raft->step_down("do_commit_tx wait error");
        }
        co_return tx_errc::timeout;
    }

    co_return tx_errc::none;
}

abort_origin rm_stm::get_abort_origin(
  const model::producer_identity& pid, model::tx_seq tx_seq) const {
    auto tx_seq_for_pid = get_tx_seq(pid);
    if (!tx_seq_for_pid) {
        return abort_origin::present;
    }

    if (tx_seq < *tx_seq_for_pid) {
        return abort_origin::past;
    }
    if (*tx_seq_for_pid < tx_seq) {
        return abort_origin::future;
    }

    return abort_origin::present;
}

ss::future<tx_errc> rm_stm::abort_tx(
  model::producer_identity pid,
  model::tx_seq tx_seq,
  model::timeout_clock::duration timeout) {
    return _state_lock.hold_read_lock().then(
      [this, pid, tx_seq, timeout](ss::basic_rwlock<>::holder unit) mutable {
          return get_tx_lock(pid.get_id())
            ->with([this, pid, tx_seq, timeout]() {
                return do_abort_tx(pid, tx_seq, timeout);
            })
            .finally([u = std::move(unit)] {});
      });
}

// abort_tx is invoked strictly after a tx is canceled on the tm_stm
// the purpose of abort is to put tx_range into the list of aborted txes
// and to fence off the old epoch.
// we need to check tx_seq to filter out stale requests
ss::future<tx_errc> rm_stm::do_abort_tx(
  model::producer_identity pid,
  std::optional<model::tx_seq> tx_seq,
  model::timeout_clock::duration timeout) {
    if (!check_tx_permitted()) {
        co_return tx_errc::request_rejected;
    }

    // doesn't make sense to fence off an abort because transaction
    // manager has already decided to abort and acked to a client
    if (!co_await sync(timeout)) {
        vlog(
          _ctx_log.trace,
          "processing name:abort_tx pid:{} tx_seq:{} => stale leader",
          pid,
          tx_seq.value_or(model::tx_seq(-1)));
        co_return tx_errc::stale;
    }
    auto synced_term = _insync_term;
    vlog(
      _ctx_log.trace,
      "processing name:abort_tx pid:{} tx_seq:{} in term:{}",
      pid,
      tx_seq.value_or(model::tx_seq(-1)),
      synced_term);

    if (!is_known_session(pid)) {
        vlog(
          _ctx_log.trace,
          "Isn't known tx pid:{} tx_seq:{}, probably already aborted",
          pid,
          tx_seq.value_or(model::tx_seq(-1)));
        co_return tx_errc::none;
    }

    if (tx_seq) {
        auto origin = get_abort_origin(pid, tx_seq.value());
        if (origin == abort_origin::past) {
            // An abort request has older tx_seq. It may mean than the request
            // was dublicated, delayed and retried later.
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
            auto expiration_it = _log_state.expiration.find(pid);
            if (expiration_it != _log_state.expiration.end()) {
                expiration_it->second.is_expiration_requested = true;
            }
            // spawing abort in the background and returning an error to
            // release locks on the tx coordinator to prevent distributed
            // deadlock
            ssx::spawn_with_gate(
              _gate, [this, pid] { return try_abort_old_tx(pid); });
            vlog(
              _ctx_log.info,
              "abort_tx request pid:{} tx_seq:{} came from the past => "
              "rejecting",
              pid,
              tx_seq.value_or(model::tx_seq(-1)));
            co_return tx_errc::request_rejected;
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
              tx_seq.value());
            co_return tx_errc::request_rejected;
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
          tx_seq.value_or(model::tx_seq(-1)));
        if (_raft->is_leader() && _raft->term() == synced_term) {
            co_await _raft->step_down("abort_tx replication error");
        }
        co_return tx_errc::timeout;
    }

    if (!co_await wait_no_throw(
          r.value().last_offset, model::timeout_clock::now() + timeout)) {
        vlog(
          _ctx_log.trace,
          "timeout on waiting until {} is applied (abort_tx pid:{} tx_seq:{})",
          r.value().last_offset,
          pid,
          tx_seq.value_or(model::tx_seq(-1)));
        if (_raft->is_leader() && _raft->term() == synced_term) {
            co_await _raft->step_down("abort_tx apply error");
        }
        co_return tx_errc::timeout;
    }

    co_return tx_errc::none;
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
        auto pid = bid.pid.get_id();
        auto tx_units = co_await get_tx_lock(pid)->get_units();
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
    _log_stats_timer.cancel();
    co_await _gate.close();
    co_await reset_producers();
    _metrics.clear();
    co_await raft::persisted_stm<>::stop();
}

ss::future<> rm_stm::start() { return persisted_stm::start(); }

std::optional<rm_stm::expiration_info>
rm_stm::get_expiration_info(model::producer_identity pid) const {
    auto it = _log_state.expiration.find(pid);
    if (it == _log_state.expiration.end()) {
        return std::nullopt;
    }

    return it->second;
}

std::optional<int32_t>
rm_stm::get_seq_number(model::producer_identity pid) const {
    auto it = _producers.find(pid);
    if (it == _producers.end()) {
        return std::nullopt;
    }
    return it->second->last_sequence_number();
}

ss::future<result<rm_stm::transaction_set>> rm_stm::get_transactions() {
    if (!co_await sync(_sync_timeout)) {
        co_return errc::not_leader;
    }

    transaction_set ans;

    // When redpanda starts writing the first batch of a transaction it
    // estimates its offset and only when the write passes it updates the offset
    // to the exact value; so for a short period of time (while tx is in the
    // initiating state) lso_bound is the offset of the last operation known at
    // moment the transaction started and when the first tx batch is written
    // it's updated to the first offset of the transaction
    for (auto& [id, offset] : _mem_state.estimated) {
        transaction_info tx_info;
        tx_info.lso_bound = offset;
        tx_info.status = rm_stm::transaction_info::status_t::initiating;
        tx_info.info = get_expiration_info(id);
        tx_info.seq = get_seq_number(id);
        ans.emplace(id, tx_info);
    }

    for (auto& [id, offset] : _log_state.ongoing_map) {
        transaction_info tx_info;
        tx_info.lso_bound = offset.first;
        tx_info.status = transaction_info::status_t::ongoing;
        tx_info.info = get_expiration_info(id);
        tx_info.seq = get_seq_number(id);
        ans.emplace(id, tx_info);
    }

    co_return ans;
}

ss::future<std::error_code> rm_stm::mark_expired(model::producer_identity pid) {
    return _state_lock.hold_read_lock().then(
      [this, pid](ss::basic_rwlock<>::holder unit) mutable {
          return get_tx_lock(pid.get_id())
            ->with([this, pid]() { return do_mark_expired(pid); })
            .finally([u = std::move(unit)] {});
      });
}

ss::future<std::error_code>
rm_stm::do_mark_expired(model::producer_identity pid) {
    if (!co_await sync(_sync_timeout)) {
        co_return std::error_code(tx_errc::leader_not_found);
    }
    if (!is_known_session(pid)) {
        co_return std::error_code(tx_errc::pid_not_found);
    }

    // We should delete information about expiration for pid, because inside
    // try_abort_old_tx it checks is tx expired or not.
    _log_state.expiration.erase(pid);
    co_return std::error_code(co_await do_try_abort_old_tx(pid));
}

ss::future<result<kafka_result>> rm_stm::do_sync_and_transactional_replicate(
  producer_ptr producer,
  model::batch_identity bid,
  model::record_batch_reader rdr,
  ssx::semaphore_units units) {
    if (!co_await sync(_sync_timeout)) {
        vlog(
          _ctx_log.trace,
          "processing name:replicate_tx pid:{} => stale leader",
          bid.pid);
        co_return errc::not_leader;
    }
    auto synced_term = _insync_term;
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
        if (result.error() == errc::sequence_out_of_order) {
            // no need to hold while the barrier is in progress.
            units.return_all();
            auto barrier = co_await _raft->linearizable_barrier();
            if (!barrier) {
                co_return errc::not_leader;
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
    // fencing
    auto fence_it = _log_state.fence_pid_epoch.find(bid.pid.get_id());
    if (fence_it == _log_state.fence_pid_epoch.end()) {
        // begin_tx should have set a fence
        vlog(_ctx_log.warn, "can't find ongoing tx for pid:{}", bid.pid);
        co_return errc::invalid_producer_epoch;
    }
    if (bid.pid.get_epoch() != fence_it->second) {
        vlog(
          _ctx_log.info, "pid:{} is fenced by {}", bid.pid, fence_it->second);
        co_return errc::invalid_producer_epoch;
    }

    if (!_log_state.current_txes.contains(bid.pid)) {
        vlog(_ctx_log.warn, "can't find ongoing tx for pid:{}", bid.pid);
        co_return errc::invalid_producer_epoch;
    }
    auto tx_seq = _log_state.current_txes[bid.pid].tx_seq;
    vlog(_ctx_log.trace, "found tx_seq:{} for pid:{}", tx_seq, bid.pid);

    if (_mem_state.estimated.contains(bid.pid)) {
        // we received second produce request while the first is still
        // being processed.
        vlog(_ctx_log.warn, "Too frequent produce with same pid:{}", bid.pid);
        co_return errc::generic_tx_error;
    }

    // For the first batch of a transaction, reset sequence tracking to handle
    // an edge case where client reuses sequence number after an aborted
    // transaction see https://github.com/redpanda-data/redpanda/pull/5026
    // for details
    bool reset_sequence_tracking = !_log_state.ongoing_map.contains(bid.pid);
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
    _mem_state.estimated[bid.pid] = last_applied_offset();

    auto expiration_it = _log_state.expiration.find(bid.pid);
    if (expiration_it == _log_state.expiration.end()) {
        vlog(_ctx_log.warn, "Can not find expiration info for pid:{}", bid.pid);
        req_ptr->set_error(errc::generic_tx_error);
        co_return errc::generic_tx_error;
    }
    expiration_it->second.last_update = clock_type::now();
    expiration_it->second.is_expiration_requested = false;

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
        req_ptr->set_error(errc::timeout);
        co_return tx_errc::timeout;
    }
    _mem_state.estimated.erase(bid.pid);
    auto result = kafka_result{
      .last_offset = from_log_offset(r.value().last_offset)};
    req_ptr->set_value(result);
    co_return result;
}

ss::future<result<kafka_result>> rm_stm::transactional_replicate(
  model::batch_identity bid, model::record_batch_reader rdr) {
    if (!check_tx_permitted()) {
        co_return errc::generic_tx_error;
    }
    auto [producer, _] = maybe_create_producer(bid.pid);
    co_return co_await producer
      ->run_with_lock([&](ssx::semaphore_units units) {
          return do_sync_and_transactional_replicate(
            producer, bid, std::move(rdr), std::move(units));
      })
      .finally([this, producer] {
          _producer_state_manager.local().touch(*producer, _vcluster_id);
      });
}

ss::future<result<kafka_result>> rm_stm::do_sync_and_idempotent_replicate(
  producer_ptr producer,
  model::batch_identity bid,
  model::record_batch_reader br,
  raft::replicate_options opts,
  ss::lw_shared_ptr<available_promise<>> enqueued,
  ssx::semaphore_units units,
  producer_previously_known known_producer) {
    if (!co_await sync(_sync_timeout)) {
        // it's ok not to set enqueued on early return because
        // the safety check in replicate_in_stages sets it automatically
        co_return errc::not_leader;
    }
    auto synced_term = _insync_term;
    auto result = co_await do_idempotent_replicate(
      synced_term,
      producer,
      bid,
      std::move(br),
      opts,
      std::move(enqueued),
      units,
      known_producer);

    if (!result) {
        vlog(
          _ctx_log.trace,
          "error from do_idempotent_replicate: {}, pid: {}, range: [{}, {}]",
          result.error(),
          bid.pid,
          bid.first_seq,
          bid.last_seq);
        if (result.error() == errc::sequence_out_of_order) {
            // release the lock so it is not held for the duration of the
            // barrier, other requests can make progress if they are
            // in the right sequence.
            units.return_all();
            // Ensure we are actually the leader and request didn't
            // ooosn on a stale state. If we are not the leader return
            // a retryable error code to the client.
            auto barrier = co_await _raft->linearizable_barrier();
            if (!barrier) {
                co_return errc::not_leader;
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
  ssx::semaphore_units& units,
  producer_previously_known known_producer) {
    // Check if the producer bumped the epoch and reset accordingly.
    if (bid.pid.epoch > producer->id().epoch) {
        producer->reset_with_new_epoch(model::producer_epoch{bid.pid.epoch});
    }
    // If the producer is unknown and is producing with a non zero
    // sequence number, it is possible that the producer has been evicted
    // from the broker memory. Instead of rejecting the request, we accept
    // the current sequence and move on. This logic is similar to what
    // Apache Kafka does.
    // Ref:
    // https://github.com/apache/kafka/blob/704476885ffb40cd3bf9b8f5c368c01eaee0a737
    // storage/src/main/java/org/apache/kafka/storage/internals/log/ProducerAppendInfo.java#L135
    auto skip_sequence_checks = !known_producer && bid.first_seq > 0;
    if (unlikely(skip_sequence_checks)) {
        vlog(
          _ctx_log.warn,
          "Accepting batch from unknown producer that likely got evicted: {}, "
          "term: {}",
          bid,
          synced_term);
    }
    auto request = producer->try_emplace_request(
      bid, synced_term, skip_sequence_checks);
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
        req_ptr->set_error(errc::replication_error);
        co_return errc::replication_error;
    }
    units.return_all();
    enqueued->set_value();
    auto replicated = co_await ss::coroutine::as_future(
      std::move(stages.replicate_finished));
    if (replicated.failed()) {
        vlog(
          _ctx_log.warn, "replication failed: {}", replicated.get_exception());
        req_ptr->set_error(errc::replication_error);
        co_return errc::replication_error;
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
    try {
        auto [producer, known_producer] = maybe_create_producer(bid.pid);
        co_return co_await producer
          ->run_with_lock([&, known_producer](ssx::semaphore_units units) {
              return do_sync_and_idempotent_replicate(
                producer,
                bid,
                std::move(br),
                opts,
                std::move(enqueued),
                std::move(units),
                known_producer);
          })
          .finally([this, producer] {
              _producer_state_manager.local().touch(*producer, _vcluster_id);
          });
    } catch (const cache_full_error& e) {
        vlog(
          _ctx_log.warn,
          "unable to register producer {} with vcluster: {} - {}",
          bid.pid,
          _vcluster_id,
          e.what());
        enqueued->set_value();
        co_return errc::producer_ids_vcluster_limit_exceeded;
    }
}

ss::future<result<kafka_result>> rm_stm::replicate_msg(
  model::record_batch_reader br,
  raft::replicate_options opts,
  ss::lw_shared_ptr<available_promise<>> enqueued) {
    using ret_t = result<kafka_result>;

    if (!co_await sync(_sync_timeout)) {
        co_return errc::not_leader;
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
    if (_is_tx_enabled) {
        if (!_log_state.ongoing_set.empty()) {
            first_tx_start = *_log_state.ongoing_set.begin();
        }

        for (auto& entry : _mem_state.estimated) {
            first_tx_start = std::min(first_tx_start, entry.second);
        }
    }

    auto synced_leader = _raft->is_leader() && _raft->term() == _insync_term
                         && _mem_state.term == _insync_term;

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
    _mem_state.last_lso = std::max(_mem_state.last_lso, lso);
    return _mem_state.last_lso;
}

static void filter_intersecting(
  fragmented_vector<rm_stm::tx_range>& target,
  const fragmented_vector<rm_stm::tx_range>& source,
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

ss::future<fragmented_vector<rm_stm::tx_range>>
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

ss::future<fragmented_vector<rm_stm::tx_range>>
rm_stm::do_aborted_transactions(model::offset from, model::offset to) {
    fragmented_vector<rm_stm::tx_range> result;
    if (!_is_tx_enabled) {
        co_return result;
    }
    fragmented_vector<abort_index> intersecting_idxes;
    for (const auto& idx : _log_state.abort_indexes) {
        if (idx.last < from) {
            continue;
        }
        if (idx.first > to) {
            continue;
        }
        if (_log_state.last_abort_snapshot.match(idx)) {
            const auto& opt = _log_state.last_abort_snapshot;
            filter_intersecting(result, opt.aborted, from, to);
        } else {
            intersecting_idxes.push_back(idx);
        }
    }

    filter_intersecting(result, _log_state.aborted, from, to);

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
    auto ready = co_await persisted_stm::sync(timeout);
    if (ready) {
        if (_mem_state.term != _insync_term) {
            _mem_state = mem_state{_tx_root_tracker};
            _mem_state.term = _insync_term;
        }
    }
    co_return ready;
}

void rm_stm::track_tx(
  model::producer_identity pid,
  std::chrono::milliseconds transaction_timeout_ms) {
    if (_gate.is_closed()) {
        return;
    }
    _log_state.expiration[pid] = expiration_info{
      .timeout = transaction_timeout_ms,
      .last_update = clock_type::now(),
      .is_expiration_requested = false};
    if (!_is_autoabort_enabled) {
        return;
    }
    auto deadline = _log_state.expiration[pid].deadline();
    try_arm(deadline);
}

void rm_stm::abort_old_txes() {
    _is_autoabort_active = true;
    ssx::spawn_with_gate(_gate, [this] {
        return _state_lock.hold_read_lock().then(
          [this](ss::basic_rwlock<>::holder unit) mutable {
              return do_abort_old_txes().finally([this, u = std::move(unit)] {
                  try_arm(clock_type::now() + _abort_interval_ms);
              });
          });
    });
}

absl::btree_set<model::producer_identity>
rm_stm::get_expired_producers() const {
    absl::btree_set<model::producer_identity> expired;
    auto maybe_add_to_expired = [&](const auto& pid) {
        if (expired.contains(pid)) {
            return;
        }
        auto it = _log_state.expiration.find(pid);
        if (
          it != _log_state.expiration.end()
          && it->second.is_expired(clock_type::now())) {
            expired.insert(pid);
        }
    };
    for (auto& [pid, _] : _mem_state.estimated) {
        maybe_add_to_expired(pid);
    }
    for (auto& [id, epoch] : _log_state.fence_pid_epoch) {
        maybe_add_to_expired(model::producer_identity{id, epoch});
    }
    for (auto& [pid, _] : _log_state.ongoing_map) {
        maybe_add_to_expired(pid);
    }
    return expired;
}

ss::future<> rm_stm::do_abort_old_txes() {
    if (!co_await sync(_sync_timeout)) {
        co_return;
    }

    auto expired = get_expired_producers();
    for (auto pid : expired) {
        co_await try_abort_old_tx(pid);
    }

    std::optional<time_point_type> earliest_deadline;
    for (auto& [pid, expiration] : _log_state.expiration) {
        if (!is_known_session(pid)) {
            continue;
        }
        auto candidate = expiration.deadline();
        if (earliest_deadline) {
            earliest_deadline = std::min(earliest_deadline.value(), candidate);
        } else {
            earliest_deadline = candidate;
        }
    }

    if (earliest_deadline) {
        auto deadline = std::max(
          earliest_deadline.value(), clock_type::now() + _tx_timeout_delay);
        try_arm(deadline);
    }
}

ss::future<> rm_stm::try_abort_old_tx(model::producer_identity pid) {
    return get_tx_lock(pid.get_id())->with([this, pid]() {
        return do_try_abort_old_tx(pid).discard_result();
    });
}

ss::future<tx_errc> rm_stm::do_try_abort_old_tx(model::producer_identity pid) {
    if (!co_await sync(_sync_timeout)) {
        co_return tx_errc::leader_not_found;
    }
    auto synced_term = _insync_term;
    if (!is_known_session(pid)) {
        co_return tx_errc::pid_not_found;
    }

    auto expiration_it = _log_state.expiration.find(pid);
    if (expiration_it != _log_state.expiration.end()) {
        if (!expiration_it->second.is_expired(clock_type::now())) {
            co_return tx_errc::stale;
        }
    }

    std::optional<model::tx_seq> tx_seq = get_tx_seq(pid);
    if (tx_seq) {
        vlog(_ctx_log.trace, "trying to expire pid:{} tx_seq:{}", pid, tx_seq);
        // It looks like a partition is fixed now but actually partitioning
        // of the tx coordinator isn't support yet so it doesn't matter see
        // https://github.com/redpanda-data/redpanda/issues/6137
        // In order to support it we ned to update begin_tx to accept the id
        // and use the true partition_id here
        auto tx_data = _log_state.current_txes.find(pid);
        model::partition_id tm_partition{
          model::partition_id(model::legacy_tm_ntp.tp.partition)};
        if (tx_data != _log_state.current_txes.end()) {
            tm_partition = tx_data->second.tm_partition;
        }

        auto r = co_await _tx_gateway_frontend.local().route_globally(
          cluster::try_abort_request(
            tm_partition, pid, tx_seq.value(), _sync_timeout));
        if (r.ec == tx_errc::none) {
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
                    co_return tx_errc::unknown_server_error;
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
                    co_return tx_errc::timeout;
                }
                co_return tx_errc::none;
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
                    co_return tx_errc::unknown_server_error;
                }

                if (!co_await wait_no_throw(
                      cr.value().last_offset,
                      model::timeout_clock::now() + _sync_timeout)) {
                    vlog(
                      _ctx_log.warn,
                      "Timed out on waiting for the abort marker to be applied "
                      "pid:{} tx_seq:{}",
                      pid,
                      tx_seq);
                    if (_raft->is_leader() && _raft->term() == synced_term) {
                        co_await _raft->step_down(
                          "try_abort(abort) apply error");
                    }
                    co_return tx_errc::timeout;
                }
                co_return tx_errc::none;
            } else {
                co_return tx_errc::timeout;
            }
        } else {
            vlog(
              _ctx_log.warn,
              "state of pid:{} tx_seq:{} is unknown:{}",
              pid,
              tx_seq,
              r.ec);
            co_return tx_errc::timeout;
        }
    } else {
        vlog(
          _ctx_log.error,
          "Can not find tx_seq for pid({}) to expire old tx",
          pid);
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
            co_return tx_errc::unknown_server_error;
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
            co_return tx_errc::timeout;
        }

        co_return tx_errc::none;
    }
}

void rm_stm::try_arm(time_point_type deadline) {
    if (auto_abort_timer.armed() && auto_abort_timer.get_timeout() > deadline) {
        auto_abort_timer.cancel();
        auto_abort_timer.arm(deadline);
    } else if (!auto_abort_timer.armed()) {
        auto_abort_timer.arm(deadline);
    }
}

void rm_stm::apply_fence(model::record_batch&& b) {
    auto batch_base_offset = b.base_offset();
    auto batch_data = read_fence_batch(std::move(b));
    vlog(
      _ctx_log.trace,
      "applying fence batch, offset: {}, pid: {}",
      batch_base_offset,
      batch_data.bid.pid);

    _highest_producer_id = std::max(
      _highest_producer_id, batch_data.bid.pid.get_id());
    auto [fence_it, _] = _log_state.fence_pid_epoch.try_emplace(
      batch_data.bid.pid.get_id(), batch_data.bid.pid.get_epoch());
    // using less-or-equal to update tx_seqs on every transaction
    if (fence_it->second <= batch_data.bid.pid.get_epoch()) {
        fence_it->second = batch_data.bid.pid.get_epoch();
        if (batch_data.tx_seq.has_value()) {
            _log_state.current_txes[batch_data.bid.pid] = tx_data{
              batch_data.tx_seq.value(), batch_data.tm};
        }
        if (batch_data.transaction_timeout_ms.has_value()) {
            // with switching to log_state an active transaction may
            // survive leadership and we need to start tracking it on
            // the new leader so we can't rely on the begin_tx initi-
            // -ated tracking and need to do it from apply
            track_tx(
              batch_data.bid.pid, batch_data.transaction_timeout_ms.value());
        }
    }
}

ss::future<> rm_stm::apply(const model::record_batch& b) {
    const auto& hdr = b.header();

    if (hdr.type == model::record_batch_type::tx_fence) {
        apply_fence(b.copy());
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
        auto bid = model::batch_identity::from(hdr);
        if (hdr.attrs.is_control()) {
            apply_control(bid.pid, parse_control_batch(b));
        } else {
            apply_data(bid, hdr);
        }
    }

    if (_is_autoabort_enabled && !_is_autoabort_active) {
        abort_old_txes();
    }
    co_return;
}

void rm_stm::apply_control(
  model::producer_identity pid, model::control_record_type crt) {
    vlog(
      _ctx_log.trace, "applying control batch of type {}, pid: {}", crt, pid);
    // either epoch is the same as fencing or it's lesser in the latter
    // case we don't fence off aborts and commits because transactional
    // manager already decided a tx's outcome and acked it to the client
    auto [producer, _] = maybe_create_producer(pid);

    if (likely(
          crt == model::control_record_type::tx_abort
          || crt == model::control_record_type::tx_commit)) {
        /**
         * Transaction is finished, update producer tx start offset
         */
        producer->update_current_txn_start_offset(std::nullopt);
    }

    // there are only two types of control batches
    if (crt == model::control_record_type::tx_abort) {
        _highest_producer_id = std::max(_highest_producer_id, pid.get_id());
        _log_state.current_txes.erase(pid);
        auto offset_it = _log_state.ongoing_map.find(pid);
        if (offset_it != _log_state.ongoing_map.end()) {
            // make a list
            _log_state.aborted.push_back(offset_it->second);
            _log_state.ongoing_set.erase(offset_it->second.first);
            _log_state.ongoing_map.erase(pid);
        }

        _mem_state.forget(pid);
        _log_state.expiration.erase(pid);

        if (
          _log_state.aborted.size() > _abort_index_segment_size
          && !_is_abort_idx_reduction_requested) {
            ssx::spawn_with_gate(
              _gate, [this] { return reduce_aborted_list(); });
        }
    } else if (crt == model::control_record_type::tx_commit) {
        _highest_producer_id = std::max(_highest_producer_id, pid.get_id());
        _log_state.current_txes.erase(pid);
        auto offset_it = _log_state.ongoing_map.find(pid);
        if (offset_it != _log_state.ongoing_map.end()) {
            _log_state.ongoing_set.erase(offset_it->second.first);
            _log_state.ongoing_map.erase(pid);
        }

        _mem_state.forget(pid);
        _log_state.expiration.erase(pid);
    }
}

ss::future<> rm_stm::reduce_aborted_list() {
    if (_is_abort_idx_reduction_requested) {
        return ss::now();
    }
    if (_log_state.aborted.size() <= _abort_index_segment_size) {
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
        const auto last_offset = header.last_offset();
        const auto last_kafka_offset = from_log_offset(header.last_offset());
        auto [producer, _] = maybe_create_producer(bid.pid);
        auto needs_touch = producer->update(bid, last_kafka_offset);
        if (needs_touch) {
            _producer_state_manager.local().touch(*producer, _vcluster_id);
        }

        if (bid.is_transactional) {
            vlog(
              _ctx_log.trace,
              "Applying tx data batch with identity: {} and offset range: "
              "[{},{}], last kafka offset: {}",
              bid,
              header.base_offset,
              last_offset,
              last_kafka_offset);
            auto ongoing_it = _log_state.ongoing_map.find(bid.pid);
            if (ongoing_it != _log_state.ongoing_map.end()) {
                if (ongoing_it->second.last < last_offset) {
                    ongoing_it->second.last = last_offset;
                }
            } else {
                // we do no have to check if the value is empty as it is already
                // done with ongoing map
                producer->update_current_txn_start_offset(
                  from_log_offset(header.base_offset));

                _log_state.ongoing_map.emplace(
                  bid.pid,
                  tx_range{
                    .pid = bid.pid,
                    .first = header.base_offset,
                    .last = last_offset});
                _log_state.ongoing_set.insert(header.base_offset);
                _mem_state.estimated.erase(bid.pid);
            }
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
template<class T>
static void move_snapshot_wo_seqs(tx_snapshot_v4& target, T& source) {
    target.fenced = std::move(source.fenced);
    target.ongoing = std::move(source.ongoing);
    target.prepared = std::move(source.prepared);
    target.aborted = std::move(source.aborted);
    target.abort_indexes = std::move(source.abort_indexes);
    target.offset = std::move(source.offset);
}

ss::future<>
rm_stm::apply_local_snapshot(raft::stm_snapshot_header hdr, iobuf&& tx_ss_buf) {
    vlog(
      _ctx_log.trace,
      "applying snapshot with last included offset: {}",
      hdr.offset);
    tx_snapshot data;
    iobuf_parser data_parser(std::move(tx_ss_buf));
    if (hdr.version == tx_snapshot_v4::version) {
        tx_snapshot_v4 data_v4
          = co_await reflection::async_adl<tx_snapshot_v4>{}.from(data_parser);
        data = tx_snapshot(std::move(data_v4), _raft->group());
    } else if (hdr.version == tx_snapshot_v3::version) {
        tx_snapshot_v4 data_v4;
        auto data_v3 = reflection::adl<tx_snapshot_v3>{}.from(data_parser);
        // convert to v4
        move_snapshot_wo_seqs(data_v4, data_v3);
        data_v4.seqs = std::move(data_v3.seqs);
        data_v4.expiration = std::move(data_v3.expiration);
        for (auto& entry : data_v3.tx_seqs) {
            data.tx_data.push_back(tx_data_snapshot{
              .pid = entry.pid,
              .tx_seq = entry.tx_seq,
              .tm = model::legacy_tm_ntp.tp.partition});
        }
        // convert to v5
        data = tx_snapshot(std::move(data_v4), _raft->group());
    } else if (hdr.version == tx_snapshot::version) {
        data = co_await reflection::async_adl<tx_snapshot>{}.from(data_parser);
    } else {
        vassert(
          false, "unsupported tx_snapshot_header version {}", hdr.version);
    }

    for (auto& entry : data.fenced) {
        _log_state.fence_pid_epoch.emplace(entry.get_id(), entry.get_epoch());
    }
    for (auto& entry : data.ongoing) {
        _log_state.ongoing_map.emplace(entry.pid, entry);
        _log_state.ongoing_set.insert(entry.first);
    }
    for (auto it = std::make_move_iterator(data.aborted.begin());
         it != std::make_move_iterator(data.aborted.end());
         it++) {
        _log_state.aborted.push_back(*it);
    }
    _highest_producer_id = std::max(
      data.highest_producer_id, _highest_producer_id);
    co_await ss::max_concurrent_for_each(
      data.abort_indexes, 32, [this](const abort_index& idx) -> ss::future<> {
          auto f_name = abort_idx_name(idx.first, idx.last);
          return _abort_snapshot_mgr.get_snapshot_size(f_name).then(
            [this, idx](uint64_t snapshot_size) {
                _abort_snapshot_sizes.emplace(
                  std::make_pair(idx.first, idx.last), snapshot_size);
            });
      });

    for (auto it = std::make_move_iterator(data.abort_indexes.begin());
         it != std::make_move_iterator(data.abort_indexes.end());
         it++) {
        _log_state.abort_indexes.push_back(*it);
    }
    co_await reset_producers();
    for (auto& entry : data.producers) {
        if (_log_state.fence_pid_epoch.contains(entry._id.get_id())) {
            continue;
        }
        auto pid = entry._id;
        auto producer = ss::make_lw_shared<producer_state>(
          [pid, this] { cleanup_producer_state(pid); }, std::move(entry));
        try {
            _producer_state_manager.local().register_producer(
              *producer, _vcluster_id);
            _producers.emplace(pid, producer);
        } catch (const cache_full_error& e) {
            vlog(
              _ctx_log.warn,
              "unable to register producer {} with vcluster: {} - {}",
              pid,
              _vcluster_id,
              e.what());
        }
    }

    abort_index last{.last = model::offset(-1)};
    for (auto& entry : _log_state.abort_indexes) {
        if (entry.last > last.last) {
            last = entry;
        }
    }
    if (last.last > model::offset(0)) {
        auto snapshot_opt = co_await load_abort_snapshot(last);
        if (snapshot_opt) {
            _log_state.last_abort_snapshot = std::move(snapshot_opt.value());
        }
    }

    for (auto& entry : data.tx_data) {
        _log_state.current_txes.emplace(
          entry.pid, tx_data{entry.tx_seq, entry.tm});
    }

    for (auto& entry : data.expiration) {
        _log_state.expiration.emplace(
          entry.pid,
          expiration_info{
            .timeout = entry.timeout,
            .last_update = clock_type::now(),
            .is_expiration_requested = false});
    }
}

uint8_t rm_stm::active_snapshot_version() {
    if (_feature_table.local().is_active(features::feature::idempotency_v2)) {
        return tx_snapshot::version;
    }
    return tx_snapshot_v4::version;
}

template<class T>
void rm_stm::fill_snapshot_wo_seqs(T& snapshot) {
    for (auto const& [k, v] : _log_state.fence_pid_epoch) {
        snapshot.fenced.push_back(model::producer_identity{k(), v()});
    }
    for (auto& entry : _log_state.ongoing_map) {
        snapshot.ongoing.push_back(entry.second);
    }
    for (auto& entry : _log_state.aborted) {
        snapshot.aborted.push_back(entry);
    }
    for (auto& entry : _log_state.abort_indexes) {
        snapshot.abort_indexes.push_back(entry);
    }
}

ss::future<> rm_stm::offload_aborted_txns() {
    // This method iterates through _log_state.aborted collection
    // and the loop's body contains sync points (co_await) so w/o
    // further assumptions it's impossible to guarantee that the
    // collection isn't updated by another coroutine while the
    // current coroutine waits.

    // We should prevent it because an update invalidates the ite-
    // rator and may lead to undefined behavior. In order to avoid
    // this situation, offload_aborted_txns should be invoked only
    // under _state_lock's write lock because all the other updators
    // use the read lock.
    std::sort(
      std::begin(_log_state.aborted),
      std::end(_log_state.aborted),
      [](tx_range a, tx_range b) { return a.first < b.first; });

    abort_snapshot snapshot{
      .first = model::offset::max(), .last = model::offset::min()};
    for (auto const& entry : _log_state.aborted) {
        snapshot.first = std::min(snapshot.first, entry.first);
        snapshot.last = std::max(snapshot.last, entry.last);
        snapshot.aborted.push_back(entry);
        if (snapshot.aborted.size() == _abort_index_segment_size) {
            auto idx = abort_index{
              .first = snapshot.first, .last = snapshot.last};
            _log_state.abort_indexes.push_back(idx);
            co_await save_abort_snapshot(std::move(snapshot));
            snapshot = abort_snapshot{
              .first = model::offset::max(), .last = model::offset::min()};
        }
    }
    _log_state.aborted = std::move(snapshot.aborted);
}

ss::future<raft::stm_snapshot> rm_stm::take_local_snapshot() {
    return do_take_local_snapshot(active_snapshot_version());
}

// DO NOT coroutinize this method as it may cause issues on ARM:
// https://github.com/redpanda-data/redpanda/issues/6768
ss::future<raft::stm_snapshot> rm_stm::do_take_local_snapshot(uint8_t version) {
    auto start_offset = _raft->start_offset();
    vlog(
      _ctx_log.trace,
      "taking snapshot with last included offset of: {}",
      last_applied_offset());

    fragmented_vector<abort_index> abort_indexes;
    fragmented_vector<abort_index> expired_abort_indexes;

    for (const auto& idx : _log_state.abort_indexes) {
        if (idx.last < start_offset) {
            // caching expired indexes instead of removing them as we go
            // to avoid giving control to another coroutine and managing
            // concurrent access to _log_state.abort_indexes
            expired_abort_indexes.push_back(idx);
        } else {
            abort_indexes.push_back(idx);
        }
    }
    _log_state.abort_indexes = std::move(abort_indexes);

    vlog(
      _ctx_log.debug,
      "Removing abort indexes {} with offset < {}",
      expired_abort_indexes.size(),
      start_offset);
    auto f = ss::do_with(
      std::move(expired_abort_indexes),
      [this](fragmented_vector<abort_index>& idxs) {
          return ss::parallel_for_each(
            idxs.begin(), idxs.end(), [this](const abort_index& idx) {
                auto f_name = abort_idx_name(idx.first, idx.last);
                vlog(
                  _ctx_log.debug,
                  "removing aborted transactions {} snapshot file",
                  f_name);
                _abort_snapshot_sizes.erase(
                  std::make_pair(idx.first, idx.last));
                return _abort_snapshot_mgr.remove_snapshot(f_name);
            });
      });

    fragmented_vector<tx_range> aborted;
    std::copy_if(
      _log_state.aborted.begin(),
      _log_state.aborted.end(),
      std::back_inserter(aborted),
      [start_offset](tx_range range) { return range.last >= start_offset; });
    _log_state.aborted = std::move(aborted);

    if (_log_state.aborted.size() > _abort_index_segment_size) {
        f = f.then([this] {
            return _state_lock.hold_write_lock().then(
              [this](ss::basic_rwlock<>::holder unit) {
                  // software engineer be careful and do not cause a deadlock.
                  // take_snapshot is invoked under the persisted_stm::_op_lock
                  // and here here we take write lock (_state_lock). most rm_stm
                  // operations require its read lock. however they don't depend
                  // of _op_lock so things are safe now
                  return offload_aborted_txns().finally(
                    [u = std::move(unit)] {});
              });
        });
    }
    kafka::offset start_kafka_offset = from_log_offset(start_offset);
    return f.then([this, start_kafka_offset, version]() mutable {
        return ss::do_with(
          iobuf{},
          [this, start_kafka_offset, version](iobuf& tx_ss_buf) mutable {
              auto fut_serialize = ss::now();
              if (version == tx_snapshot_v4::version) {
                  tx_snapshot_v4 tx_ss;
                  fill_snapshot_wo_seqs(tx_ss);
                  for (const auto& [_, state] : _producers) {
                      /**
                       * Only store those producer id sequences which offset is
                       * greater than log start offset. This way a snapshot will
                       * not retain producers ids for which all the batches were
                       * removed with log cleanup policy.
                       *
                       * Note that we are not removing producer ids from the in
                       * memory state but rather relay on the expiration policy
                       * to do it, however when recovering state from the
                       * snapshot removed producers will be gone.
                       */
                      auto snapshot = state->snapshot(start_kafka_offset);
                      auto seq_entry
                        = deprecated_seq_entry::from_producer_state_snapshot(
                          snapshot);
                      if (seq_entry.seq != -1) {
                          tx_ss.seqs.push_back(std::move(seq_entry));
                      }
                  }
                  tx_ss.offset = last_applied_offset();

                  for (const auto& entry : _log_state.current_txes) {
                      tx_ss.tx_data.push_back(tx_data_snapshot{
                        .pid = entry.first,
                        .tx_seq = entry.second.tx_seq,
                        .tm = entry.second.tm_partition});
                  }

                  for (const auto& entry : _log_state.expiration) {
                      tx_ss.expiration.push_back(expiration_snapshot{
                        .pid = entry.first, .timeout = entry.second.timeout});
                  }

                  fut_serialize = reflection::async_adl<tx_snapshot_v4>{}.to(
                    tx_ss_buf, std::move(tx_ss));
              } else if (version == tx_snapshot::version) {
                  tx_snapshot tx_ss;
                  fill_snapshot_wo_seqs(tx_ss);
                  for (const auto& [_, state] : _producers) {
                      auto snapshot = state->snapshot(start_kafka_offset);
                      if (!snapshot._finished_requests.empty()) {
                          tx_ss.producers.push_back(std::move(snapshot));
                      }
                  }
                  tx_ss.offset = last_applied_offset();

                  for (const auto& entry : _log_state.current_txes) {
                      tx_ss.tx_data.push_back(tx_data_snapshot{
                        .pid = entry.first,
                        .tx_seq = entry.second.tx_seq,
                        .tm = entry.second.tm_partition});
                  }

                  for (const auto& entry : _log_state.expiration) {
                      tx_ss.expiration.push_back(expiration_snapshot{
                        .pid = entry.first, .timeout = entry.second.timeout});
                  }
                  tx_ss.highest_producer_id = _highest_producer_id;

                  fut_serialize = reflection::async_adl<tx_snapshot>{}.to(
                    tx_ss_buf, std::move(tx_ss));

              } else {
                  vassert(false, "unsupported tx_snapshot version {}", version);
              }
              return fut_serialize.then([version, &tx_ss_buf, this]() {
                  return raft::stm_snapshot::create(
                    version, last_applied_offset(), std::move(tx_ss_buf));
              });
          });
    });
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

ss::future<std::optional<rm_stm::abort_snapshot>>
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
    for (const auto& idx : _log_state.abort_indexes) {
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
    _log_state.reset();
    _mem_state = mem_state{_tx_root_tracker};
    co_await reset_producers();
    set_next(_raft->start_offset());
    co_return;
}

std::ostream& operator<<(std::ostream& o, const rm_stm::abort_snapshot& as) {
    fmt::print(
      o,
      "{{first: {}, last: {}, aborted tx count: {}}}",
      as.first,
      as.last,
      as.aborted.size());
    return o;
}

std::ostream& operator<<(std::ostream& o, const rm_stm::mem_state& state) {
    fmt::print(o, "{{ estimated: {} }} ", state.estimated.size());
    return o;
}

std::ostream& operator<<(std::ostream& o, const rm_stm::log_state& state) {
    fmt::print(
      o,
      "{{ fence_epochs: {}, ongoing_m: {}, ongoing_set: {}, "
      "aborted: {}, abort_indexes: {}, tx_seqs: {}, expiration: "
      "{}}}",
      state.fence_pid_epoch.size(),
      state.ongoing_map.size(),
      state.ongoing_set.size(),
      state.aborted.size(),
      state.abort_indexes.size(),
      state.current_txes.size(),
      state.expiration.size());
    return o;
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
          [this] { return _log_state.ongoing_map.size(); },
          sm::description("Number of ongoing transactional requests."),
          labels),
        sm::make_gauge(
          "tx_mem_tracker_consumption_bytes",
          [this] { return _tx_root_tracker.consumption(); },
          sm::description("Total memory bytes in use by tx subsystem."),
          labels),
      },
      {},
      {sm::shard_label, partition_label});
}

ss::future<> rm_stm::maybe_log_tx_stats() {
    if (likely(!_ctx_log.logger().is_enabled(ss::log_level::debug))) {
        co_return;
    }
    _ctx_log.debug(
      "tx root mem_tracker aggregate consumption: {}",
      human::bytes(static_cast<double>(_tx_root_tracker.consumption())));
    _ctx_log.debug(
      "tx mem tracker breakdown: {}", _tx_root_tracker.pretty_print_json());
    auto units = co_await _state_lock.hold_read_lock();
    _ctx_log.debug(
      "tx memory snapshot stats: {{mem_state: {}, log_state: "
      "{}}}",
      _mem_state,
      _log_state);
}

void rm_stm::log_tx_stats() {
    ssx::background = ssx::spawn_with_gate_then(_gate, [this] {
                          return maybe_log_tx_stats().then([this] {
                              if (!_gate.is_closed()) {
                                  _log_stats_timer.rearm(
                                    clock_type::now()
                                    + _log_stats_interval_s());
                              }
                          });
                      }).handle_exception([](const std::exception_ptr&) {});
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

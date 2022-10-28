// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cluster/rm_stm.h"

#include "cluster/logger.h"
#include "cluster/tx_gateway_frontend.h"
#include "kafka/protocol/request_reader.h"
#include "kafka/protocol/response_writer.h"
#include "model/record.h"
#include "raft/consensus_utils.h"
#include "raft/errc.h"
#include "raft/types.h"
#include "storage/parser_utils.h"
#include "storage/record_batch_builder.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/future.hh>

#include <filesystem>
#include <optional>

namespace cluster {
using namespace std::chrono_literals;

static ss::sstring abort_idx_name(model::offset first, model::offset last) {
    return fmt::format("abort.idx.{}.{}", first, last);
}

static const model::violation_recovery_policy crash{
  model::violation_recovery_policy::crash};
static const model::violation_recovery_policy best_effort{
  model::violation_recovery_policy::best_effort};

static bool is_sequence(int32_t last_seq, int32_t next_seq) {
    return (last_seq + 1 == next_seq)
           || (next_seq == 0 && last_seq == std::numeric_limits<int32_t>::max());
}

static model::record_batch make_fence_batch(model::producer_identity pid) {
    iobuf key;
    auto pid_id = pid.id;
    reflection::serialize(key, model::record_batch_type::tx_fence, pid_id);

    iobuf value;
    reflection::serialize(value, rm_stm::fence_control_record_v0_version);

    storage::record_batch_builder builder(
      model::record_batch_type::tx_fence, model::offset(0));
    builder.set_producer_identity(pid.id, pid.epoch);
    builder.set_control_type();
    builder.add_raw_kv(std::move(key), std::move(value));

    return std::move(builder).build();
}

static model::record_batch make_fence_batch(
  model::producer_identity pid,
  model::tx_seq tx_seq,
  std::chrono::milliseconds transaction_timeout_ms) {
    iobuf key;
    auto pid_id = pid.id;
    reflection::serialize(key, model::record_batch_type::tx_fence, pid_id);

    iobuf value;
    reflection::serialize(
      value,
      rm_stm::fence_control_record_version,
      tx_seq,
      transaction_timeout_ms);

    storage::record_batch_builder builder(
      model::record_batch_type::tx_fence, model::offset(0));
    builder.set_producer_identity(pid.id, pid.epoch);
    builder.set_control_type();
    builder.add_raw_kv(std::move(key), std::move(value));

    return std::move(builder).build();
}

static model::record_batch make_prepare_batch(rm_stm::prepare_marker record) {
    storage::record_batch_builder builder(
      model::record_batch_type::tx_prepare, model::offset(0));
    builder.set_producer_identity(record.pid.id, record.pid.epoch);
    builder.set_control_type();

    iobuf key;
    reflection::serialize(key, model::record_batch_type::tx_prepare);
    reflection::serialize(key, record.pid.id);

    iobuf value;
    reflection::serialize(value, rm_stm::prepare_control_record_version);
    reflection::serialize(value, record.tm_partition());
    reflection::serialize(value, record.tx_seq());

    builder.add_raw_kv(std::move(key), std::move(value));
    return std::move(builder).build();
}

static model::record_batch make_tx_control_batch(
  model::producer_identity pid, model::control_record_type crt) {
    iobuf key;
    kafka::response_writer kw(key);
    kw.write(model::current_control_record_version());
    kw.write(static_cast<int16_t>(crt));

    iobuf value;
    kafka::response_writer vw(value);
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

static rm_stm::prepare_marker parse_prepare_batch(model::record_batch&& b) {
    const auto& hdr = b.header();
    auto bid = model::batch_identity::from(hdr);

    vassert(
      b.record_count() == 1,
      "model::record_batch_type::tx_prepare batch must contain a single "
      "record");
    auto r = b.copy_records();
    auto& record = *r.begin();
    auto val_buf = record.release_value();

    iobuf_parser val_reader(std::move(val_buf));
    auto version = reflection::adl<int8_t>{}.from(val_reader);
    vassert(
      version == rm_stm::prepare_control_record_version,
      "unknown prepare record version: {} expected: {}",
      version,
      rm_stm::prepare_control_record_version);
    auto tm_partition = model::partition_id(
      reflection::adl<int32_t>{}.from(val_reader));
    auto tx_seq = model::tx_seq(reflection::adl<int64_t>{}.from(val_reader));

    auto key_buf = record.release_key();
    iobuf_parser key_reader(std::move(key_buf));
    auto batch_type = reflection::adl<model::record_batch_type>{}.from(
      key_reader);
    vassert(
      hdr.type == batch_type,
      "broken model::record_batch_type::tx_prepare batch. expected batch type "
      "{} got: {}",
      hdr.type,
      batch_type);
    auto p_id = model::producer_id(reflection::adl<int64_t>{}.from(key_reader));
    vassert(
      p_id == bid.pid.id,
      "broken model::record_batch_type::tx_prepare batch. expected pid {} got: "
      "{}",
      bid.pid.id,
      p_id);

    return rm_stm::prepare_marker{
      .tm_partition = tm_partition,
      .tx_seq = tx_seq,
      .pid = bid.pid,
    };
}

static model::control_record_type
parse_control_batch(const model::record_batch& b) {
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
    kafka::request_reader key_reader(std::move(key));
    auto version = model::control_record_version(key_reader.read_int16());
    vassert(
      version == model::current_control_record_version,
      "unknown control record version");
    return model::control_record_type(key_reader.read_int16());
}

model::control_record_type
rm_stm::parse_tx_control_batch(const model::record_batch& b) {
    return parse_control_batch(b);
}

struct seq_entry_v0 {
    model::producer_identity pid;
    int32_t seq;
    model::timestamp::type last_write_timestamp;
};

struct tx_snapshot_v0 {
    static constexpr uint8_t version = 0;

    std::vector<model::producer_identity> fenced;
    std::vector<rm_stm::tx_range> ongoing;
    std::vector<rm_stm::prepare_marker> prepared;
    std::vector<rm_stm::tx_range> aborted;
    std::vector<rm_stm::abort_index> abort_indexes;
    model::offset offset;
    std::vector<seq_entry_v0> seqs;
};

struct seq_cache_entry_v1 {
    int32_t seq{-1};
    model::offset offset;
};

struct seq_entry_v1 {
    model::producer_identity pid;
    int32_t seq{-1};
    model::offset last_offset{-1};
    ss::circular_buffer<seq_cache_entry_v1> seq_cache;
    model::timestamp::type last_write_timestamp;
};

struct tx_snapshot_v1 {
    static constexpr uint8_t version = 1;

    std::vector<model::producer_identity> fenced;
    std::vector<rm_stm::tx_range> ongoing;
    std::vector<rm_stm::prepare_marker> prepared;
    std::vector<rm_stm::tx_range> aborted;
    std::vector<rm_stm::abort_index> abort_indexes;
    model::offset offset;
    std::vector<seq_entry_v1> seqs;
};

rm_stm::rm_stm(
  ss::logger& logger,
  raft::consensus* c,
  ss::sharded<cluster::tx_gateway_frontend>& tx_gateway_frontend,
  ss::sharded<features::feature_table>& feature_table)
  : persisted_stm("tx.snapshot", logger, c)
  , _oldest_session(model::timestamp::now())
  , _sync_timeout(config::shard_local_cfg().rm_sync_timeout_ms.value())
  , _tx_timeout_delay(config::shard_local_cfg().tx_timeout_delay_ms.value())
  , _abort_interval_ms(config::shard_local_cfg()
                         .abort_timed_out_transactions_interval_ms.value())
  , _abort_index_segment_size(
      config::shard_local_cfg().abort_index_segment_size.value())
  , _seq_table_min_size(config::shard_local_cfg().seq_table_min_size.value())
  , _recovery_policy(
      config::shard_local_cfg().rm_violation_recovery_policy.value())
  , _transactional_id_expiration(
      config::shard_local_cfg().transactional_id_expiration_ms.value())
  , _is_tx_enabled(config::shard_local_cfg().enable_transactions.value())
  , _tx_gateway_frontend(tx_gateway_frontend)
  , _abort_snapshot_mgr(
      "abort.idx",
      std::filesystem::path(c->log_config().work_directory()),
      ss::default_priority_class())
  , _feature_table(feature_table)
  , _ctx_log(txlog, ssx::sformat("[{}]", c->ntp())) {
    if (!_is_tx_enabled) {
        _is_autoabort_enabled = false;
    }
    if (_recovery_policy != crash && _recovery_policy != best_effort) {
        vassert(false, "Unknown recovery policy: {}", _recovery_policy);
    }
    auto_abort_timer.set_callback([this] { abort_old_txes(); });
}

ss::future<checked<model::term_id, tx_errc>> rm_stm::begin_tx(
  model::producer_identity pid,
  model::tx_seq tx_seq,
  std::chrono::milliseconds transaction_timeout_ms) {
    return _state_lock.hold_read_lock().then(
      [this, pid, tx_seq, transaction_timeout_ms](
        ss::basic_rwlock<>::holder unit) mutable {
          return get_tx_lock(pid.get_id())
            ->with([this, pid, tx_seq, transaction_timeout_ms]() {
                return do_begin_tx(pid, tx_seq, transaction_timeout_ms);
            })
            .finally([u = std::move(unit)] {});
      });
}

ss::future<checked<model::term_id, tx_errc>> rm_stm::do_begin_tx(
  model::producer_identity pid,
  model::tx_seq tx_seq,
  std::chrono::milliseconds transaction_timeout_ms) {
    if (!check_tx_permitted()) {
        co_return tx_errc::request_rejected;
    }

    if (!_c->is_leader()) {
        co_return tx_errc::leader_not_found;
    }

    if (!co_await sync(_sync_timeout)) {
        co_return tx_errc::stale;
    }
    auto synced_term = _insync_term;

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
        if (ar == tx_errc::stale) {
            co_return tx_errc::stale;
        }
        if (ar != tx_errc::none) {
            co_return tx_errc::unknown_server_error;
        }

        if (is_known_session(old_pid)) {
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

    auto is_txn_ga = is_transaction_ga();

    auto batch = is_txn_ga
                   ? make_fence_batch(pid, tx_seq, transaction_timeout_ms)
                   : make_fence_batch(pid);

    auto reader = model::make_memory_record_batch_reader(std::move(batch));
    auto r = co_await _c->replicate(
      synced_term,
      std::move(reader),
      raft::replicate_options(raft::consistency_level::quorum_ack));

    if (!r) {
        vlog(
          _ctx_log.warn,
          "Error \"{}\" on replicating pid:{} fencing batch",
          r.error(),
          pid);
        co_return tx_errc::unknown_server_error;
    }

    if (!co_await wait_no_throw(
          model::offset(r.value().last_offset()), _sync_timeout)) {
        co_return tx_errc::unknown_server_error;
    }

    if (_c->term() != synced_term) {
        vlog(
          _ctx_log.warn,
          "term changeg from {} to {} during fencing pid {}",
          synced_term,
          _c->term(),
          pid);
        co_return tx_errc::unknown_server_error;
    }

    if (is_txn_ga) {
        auto tx_seq_it = _log_state.tx_seqs.find(pid);
        if (tx_seq_it == _log_state.tx_seqs.end()) {
            vlog(
              _ctx_log.error,
              "tx_seqs should be updated after fencing pid {}",
              pid);
            co_return tx_errc::unknown_server_error;
        }
        if (tx_seq_it->second != tx_seq) {
            vlog(
              _ctx_log.error,
              "expected tx_seq={} for pid {} got {}",
              tx_seq,
              pid,
              tx_seq_it->second);
            co_return tx_errc::unknown_server_error;
        }
    } else {
        auto [_, inserted] = _mem_state.expected.emplace(pid, tx_seq);
        if (!inserted) {
            vlog(
              _ctx_log.error,
              "there is already an ongoing transaction with tx_seq:{} within "
              "{} session",
              tx_seq,
              pid);
            co_return tx_errc::unknown_server_error;
        }

        track_tx(pid, transaction_timeout_ms);
    }

    // a client may start new transaction only when the previous
    // tx is committed. since a client commits a transacitons
    // strictly after all records are written it means that it
    // won't be retrying old writes and we may reset the seq cache
    _log_state.seq_table.erase(pid);
    _mem_state.inflight[pid] = 0;

    co_return synced_term;
}

ss::future<tx_errc> rm_stm::prepare_tx(
  model::term_id etag,
  model::partition_id tm,
  model::producer_identity pid,
  model::tx_seq tx_seq,
  model::timeout_clock::duration timeout) {
    return _state_lock.hold_read_lock().then(
      [this, etag, tm, pid, tx_seq, timeout](
        ss::basic_rwlock<>::holder unit) mutable {
          return get_tx_lock(pid.get_id())
            ->with([this, etag, tm, pid, tx_seq, timeout]() {
                return do_prepare_tx(etag, tm, pid, tx_seq, timeout);
            })
            .finally([u = std::move(unit)] {});
      });
}

ss::future<tx_errc> rm_stm::do_prepare_tx(
  model::term_id etag,
  model::partition_id tm,
  model::producer_identity pid,
  model::tx_seq tx_seq,
  model::timeout_clock::duration timeout) {
    if (!check_tx_permitted()) {
        co_return tx_errc::request_rejected;
    }

    if (!co_await sync(timeout)) {
        co_return tx_errc::stale;
    }
    auto synced_term = _insync_term;

    auto prepared_it = _log_state.prepared.find(pid);
    if (prepared_it != _log_state.prepared.end()) {
        if (prepared_it->second.tx_seq != tx_seq) {
            // current prepare_tx call is stale, rejecting
            co_return tx_errc::request_rejected;
        }
        // a tx was already prepared
        co_return tx_errc::none;
    }

    // checking fencing
    auto fence_it = _log_state.fence_pid_epoch.find(pid.get_id());
    if (fence_it == _log_state.fence_pid_epoch.end()) {
        // begin_tx should have set a fence
        co_return tx_errc::request_rejected;
    }
    if (pid.get_epoch() != fence_it->second) {
        vlog(
          _ctx_log.error,
          "Can't prepare pid:{} - fenced out by epoch {}",
          pid,
          fence_it->second);
        co_return tx_errc::fenced;
    }

    if (_log_state.tx_seqs.contains(pid)) {
        if (_log_state.tx_seqs[pid] != tx_seq) {
            vlog(
              _ctx_log.warn,
              "expectd tx_seq {} doesn't match gived {} for pid {}",
              _log_state.tx_seqs[pid],
              tx_seq,
              pid);
        }
        co_return errc::invalid_producer_epoch;
    }

    if (synced_term != etag) {
        vlog(
          _ctx_log.warn,
          "Can't prepare pid:{} - partition lost leadership current term: {} "
          "expected term: {}",
          pid,
          synced_term,
          etag);
        // current partition changed leadership since a transaction started
        // there is a chance that not all writes were replicated
        // rejecting a tx to prevent data loss
        co_return tx_errc::request_rejected;
    }

    if (!is_transaction_ga()) {
        auto expected_it = _mem_state.expected.find(pid);
        if (expected_it == _mem_state.expected.end()) {
            // impossible situation, a transaction coordinator tries
            // to prepare a transaction which wasn't started
            vlog(_ctx_log.error, "Can't prepare pid:{} - unknown session", pid);
            co_return tx_errc::request_rejected;
        }
        if (expected_it->second != tx_seq) {
            // current prepare_tx call is stale, rejecting
            co_return tx_errc::request_rejected;
        }
    }

    auto marker = prepare_marker{
      .tm_partition = tm, .tx_seq = tx_seq, .pid = pid};
    auto [_, inserted] = _mem_state.preparing.try_emplace(pid, marker);
    if (!inserted) {
        vlog(
          _ctx_log.error,
          "Can't prepare pid:{} - concurrent operation on the same session",
          pid);
        co_return tx_errc::conflict;
    }

    auto batch = make_prepare_batch(marker);
    auto reader = model::make_memory_record_batch_reader(std::move(batch));
    auto r = co_await _c->replicate(
      etag,
      std::move(reader),
      raft::replicate_options(raft::consistency_level::quorum_ack));

    if (!r) {
        vlog(
          _ctx_log.error,
          "Error \"{}\" on replicating pid:{} prepare batch",
          r.error(),
          pid);
        co_return tx_errc::unknown_server_error;
    }

    if (!co_await wait_no_throw(
          model::offset(r.value().last_offset()), timeout)) {
        co_return tx_errc::unknown_server_error;
    }

    co_return tx_errc::none;
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
    if (!check_tx_permitted()) {
        co_return tx_errc::request_rejected;
    }

    // doesn't make sense to fence off a commit because transaction
    // manager has already decided to commit and acked to a client
    if (!co_await sync(timeout)) {
        co_return tx_errc::stale;
    }
    auto synced_term = _insync_term;
    // catching up with all previous end_tx operations (commit | abort)
    // to avoid writing the same commit | abort marker twice
    if (_mem_state.last_end_tx >= model::offset{0}) {
        if (!co_await wait_no_throw(_mem_state.last_end_tx, timeout)) {
            co_return tx_errc::stale;
        }
    }

    auto fence_it = _log_state.fence_pid_epoch.find(pid.get_id());
    if (fence_it == _log_state.fence_pid_epoch.end()) {
        // begin_tx should have set a fence
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

    std::optional<model::tx_seq> tx_seq_for_pid;
    if (is_transaction_ga()) {
        if (_log_state.tx_seqs.contains(pid)) {
            if (tx_seq != _log_state.tx_seqs[pid]) {
                co_return tx_errc::request_rejected;
            }
        }
    } else {
        auto preparing_it = _mem_state.preparing.find(pid);

        if (preparing_it != _mem_state.preparing.end()) {
            if (preparing_it->second.tx_seq > tx_seq) {
                // - tm_stm & rm_stm failed during prepare
                // - during recovery tm_stm recommits its previous tx
                // - that commit (we're here) collides with "next" failed
                // prepare it may happen only if the commit passed => acking
                co_return tx_errc::none;
            }

            ss::sstring msg;
            if (preparing_it->second.tx_seq == tx_seq) {
                msg = ssx::sformat(
                  "Prepare hasn't completed => can't commit pid:{} tx_seq:{}",
                  pid,
                  tx_seq);
            } else {
                msg = ssx::sformat(
                  "Commit pid:{} tx_seq:{} conflicts with preparing tx_seq:{}",
                  pid,
                  tx_seq,
                  preparing_it->second.tx_seq);
            }

            if (_recovery_policy == best_effort) {
                vlog(_ctx_log.error, "{}", msg);
                co_return tx_errc::request_rejected;
            }
            vassert(false, "{}", msg);
        }

        auto prepare_it = _log_state.prepared.find(pid);

        if (prepare_it != _log_state.prepared.end()) {
            tx_seq_for_pid = prepare_it->second.tx_seq;
        }
    }

    if (!tx_seq_for_pid) {
        tx_seq_for_pid = get_tx_seq(pid);
    }

    _mem_state.expected.erase(pid);
    _mem_state.preparing.erase(pid);

    if (!tx_seq_for_pid) {
        vlog(
          _ctx_log.trace,
          "Can't find tx_seq for pid:{} => replaying already comitted "
          "commit",
          pid);
        co_return tx_errc::none;
    }

    if (*tx_seq_for_pid > tx_seq) {
        // rare situation:
        //   * tm_stm prepares (tx_seq+1)
        //   * prepare on this rm passes but tm_stm failed to write to disk
        //   * during recovery tm_stm recommits (tx_seq)
        // existence of {pid, tx_seq+1} implies {pid, tx_seq} is committed
        vlog(
          _ctx_log.trace,
          "prepare for pid:{} has higher tx_seq:{} than given: {} => replaying "
          "already comitted commit",
          pid,
          *tx_seq_for_pid,
          tx_seq);
        co_return tx_errc::none;
    } else if (*tx_seq_for_pid < tx_seq) {
        std::string msg = fmt::format(
          "Rejecting commit with tx_seq:{} since prepare with lesser tx_seq:{} "
          "exists",
          tx_seq,
          *tx_seq_for_pid);
        if (_recovery_policy == best_effort) {
            vlog(_ctx_log.error, "{}", msg);
            co_return tx_errc::request_rejected;
        }
        vassert(false, "{}", msg);
    }

    // we commit only if a provided tx_seq matches prepared tx_seq
    auto batch = make_tx_control_batch(
      pid, model::control_record_type::tx_commit);
    auto reader = model::make_memory_record_batch_reader(std::move(batch));
    auto r = co_await _c->replicate(
      synced_term,
      std::move(reader),
      raft::replicate_options(raft::consistency_level::quorum_ack));

    if (!r) {
        vlog(
          _ctx_log.error,
          "Error \"{}\" on replicating pid:{} commit batch",
          r.error(),
          pid);
        co_return tx_errc::unknown_server_error;
    }
    if (_mem_state.last_end_tx < r.value().last_offset) {
        _mem_state.last_end_tx = r.value().last_offset;
    }
    if (!co_await wait_no_throw(r.value().last_offset, timeout)) {
        co_return tx_errc::unknown_server_error;
    }

    co_return tx_errc::none;
}

abort_origin rm_stm::get_abort_origin(
  const model::producer_identity& pid, model::tx_seq tx_seq) const {
    auto expected_it = _mem_state.expected.find(pid);
    if (expected_it != _mem_state.expected.end()) {
        if (tx_seq < expected_it->second) {
            return abort_origin::past;
        }
        if (expected_it->second < tx_seq) {
            return abort_origin::future;
        }
    }

    auto preparing_it = _mem_state.preparing.find(pid);
    if (preparing_it != _mem_state.preparing.end()) {
        if (tx_seq < preparing_it->second.tx_seq) {
            return abort_origin::past;
        }
        if (preparing_it->second.tx_seq < tx_seq) {
            return abort_origin::future;
        }
    }

    auto prepared_it = _log_state.prepared.find(pid);
    if (prepared_it != _log_state.prepared.end()) {
        if (tx_seq < prepared_it->second.tx_seq) {
            return abort_origin::past;
        }
        if (prepared_it->second.tx_seq < tx_seq) {
            return abort_origin::future;
        }
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
        co_return tx_errc::stale;
    }
    auto synced_term = _insync_term;
    // catching up with all previous end_tx operations (commit | abort)
    // to avoid writing the same commit | abort marker twice
    if (_mem_state.last_end_tx >= model::offset{0}) {
        if (!co_await wait_no_throw(_mem_state.last_end_tx, timeout)) {
            co_return tx_errc::stale;
        }
    }

    if (!is_known_session(pid)) {
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
            auto expiration_it = _mem_state.expiration.find(pid);
            if (expiration_it != _mem_state.expiration.end()) {
                expiration_it->second.is_expiration_requested = true;
            }
            // spawing abort in the background and returning an error to
            // release locks on the tx coordinator to prevent distributed
            // deadlock
            ssx::spawn_with_gate(
              _gate, [this, pid] { return try_abort_old_tx(pid); });
            co_return tx_errc::request_rejected;
        }
        if (origin == abort_origin::future) {
            // impossible situation: before transactional coordinator may issue
            // abort of the current transaction it should begin it and abort all
            // previous transactions with the same pid
            vlog(
              _ctx_log.error,
              "Rejecting abort (pid:{}, tx_seq: {}) because it isn't "
              "consistent "
              "with the current ongoing transaction",
              pid,
              tx_seq.value());
            co_return tx_errc::request_rejected;
        }
    }

    // preventing prepare and replicte once we
    // know we're going to abort tx and abandon pid
    _mem_state.expected.erase(pid);

    auto batch = make_tx_control_batch(
      pid, model::control_record_type::tx_abort);
    auto reader = model::make_memory_record_batch_reader(std::move(batch));
    auto r = co_await _c->replicate(
      synced_term,
      std::move(reader),
      raft::replicate_options(raft::consistency_level::quorum_ack));

    if (!r) {
        vlog(
          _ctx_log.error,
          "Error \"{}\" on replicating pid:{} abort batch",
          r.error(),
          pid);
        co_return tx_errc::unknown_server_error;
    }
    if (_mem_state.last_end_tx < r.value().last_offset) {
        _mem_state.last_end_tx = r.value().last_offset;
    }

    if (!co_await wait_no_throw(r.value().last_offset, timeout)) {
        co_return tx_errc::unknown_server_error;
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

ss::future<std::error_code>
rm_stm::transfer_leadership(std::optional<model::node_id> target) {
    return _state_lock.hold_write_lock().then(
      [this, target](ss::basic_rwlock<>::holder unit) {
          return _c->do_transfer_leadership(target).finally(
            [u = std::move(unit)] {});
      });
}

ss::future<result<kafka_result>> rm_stm::do_replicate(
  model::batch_identity bid,
  model::record_batch_reader b,
  raft::replicate_options opts,
  ss::lw_shared_ptr<available_promise<>> enqueued) {
    return _state_lock.hold_read_lock().then(
      [this, enqueued, bid, opts, b = std::move(b)](
        ss::basic_rwlock<>::holder unit) mutable {
          if (bid.is_transactional) {
              auto pid = bid.pid.get_id();
              return get_tx_lock(pid)
                ->with([this, bid, b = std::move(b)]() mutable {
                    return replicate_tx(bid, std::move(b));
                })
                .finally([u = std::move(unit)] {});
          } else if (bid.has_idempotent()) {
              return replicate_seq(bid, std::move(b), opts, enqueued)
                .finally([u = std::move(unit)] {});
          }

          return replicate_msg(std::move(b), opts, enqueued)
            .finally([u = std::move(unit)] {});
      });
}

ss::future<> rm_stm::stop() {
    auto_abort_timer.cancel();
    return raft::state_machine::stop();
}

ss::future<> rm_stm::start() {
    _translator = _c->get_offset_translator_state();
    return persisted_stm::start();
}

rm_stm::transaction_info::status_t
rm_stm::get_tx_status(model::producer_identity pid) const {
    if (_mem_state.preparing.contains(pid)) {
        return transaction_info::status_t::preparing;
    }

    if (_log_state.prepared.contains(pid)) {
        return transaction_info::status_t::prepared;
    }

    return transaction_info::status_t::ongoing;
}

std::optional<rm_stm::expiration_info>
rm_stm::get_expiration_info(model::producer_identity pid) const {
    auto it = _mem_state.expiration.find(pid);
    if (it == _mem_state.expiration.end()) {
        return std::nullopt;
    }

    return it->second;
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
        ans.emplace(id, tx_info);
    }

    for (auto& [id, offset] : _mem_state.tx_start) {
        transaction_info tx_info;
        tx_info.lso_bound = offset;
        tx_info.status = get_tx_status(id);
        tx_info.info = get_expiration_info(id);
        ans.emplace(id, tx_info);
    }

    for (auto& [id, offset] : _log_state.ongoing_map) {
        transaction_info tx_info;
        tx_info.lso_bound = offset.first;
        tx_info.status = get_tx_status(id);
        tx_info.info = get_expiration_info(id);
        ans.emplace(id, tx_info);
    }

    co_return ans;
}

ss::future<std::error_code> rm_stm::mark_expired(model::producer_identity pid) {
    return get_tx_lock(pid.get_id())->with([this, pid]() {
        return do_mark_expired(pid);
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
    _mem_state.expiration.erase(pid);
    co_await do_try_abort_old_tx(pid);
    co_return std::error_code(tx_errc::none);
}

bool rm_stm::check_seq(model::batch_identity bid) {
    auto& seq = _log_state.seq_table[bid.pid];
    auto last_write_timestamp = model::timestamp::now().value();

    if (!is_sequence(seq.seq, bid.first_seq)) {
        return false;
    }

    seq.update(bid.last_seq, kafka::offset{-1});

    seq.pid = bid.pid;
    seq.last_write_timestamp = last_write_timestamp;
    _oldest_session = std::min(
      _oldest_session, model::timestamp(seq.last_write_timestamp));

    return true;
}

std::optional<kafka::offset>
rm_stm::known_seq(model::batch_identity bid) const {
    auto pid_seq = _log_state.seq_table.find(bid.pid);
    if (pid_seq == _log_state.seq_table.end()) {
        return std::nullopt;
    }
    if (pid_seq->second.seq == bid.last_seq) {
        return pid_seq->second.last_offset;
    }
    for (auto& entry : pid_seq->second.seq_cache) {
        if (entry.seq == bid.last_seq) {
            return entry.offset;
        }
    }
    return std::nullopt;
}

std::optional<int32_t> rm_stm::tail_seq(model::producer_identity pid) const {
    auto pid_seq = _log_state.seq_table.find(pid);
    if (pid_seq == _log_state.seq_table.end()) {
        return std::nullopt;
    }
    return pid_seq->second.seq;
}

void rm_stm::set_seq(model::batch_identity bid, kafka::offset last_offset) {
    auto pid_seq = _log_state.seq_table.find(bid.pid);
    if (pid_seq != _log_state.seq_table.end()) {
        if (pid_seq->second.seq == bid.last_seq) {
            pid_seq->second.last_offset = last_offset;
        }
    }
}

void rm_stm::reset_seq(model::batch_identity bid) {
    _log_state.seq_table.erase(bid.pid);
    auto& seq = _log_state.seq_table[bid.pid];
    seq.seq = bid.last_seq;
    seq.last_offset = kafka::offset{-1};
    seq.pid = bid.pid;
    seq.last_write_timestamp = model::timestamp::now().value();
    _oldest_session = std::min(
      _oldest_session, model::timestamp(seq.last_write_timestamp));
}

ss::future<result<kafka_result>>
rm_stm::replicate_tx(model::batch_identity bid, model::record_batch_reader br) {
    if (!check_tx_permitted()) {
        co_return errc::generic_tx_error;
    }

    if (!co_await sync(_sync_timeout)) {
        co_return errc::not_leader;
    }
    auto synced_term = _insync_term;

    // fencing
    auto fence_it = _log_state.fence_pid_epoch.find(bid.pid.get_id());
    if (fence_it == _log_state.fence_pid_epoch.end()) {
        // begin_tx should have set a fence
        co_return errc::invalid_producer_epoch;
    }
    if (bid.pid.get_epoch() != fence_it->second) {
        co_return errc::invalid_producer_epoch;
    }

    if (!_mem_state.expected.contains(bid.pid)) {
        // there is an inflight abort
        // or this partition lost leadership => can't continue tx since there is
        //  risk that ack=1 writes are lost
        // or it's a client bug and it keeps producing after commit / abort
        // or a replication of the first batch in a transaction has failed
        vlog(
          _ctx_log.warn,
          "Partition doesn't expect record with pid:{}",
          bid.pid);
        co_return errc::invalid_producer_epoch;
    }

    if (_mem_state.preparing.contains(bid.pid)) {
        vlog(
          _ctx_log.warn,
          "Client keeps producing after initiating a prepare for pid:{}",
          bid.pid);
        co_return errc::generic_tx_error;
    }

    if (_mem_state.estimated.contains(bid.pid)) {
        // we received second produce request while the first is still
        // being processed. this is highly unlikely situation because
        // we replicate with ack=1 and it should be fast
        vlog(_ctx_log.warn, "Too frequent produce with same pid:{}", bid.pid);
        co_return errc::generic_tx_error;
    }

    if (_mem_state.inflight[bid.pid] > 0) {
        // this isn't the first attempt in the tx we should try dedupe
        auto cached_offset = known_seq(bid);
        if (cached_offset) {
            if (cached_offset.value() < kafka::offset{0}) {
                vlog(
                  _ctx_log.warn,
                  "Status of the original attempt is unknown (still is "
                  "in-flight "
                  "or failed). Failing a retried batch with the same pid:{} & "
                  "seq:{} to avoid duplicates",
                  bid.pid,
                  bid.last_seq);
                // kafka clients don't automaticly handle fatal errors
                // so when they encounter the error they will be forced
                // to propagate it to the app layer
                co_return errc::generic_tx_error;
            }
            co_return kafka_result{.last_offset = cached_offset.value()};
        }

        if (!check_seq(bid)) {
            co_return errc::sequence_out_of_order;
        }
    } else {
        // this is the first attempt in the tx, reset dedupe cache
        reset_seq(bid);
    }

    _mem_state.inflight[bid.pid] = _mem_state.inflight[bid.pid] + 1;

    if (!_mem_state.tx_start.contains(bid.pid)) {
        _mem_state.estimated.emplace(bid.pid, _insync_offset);
    }

    auto r = co_await _c->replicate(
      synced_term,
      std::move(br),
      raft::replicate_options(raft::consistency_level::leader_ack));
    if (!r) {
        if (_mem_state.estimated.contains(bid.pid)) {
            // an error during replication, preventin tx from progress
            _mem_state.expected.erase(bid.pid);
        }
        co_return r.error();
    }

    auto expiration_it = _mem_state.expiration.find(bid.pid);
    if (expiration_it == _mem_state.expiration.end()) {
        co_return errc::generic_tx_error;
    }
    expiration_it->second.last_update = clock_type::now();
    expiration_it->second.is_expiration_requested = false;

    auto old_offset = r.value().last_offset;
    auto new_offset = from_log_offset(old_offset);

    set_seq(bid, new_offset);

    if (!_mem_state.tx_start.contains(bid.pid)) {
        auto base_offset = model::offset(old_offset() - (bid.record_count - 1));
        _mem_state.tx_start.emplace(bid.pid, base_offset);
        _mem_state.tx_starts.insert(base_offset);
        _mem_state.estimated.erase(bid.pid);
    }

    co_return kafka_result{.last_offset = new_offset};
}

ss::future<result<kafka_result>> rm_stm::replicate_seq(
  model::batch_identity bid,
  model::record_batch_reader br,
  raft::replicate_options opts,
  ss::lw_shared_ptr<available_promise<>> enqueued) {
    using ret_t = result<kafka_result>;

    if (!co_await sync(_sync_timeout)) {
        // it's ok not to set enqueued on early return because
        // the safety check in replicate_in_stages sets it automatically
        co_return errc::not_leader;
    }
    auto synced_term = _insync_term;

    if (bid.first_seq > bid.last_seq) {
        vlog(
          _ctx_log.warn,
          "first_seq={} of the batch should be less or equal to last_seq={}",
          bid.first_seq,
          bid.last_seq);
        co_return errc::invalid_request;
    }

    // getting session by client's pid
    auto session = _inflight_requests
                     .lazy_emplace(
                       bid.pid,
                       [&](const auto& ctor) {
                           ctor(
                             bid.pid, ss::make_lw_shared<inflight_requests>());
                       })
                     ->second;

    // critical section, rm_stm evaluates the request its order and
    // whether or not it's duplicated
    // ASSUMING THAT ALL INFLIGHT REPLICATION REQUESTS WILL SUCCEED
    // if any of the request fail we expect that the leader step down
    auto u = co_await session->lock.get_units();

    if (!_c->is_leader() || _c->term() != synced_term) {
        co_return errc::not_leader;
    }

    if (session->term < synced_term) {
        // we don't care about old inflight requests because the sync
        // guarantee (all replicated records of the last term are
        // applied) makes sure that the passed inflight records got
        // into _log_state->seq_table
        session->forget();
        session->term = synced_term;
    }

    if (session->term > synced_term) {
        co_return errc::not_leader;
    }

    // checking if the request (identified by seq) is already resolved
    // checking among the pending requests
    auto cached_r = session->known_seq(bid.last_seq);
    if (cached_r) {
        co_return cached_r.value();
    }
    // checking among the responded requests
    auto cached_offset = known_seq(bid);
    if (cached_offset) {
        co_return kafka_result{.last_offset = cached_offset.value()};
    }

    // checking if the request is already being processed
    for (auto& inflight : session->cache) {
        if (inflight->last_seq == bid.last_seq && inflight->is_processing) {
            // found an inflight request, parking the current request
            // until the former is resolved
            auto promise
              = ss::make_lw_shared<available_promise<result<kafka_result>>>();
            inflight->parked.push_back(promise);
            u.return_all();
            co_return co_await promise->get_future();
        }
    }

    // apparantly we process an unseen request
    // checking if it isn't out of order with the already procceses
    // or inflight requests
    if (session->cache.size() == 0) {
        // no inflight requests > checking processed
        auto tail = tail_seq(bid.pid);
        if (tail) {
            if (!is_sequence(tail.value(), bid.first_seq)) {
                // there is a gap between the request'ss seq number
                // and the seq number of the latest processed request
                co_return errc::sequence_out_of_order;
            }
        } else {
            if (bid.first_seq != 0) {
                // first request in a session should have seq=0
                co_return errc::sequence_out_of_order;
            }
        }
    } else {
        if (!is_sequence(session->tail_seq, bid.first_seq)) {
            // there is a gap between the request'ss seq number
            // and the seq number of the latest inflight request
            co_return errc::sequence_out_of_order;
        }
    }

    // updating last observed seq, since we hold the mutex
    // it's guranteed to be latest
    session->tail_seq = bid.last_seq;

    auto request = ss::make_lw_shared<inflight_request>();
    request->last_seq = bid.last_seq;
    request->is_processing = true;
    session->cache.push_back(request);

    // request comes in the right order, it's ok to replicate
    ss::lw_shared_ptr<raft::replicate_stages> ss;
    auto has_failed = false;
    try {
        ss = _c->replicate_in_stages(synced_term, std::move(br), opts);
        co_await std::move(ss->request_enqueued);
    } catch (...) {
        vlog(
          _ctx_log.warn,
          "replication failed with {}",
          std::current_exception());
        has_failed = true;
    }

    result<raft::replicate_result> r = errc::success;

    if (has_failed) {
        if (_c->is_leader() && _c->term() == synced_term) {
            // we may not care about the requests waiting on the lock
            // as soon as we release the lock the leader or term will
            // be changed so the pending fibers won't pass the initial
            // checks and be rejected with not_leader
            co_await _c->step_down();
        }
        u.return_all();
        r = errc::replication_error;
    } else {
        try {
            u.return_all();
            enqueued->set_value();
            r = co_await std::move(ss->replicate_finished);
        } catch (...) {
            vlog(
              _ctx_log.warn,
              "replication failed with {}",
              std::current_exception());
            r = errc::replication_error;
        }
    }

    // we don't need session->lock because we never interleave
    // access to is_processing and offset with sync point (await)
    request->is_processing = false;
    if (r) {
        auto old_offset = r.value().last_offset;
        auto new_offset = from_log_offset(old_offset);
        request->r = ret_t(kafka_result{new_offset});
    } else {
        request->r = ret_t(r.error());
    }
    for (auto& pending : request->parked) {
        pending->set_value(request->r);
    }
    request->parked.clear();

    if (!request->r) {
        // if r was failed at the consensus level (not because has_failed)
        // it should guarantee that all follow up replication requests fail
        // too but just in case stepping down to minimize the risk
        if (_c->is_leader() && _c->term() == synced_term) {
            co_await _c->step_down();
        }
        co_return request->r;
    }

    // requests get into session->cache in seq order so when we iterate
    // starting from the beginning and until we meet the first still
    // in flight request we may be sure that we update seq_table in
    // order to preserve monotonicity and the lack of gaps
    while (!session->cache.empty() && !session->cache.front()->is_processing) {
        auto front = session->cache.front();
        if (!front->r) {
            // just encountered a failed request it but when a request
            // fails it causes a step down so we may not care about
            // updating seq_table and stop doing it; the next leader's
            // apply will do it for us
            break;
        }
        auto [seq_it, inserted] = _log_state.seq_table.try_emplace(bid.pid);
        if (inserted) {
            seq_it->second.pid = bid.pid;
            seq_it->second.seq = front->last_seq;
            seq_it->second.last_offset = front->r.value().last_offset;
        } else {
            seq_it->second.update(
              front->last_seq, front->r.value().last_offset);
        }
        session->cache.pop_front();
    }

    if (session->cache.empty() && session->lock.ready()) {
        _inflight_requests.erase(bid.pid);
    }

    co_return request->r;
}

ss::future<result<kafka_result>> rm_stm::replicate_msg(
  model::record_batch_reader br,
  raft::replicate_options opts,
  ss::lw_shared_ptr<available_promise<>> enqueued) {
    using ret_t = result<kafka_result>;

    if (!co_await sync(_sync_timeout)) {
        co_return errc::not_leader;
    }

    auto ss = _c->replicate_in_stages(_insync_term, std::move(br), opts);
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
    auto first_tx_start = model::offset::max();

    if (_is_tx_enabled) {
        if (!_log_state.ongoing_set.empty()) {
            first_tx_start = *_log_state.ongoing_set.begin();
        }

        if (!_mem_state.tx_starts.empty()) {
            first_tx_start = std::min(
              first_tx_start, *_mem_state.tx_starts.begin());
        }

        for (auto& entry : _mem_state.estimated) {
            first_tx_start = std::min(first_tx_start, entry.second);
        }
    }

    auto last_visible_index = _c->last_visible_index();
    if (first_tx_start <= last_visible_index) {
        return first_tx_start;
    }

    return model::next_offset(last_visible_index);
}

static void filter_intersecting(
  std::vector<rm_stm::tx_range>& target,
  const std::vector<rm_stm::tx_range>& source,
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

ss::future<std::vector<rm_stm::tx_range>>
rm_stm::aborted_transactions(model::offset from, model::offset to) {
    return _state_lock.hold_read_lock().then(
      [from, to, this](ss::basic_rwlock<>::holder unit) mutable {
          return do_aborted_transactions(from, to).finally(
            [u = std::move(unit)] {});
      });
}

ss::future<std::vector<rm_stm::tx_range>>
rm_stm::do_aborted_transactions(model::offset from, model::offset to) {
    std::vector<rm_stm::tx_range> result;
    if (!_is_tx_enabled) {
        co_return result;
    }
    std::vector<abort_index> intersecting_idxes;
    for (const auto& idx : _log_state.abort_indexes) {
        if (idx.last < from) {
            continue;
        }
        if (idx.first > to) {
            continue;
        }
        if (_log_state.last_abort_snapshot.match(idx)) {
            auto opt = _log_state.last_abort_snapshot;
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
    co_return result;
}

void rm_stm::compact_snapshot() {
    auto cutoff_timestamp = model::timestamp::now().value()
                            - _transactional_id_expiration.count();
    if (cutoff_timestamp <= _oldest_session.value()) {
        return;
    }

    if (_log_state.seq_table.size() <= _seq_table_min_size) {
        return;
    }

    if (_log_state.seq_table.size() == 0) {
        return;
    }

    std::vector<model::timestamp::type> lw_tss;
    lw_tss.reserve(_log_state.seq_table.size());
    for (auto it = _log_state.seq_table.cbegin();
         it != _log_state.seq_table.cend();
         it++) {
        lw_tss.push_back(it->second.last_write_timestamp);
    }
    std::sort(lw_tss.begin(), lw_tss.end());
    auto pivot = lw_tss[lw_tss.size() - 1 - _seq_table_min_size];

    if (pivot < cutoff_timestamp) {
        cutoff_timestamp = pivot;
    }

    auto next_oldest_session = model::timestamp::now();
    auto size = _log_state.seq_table.size();
    for (auto it = _log_state.seq_table.cbegin();
         it != _log_state.seq_table.cend();) {
        if (
          size > _seq_table_min_size
          && it->second.last_write_timestamp <= cutoff_timestamp) {
            size--;
            _log_state.seq_table.erase(it++);
        } else {
            next_oldest_session = std::min(
              next_oldest_session,
              model::timestamp(it->second.last_write_timestamp));
            ++it;
        }
    }
    _oldest_session = next_oldest_session;
}

ss::future<bool> rm_stm::sync(model::timeout_clock::duration timeout) {
    auto ready = co_await persisted_stm::sync(timeout);
    if (ready) {
        if (_mem_state.term != _insync_term) {
            _mem_state = mem_state{.term = _insync_term};
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
    _mem_state.expiration[pid] = expiration_info{
      .timeout = transaction_timeout_ms,
      .last_update = clock_type::now(),
      .is_expiration_requested = false};
    if (!_is_autoabort_enabled) {
        return;
    }
    auto deadline = _mem_state.expiration[pid].deadline();
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

ss::future<> rm_stm::do_abort_old_txes() {
    if (!co_await sync(_sync_timeout)) {
        co_return;
    }

    std::vector<model::producer_identity> pids;
    for (auto& [k, _] : _mem_state.estimated) {
        pids.push_back(k);
    }
    for (auto& [k, _] : _mem_state.tx_start) {
        pids.push_back(k);
    }
    for (auto& [k, _] : _log_state.ongoing_map) {
        pids.push_back(k);
    }
    absl::btree_set<model::producer_identity> expired;
    for (auto pid : pids) {
        auto expiration_it = _mem_state.expiration.find(pid);
        if (expiration_it != _mem_state.expiration.end()) {
            if (!expiration_it->second.is_expired(clock_type::now())) {
                continue;
            }
        }
        expired.insert(pid);
    }

    for (auto pid : expired) {
        co_await try_abort_old_tx(pid);
    }

    std::optional<time_point_type> earliest_deadline;
    for (auto& [pid, expiration] : _mem_state.expiration) {
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
        return do_try_abort_old_tx(pid);
    });
}

ss::future<> rm_stm::do_try_abort_old_tx(model::producer_identity pid) {
    if (!co_await sync(_sync_timeout)) {
        co_return;
    }
    auto synced_term = _insync_term;
    // catching up with all previous end_tx operations (commit | abort)
    // to avoid writing the same commit | abort marker twice
    if (_mem_state.last_end_tx >= model::offset{0}) {
        if (!co_await wait_no_throw(_mem_state.last_end_tx, _sync_timeout)) {
            co_return;
        }
    }

    if (!is_known_session(pid)) {
        co_return;
    }

    auto expiration_it = _mem_state.expiration.find(pid);
    if (expiration_it != _mem_state.expiration.end()) {
        if (!expiration_it->second.is_expired(clock_type::now())) {
            co_return;
        }
    }

    _mem_state.expected.erase(pid);

    std::optional<prepare_marker> marker;
    auto prepare_it = _mem_state.preparing.find(pid);
    if (prepare_it != _mem_state.preparing.end()) {
        marker = prepare_it->second;
    }
    prepare_it = _log_state.prepared.find(pid);
    if (prepare_it != _log_state.prepared.end()) {
        marker = prepare_it->second;
    }

    if (marker) {
        auto r = co_await _tx_gateway_frontend.local().try_abort(
          marker->tm_partition, marker->pid, marker->tx_seq, _sync_timeout);
        if (r.ec == tx_errc::none) {
            if (r.commited) {
                auto batch = make_tx_control_batch(
                  pid, model::control_record_type::tx_commit);
                auto reader = model::make_memory_record_batch_reader(
                  std::move(batch));
                auto cr = co_await _c->replicate(
                  synced_term,
                  std::move(reader),
                  raft::replicate_options(raft::consistency_level::quorum_ack));
                if (!cr) {
                    vlog(
                      _ctx_log.error,
                      "Error \"{}\" on replicating pid:{} autoabort/commit "
                      "batch",
                      cr.error(),
                      pid);
                    co_return;
                }
                if (_mem_state.last_end_tx < cr.value().last_offset) {
                    _mem_state.last_end_tx = cr.value().last_offset;
                }
            } else if (r.aborted) {
                auto batch = make_tx_control_batch(
                  pid, model::control_record_type::tx_abort);
                auto reader = model::make_memory_record_batch_reader(
                  std::move(batch));
                auto cr = co_await _c->replicate(
                  synced_term,
                  std::move(reader),
                  raft::replicate_options(raft::consistency_level::quorum_ack));
                if (!cr) {
                    vlog(
                      _ctx_log.error,
                      "Error \"{}\" on replicating pid:{} autoabort/abort "
                      "batch",
                      cr.error(),
                      pid);
                    co_return;
                }
                if (_mem_state.last_end_tx < cr.value().last_offset) {
                    _mem_state.last_end_tx = cr.value().last_offset;
                }
            }
        }
    } else {
        auto batch = make_tx_control_batch(
          pid, model::control_record_type::tx_abort);
        auto reader = model::make_memory_record_batch_reader(std::move(batch));
        auto cr = co_await _c->replicate(
          _insync_term,
          std::move(reader),
          raft::replicate_options(raft::consistency_level::quorum_ack));
        if (!cr) {
            vlog(
              _ctx_log.error,
              "Error \"{}\" on replicating pid:{} autoabort/abort batch",
              cr.error(),
              pid);
            co_return;
        }
        if (_mem_state.last_end_tx < cr.value().last_offset) {
            _mem_state.last_end_tx = cr.value().last_offset;
        }
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
    if (version == rm_stm::fence_control_record_version) {
        tx_seq = reflection::adl<model::tx_seq>{}.from(val_reader);
        transaction_timeout_ms
          = reflection::adl<std::chrono::milliseconds>{}.from(val_reader);
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

    auto [fence_it, _] = _log_state.fence_pid_epoch.try_emplace(
      bid.pid.get_id(), bid.pid.get_epoch());
    // using less-or-equal to update tx_seqs on every transaction
    if (fence_it->second <= bid.pid.get_epoch()) {
        fence_it->second = bid.pid.get_epoch();
        if (tx_seq.has_value()) {
            _log_state.tx_seqs[bid.pid] = tx_seq.value();
        }
    }
}

ss::future<> rm_stm::apply(model::record_batch b) {
    auto last_offset = b.last_offset();

    const auto& hdr = b.header();

    if (hdr.type == model::record_batch_type::tx_fence) {
        apply_fence(std::move(b));
    } else if (hdr.type == model::record_batch_type::tx_prepare) {
        apply_prepare(parse_prepare_batch(std::move(b)));
    } else if (hdr.type == model::record_batch_type::raft_data) {
        auto bid = model::batch_identity::from(hdr);
        if (hdr.attrs.is_control()) {
            co_await apply_control(bid.pid, parse_control_batch(b));
        } else {
            apply_data(bid, last_offset);
        }
    }

    _insync_offset = last_offset;

    compact_snapshot();
    if (_is_autoabort_enabled && !_is_autoabort_active) {
        abort_old_txes();
    }
}

void rm_stm::apply_prepare(rm_stm::prepare_marker prepare) {
    auto pid = prepare.pid;
    _log_state.prepared.try_emplace(pid, prepare);
    _mem_state.expected.erase(pid);
    _mem_state.preparing.erase(pid);
}

ss::future<> rm_stm::apply_control(
  model::producer_identity pid, model::control_record_type crt) {
    // either epoch is the same as fencing or it's lesser in the latter
    // case we don't fence off aborts and commits because transactional
    // manager already decided a tx's outcome and acked it to the client

    if (crt == model::control_record_type::tx_abort) {
        _log_state.prepared.erase(pid);
        auto offset_it = _log_state.ongoing_map.find(pid);
        if (offset_it != _log_state.ongoing_map.end()) {
            // make a list
            _log_state.aborted.push_back(offset_it->second);
            _log_state.ongoing_set.erase(offset_it->second.first);
            _log_state.ongoing_map.erase(pid);
        }

        _mem_state.forget(pid);

        if (
          _log_state.aborted.size() > _abort_index_segment_size
          && !_is_abort_idx_reduction_requested) {
            ssx::spawn_with_gate(
              _gate, [this] { return reduce_aborted_list(); });
        }
    } else if (crt == model::control_record_type::tx_commit) {
        _log_state.prepared.erase(pid);
        auto offset_it = _log_state.ongoing_map.find(pid);
        if (offset_it != _log_state.ongoing_map.end()) {
            _log_state.ongoing_set.erase(offset_it->second.first);
            _log_state.ongoing_map.erase(pid);
        }

        _mem_state.forget(pid);
    }

    co_return;
}

ss::future<> rm_stm::reduce_aborted_list() {
    if (_is_abort_idx_reduction_requested) {
        return ss::now();
    }
    if (_log_state.aborted.size() <= _abort_index_segment_size) {
        return ss::now();
    }
    _is_abort_idx_reduction_requested = true;
    return make_snapshot().finally(
      [this] { _is_abort_idx_reduction_requested = false; });
}

void rm_stm::apply_data(model::batch_identity bid, model::offset last_offset) {
    if (bid.has_idempotent()) {
        auto [seq_it, inserted] = _log_state.seq_table.try_emplace(bid.pid);
        auto translated = from_log_offset(last_offset);
        if (inserted) {
            seq_it->second.pid = bid.pid;
            seq_it->second.seq = bid.last_seq;
            seq_it->second.last_offset = translated;
        } else {
            seq_it->second.update(bid.last_seq, translated);
        }
        seq_it->second.last_write_timestamp = bid.first_timestamp.value();
        _oldest_session = std::min(_oldest_session, bid.first_timestamp);
    }

    if (bid.is_transactional) {
        if (_log_state.prepared.contains(bid.pid)) {
            vlog(
              _ctx_log.error,
              "Adding a record with pid:{} to a tx after it was prepared",
              bid.pid);
            if (_recovery_policy != best_effort) {
                vassert(
                  false,
                  "Adding a record with pid:{} to a tx after it was prepared",
                  bid.pid);
            }
            return;
        }

        auto ongoing_it = _log_state.ongoing_map.find(bid.pid);
        if (ongoing_it != _log_state.ongoing_map.end()) {
            if (ongoing_it->second.last < last_offset) {
                ongoing_it->second.last = last_offset;
            }
        } else {
            auto base_offset = model::offset(
              last_offset() - (bid.record_count - 1));
            _log_state.ongoing_map.emplace(
              bid.pid,
              tx_range{
                .pid = bid.pid,
                .first = base_offset,
                .last = model::offset(last_offset)});
            _log_state.ongoing_set.insert(base_offset);
            _mem_state.estimated.erase(bid.pid);
        }
    }
}

ss::future<>
rm_stm::apply_snapshot(stm_snapshot_header hdr, iobuf&& tx_ss_buf) {
    tx_snapshot data;
    iobuf_parser data_parser(std::move(tx_ss_buf));
    if (hdr.version == tx_snapshot::version) {
        data = reflection::adl<tx_snapshot>{}.from(data_parser);
    } else if (hdr.version == tx_snapshot_v1::version) {
        auto data_v1 = reflection::adl<tx_snapshot_v1>{}.from(data_parser);
        data.fenced = std::move(data_v1.fenced);
        data.ongoing = std::move(data_v1.ongoing);
        data.prepared = std::move(data_v1.prepared);
        data.aborted = std::move(data_v1.aborted);
        data.abort_indexes = std::move(data_v1.abort_indexes);
        data.offset = std::move(data_v1.offset);
        for (auto& seq_v1 : data_v1.seqs) {
            seq_entry seq;
            seq.pid = seq_v1.pid;
            seq.seq = seq_v1.seq;
            try {
                seq.last_offset = from_log_offset(seq_v1.last_offset);
            } catch (...) {
                // ignoring outside the translation range errors
                continue;
            }
            seq.seq_cache.reserve(seq_v1.seq_cache.size());
            for (auto& item : seq_v1.seq_cache) {
                try {
                    seq.seq_cache.push_back(seq_cache_entry{
                      .seq = item.seq, .offset = from_log_offset(item.offset)});
                } catch (...) {
                    // ignoring outside the translation range errors
                    continue;
                }
            }
            seq.last_write_timestamp = seq_v1.last_write_timestamp;
            data.seqs.push_back(std::move(seq));
        }
    } else if (hdr.version == tx_snapshot_v0::version) {
        auto data_v0 = reflection::adl<tx_snapshot_v0>{}.from(data_parser);
        data.fenced = std::move(data_v0.fenced);
        data.ongoing = std::move(data_v0.ongoing);
        data.prepared = std::move(data_v0.prepared);
        data.aborted = std::move(data_v0.aborted);
        data.abort_indexes = std::move(data_v0.abort_indexes);
        data.offset = std::move(data_v0.offset);
        for (auto seq_v0 : data_v0.seqs) {
            auto seq = seq_entry{
              .pid = seq_v0.pid,
              .seq = seq_v0.seq,
              .last_offset = kafka::offset{-1},
              .last_write_timestamp = seq_v0.last_write_timestamp};
            data.seqs.push_back(std::move(seq));
        }
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
    for (auto& entry : data.prepared) {
        _log_state.prepared.emplace(entry.pid, entry);
    }
    _log_state.aborted.insert(
      _log_state.aborted.end(),
      std::make_move_iterator(data.aborted.begin()),
      std::make_move_iterator(data.aborted.end()));
    _log_state.abort_indexes.insert(
      _log_state.abort_indexes.end(),
      std::make_move_iterator(data.abort_indexes.begin()),
      std::make_move_iterator(data.abort_indexes.end()));
    for (auto& entry : data.seqs) {
        auto [seq_it, inserted] = _log_state.seq_table.try_emplace(
          entry.pid, std::move(entry));
        // try_emplace does not move from r-value reference if the insertion
        // didn't take place so the clang-tidy warning is a false positive
        // NOLINTNEXTLINE(hicpp-invalid-access-moved)
        if (!inserted && seq_it->second.seq < entry.seq) {
            seq_it->second = std::move(entry);
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
            _log_state.last_abort_snapshot = snapshot_opt.value();
        }
    }

    _last_snapshot_offset = data.offset;
    _insync_offset = data.offset;
}

uint8_t rm_stm::active_snapshot_version() {
    if (_feature_table.local().is_active(
          features::feature::rm_stm_kafka_cache)) {
        return tx_snapshot::version;
    }
    return tx_snapshot_v1::version;
}

template<class T>
void rm_stm::fill_snapshot_wo_seqs(T& snapshot) {
    for (auto const& [k, v] : _log_state.fence_pid_epoch) {
        snapshot.fenced.push_back(model::producer_identity{k(), v()});
    }
    for (auto& entry : _log_state.ongoing_map) {
        snapshot.ongoing.push_back(entry.second);
    }
    for (auto& entry : _log_state.prepared) {
        snapshot.prepared.push_back(entry.second);
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
            co_await save_abort_snapshot(snapshot);
            snapshot = abort_snapshot{
              .first = model::offset::max(), .last = model::offset::min()};
        }
    }
    _log_state.aborted = snapshot.aborted;
}

ss::future<stm_snapshot> rm_stm::take_snapshot() {
    auto start_offset = _raft->start_offset();

    std::vector<abort_index> abort_indexes;
    std::vector<abort_index> expired_abort_indexes;
    abort_indexes.reserve(_log_state.abort_indexes.size());

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

    for (const auto& idx : expired_abort_indexes) {
        auto filename = abort_idx_name(idx.first, idx.last);
        co_await _abort_snapshot_mgr.remove_snapshot(filename);
    }

    std::vector<tx_range> aborted;
    aborted.reserve(_log_state.aborted.size());
    std::copy_if(
      _log_state.aborted.begin(),
      _log_state.aborted.end(),
      std::back_inserter(aborted),
      [start_offset](tx_range range) { return range.last >= start_offset; });
    _log_state.aborted = std::move(aborted);

    if (_log_state.aborted.size() > _abort_index_segment_size) {
        co_await _state_lock.hold_write_lock().then(
          [this](ss::basic_rwlock<>::holder unit) {
              // software engineer be careful and do not cause a deadlock.
              // take_snapshot is invoked under the persisted_stm::_op_lock
              // and here here we take write lock (_state_lock). most rm_stm
              // operations require its read lock. however they don't depend
              // of _op_lock so things are safe now
              return offload_aborted_txns().finally([u = std::move(unit)] {});
          });
    }

    iobuf tx_ss_buf;
    auto version = active_snapshot_version();
    if (version == tx_snapshot::version) {
        tx_snapshot tx_ss;
        fill_snapshot_wo_seqs(tx_ss);
        for (const auto& entry : _log_state.seq_table) {
            tx_ss.seqs.push_back(entry.second.copy());
        }
        tx_ss.offset = _insync_offset;
        reflection::adl<tx_snapshot>{}.to(tx_ss_buf, std::move(tx_ss));
    } else if (version == tx_snapshot_v1::version) {
        tx_snapshot_v1 tx_ss;
        fill_snapshot_wo_seqs(tx_ss);
        for (const auto& it : _log_state.seq_table) {
            auto& entry = it.second;
            seq_entry_v1 seqs;
            seqs.pid = entry.pid;
            seqs.seq = entry.seq;
            try {
                seqs.last_offset = to_log_offset(entry.last_offset);
            } catch (...) {
                // ignoring outside the translation range errors
                continue;
            }
            seqs.last_write_timestamp = entry.last_write_timestamp;
            seqs.seq_cache.reserve(seqs.seq_cache.size());
            for (auto& item : entry.seq_cache) {
                try {
                    seqs.seq_cache.push_back(seq_cache_entry_v1{
                      .seq = item.seq, .offset = to_log_offset(item.offset)});
                } catch (...) {
                    // ignoring outside the translation range errors
                    continue;
                }
            }
            tx_ss.seqs.push_back(std::move(seqs));
        }
        tx_ss.offset = _insync_offset;
        reflection::adl<tx_snapshot_v1>{}.to(tx_ss_buf, std::move(tx_ss));
    } else {
        vassert(false, "unsupported tx_snapshot version {}", version);
    }

    co_return stm_snapshot::create(
      version, _insync_offset, std::move(tx_ss_buf));
}

ss::future<> rm_stm::save_abort_snapshot(abort_snapshot snapshot) {
    iobuf snapshot_data;
    reflection::adl<abort_snapshot>{}.to(snapshot_data, snapshot);
    int32_t snapshot_size = snapshot_data.size_bytes();

    auto filename = abort_idx_name(snapshot.first, snapshot.last);
    auto writer = co_await _abort_snapshot_mgr.start_snapshot(filename);

    iobuf metadata_buf;
    reflection::serialize(metadata_buf, abort_snapshot_version, snapshot_size);
    co_await writer.write_metadata(std::move(metadata_buf));
    co_await write_iobuf_to_output_stream(
      std::move(snapshot_data), writer.output());
    co_await writer.close();
    co_await _abort_snapshot_mgr.finish_snapshot(writer);
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
    for (const auto& idx : _log_state.abort_indexes) {
        auto filename = abort_idx_name(idx.first, idx.last);
        co_await _abort_snapshot_mgr.remove_snapshot(filename);
    }
    co_await _abort_snapshot_mgr.remove_partial_snapshots();
    co_return co_await persisted_stm::remove_persistent_state();
}

ss::future<> rm_stm::handle_eviction() {
    return _state_lock.hold_write_lock().then(
      [this]([[maybe_unused]] ss::basic_rwlock<>::holder unit) {
          _log_state = {};
          _mem_state = {};
          set_next(_c->start_offset());
          return ss::now();
      });
}

} // namespace cluster

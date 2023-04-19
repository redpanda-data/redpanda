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
#include "cluster/tx_gateway_frontend.h"
#include "kafka/protocol/wire.h"
#include "model/fundamental.h"
#include "model/record.h"
#include "prometheus/prometheus_sanitize.h"
#include "raft/consensus_utils.h"
#include "raft/errc.h"
#include "raft/types.h"
#include "ssx/future-util.h"
#include "ssx/metrics.h"
#include "storage/parser_utils.h"
#include "storage/record_batch_builder.h"
#include "utils/human.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/future.hh>
#include <seastar/core/loop.hh>

#include <filesystem>
#include <optional>

namespace cluster {
using namespace std::chrono_literals;

static ss::sstring abort_idx_name(model::offset first, model::offset last) {
    return fmt::format("abort.idx.{}.{}", first, last);
}

static bool is_sequence(int32_t last_seq, int32_t next_seq) {
    return (last_seq + 1 == next_seq)
           || (next_seq == 0 && last_seq == std::numeric_limits<int32_t>::max());
}

model::record_batch make_fence_batch_v0(model::producer_identity pid) {
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
    kafka::protocol::decoder key_reader(std::move(key));
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

    fragmented_vector<model::producer_identity> fenced;
    fragmented_vector<rm_stm::tx_range> ongoing;
    fragmented_vector<rm_stm::prepare_marker> prepared;
    fragmented_vector<rm_stm::tx_range> aborted;
    fragmented_vector<rm_stm::abort_index> abort_indexes;
    model::offset offset;
    fragmented_vector<seq_entry_v0> seqs;
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

    fragmented_vector<model::producer_identity> fenced;
    fragmented_vector<rm_stm::tx_range> ongoing;
    fragmented_vector<rm_stm::prepare_marker> prepared;
    fragmented_vector<rm_stm::tx_range> aborted;
    fragmented_vector<rm_stm::abort_index> abort_indexes;
    model::offset offset;
    fragmented_vector<seq_entry_v1> seqs;
};

struct tx_snapshot_v2 {
    static constexpr uint8_t version = 2;

    fragmented_vector<model::producer_identity> fenced;
    fragmented_vector<rm_stm::tx_range> ongoing;
    fragmented_vector<rm_stm::prepare_marker> prepared;
    fragmented_vector<rm_stm::tx_range> aborted;
    fragmented_vector<rm_stm::abort_index> abort_indexes;
    model::offset offset;
    fragmented_vector<rm_stm::seq_entry> seqs;
};

struct tx_snapshot_v3 {
    static constexpr uint8_t version = 3;

    fragmented_vector<model::producer_identity> fenced;
    fragmented_vector<rm_stm::tx_range> ongoing;
    fragmented_vector<rm_stm::prepare_marker> prepared;
    fragmented_vector<rm_stm::tx_range> aborted;
    fragmented_vector<rm_stm::abort_index> abort_indexes;
    model::offset offset;
    fragmented_vector<rm_stm::seq_entry> seqs;

    struct tx_seqs_snapshot {
        model::producer_identity pid;
        model::tx_seq tx_seq;
    };

    fragmented_vector<tx_seqs_snapshot> tx_seqs;
    fragmented_vector<rm_stm::tx_snapshot::expiration_snapshot> expiration;
};

rm_stm::rm_stm(
  ss::logger& logger,
  raft::consensus* c,
  ss::sharded<cluster::tx_gateway_frontend>& tx_gateway_frontend,
  ss::sharded<features::feature_table>& feature_table,
  config::binding<uint64_t> max_concurrent_producer_ids)
  : persisted_stm("tx.snapshot", logger, c)
  , _tx_locks(
      mt::
        map<absl::flat_hash_map, model::producer_id, ss::lw_shared_ptr<mutex>>(
          _tx_root_tracker.create_child("tx-locks")))
  , _inflight_requests(mt::map<
                       absl::flat_hash_map,
                       model::producer_identity,
                       ss::lw_shared_ptr<inflight_requests>>(
      _tx_root_tracker.create_child("in-flight")))
  , _idempotent_producer_locks(mt::map<
                               absl::flat_hash_map,
                               model::producer_identity,
                               ss::lw_shared_ptr<ss::basic_rwlock<>>>(
      _tx_root_tracker.create_child("idempotent-producer-locks")))
  , _log_state(_tx_root_tracker)
  , _mem_state(_tx_root_tracker)
  , _oldest_session(model::timestamp::now())
  , _sync_timeout(config::shard_local_cfg().rm_sync_timeout_ms.value())
  , _tx_timeout_delay(config::shard_local_cfg().tx_timeout_delay_ms.value())
  , _abort_interval_ms(config::shard_local_cfg()
                         .abort_timed_out_transactions_interval_ms.value())
  , _abort_index_segment_size(
      config::shard_local_cfg().abort_index_segment_size.value())
  , _seq_table_min_size(config::shard_local_cfg().seq_table_min_size.value())
  , _transactional_id_expiration(
      config::shard_local_cfg().transactional_id_expiration_ms.value())
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
  , _max_concurrent_producer_ids(max_concurrent_producer_ids) {
    setup_metrics();
    if (!_is_tx_enabled) {
        _is_autoabort_enabled = false;
    }
    auto_abort_timer.set_callback([this] { abort_old_txes(); });
    _log_stats_timer.set_callback([this] { log_tx_stats(); });
    _log_stats_timer.arm(clock_type::now() + _log_stats_interval_s());
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

model::record_batch rm_stm::make_fence_batch(
  model::producer_identity pid,
  model::tx_seq tx_seq,
  std::chrono::milliseconds transaction_timeout_ms,
  model::partition_id tm) {
    if (is_transaction_partitioning()) {
        return make_fence_batch_v2(pid, tx_seq, transaction_timeout_ms, tm);
    } else if (is_transaction_ga()) {
        return make_fence_batch_v1(pid, tx_seq, transaction_timeout_ms);
    } else {
        return make_fence_batch_v0(pid);
    }
}

ss::future<checked<model::term_id, tx_errc>> rm_stm::do_begin_tx(
  model::producer_identity pid,
  model::tx_seq tx_seq,
  std::chrono::milliseconds transaction_timeout_ms,
  model::partition_id tm) {
    if (!check_tx_permitted()) {
        co_return tx_errc::request_rejected;
    }

    if (!_c->is_leader()) {
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
    auto r = co_await _c->replicate(
      synced_term,
      std::move(reader),
      raft::replicate_options(raft::consistency_level::quorum_ack));

    if (!r) {
        vlog(
          _ctx_log.trace,
          "Error \"{}\" on replicating pid:{} tx_seq:{} fencing batch",
          r.error(),
          pid,
          tx_seq);
        if (_c->is_leader() && _c->term() == synced_term) {
            co_await _c->step_down("begin_tx replication error");
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
        if (_c->is_leader() && _c->term() == synced_term) {
            co_await _c->step_down("begin_tx apply error");
        }
        co_return tx_errc::timeout;
    }

    if (_c->term() != synced_term) {
        vlog(
          _ctx_log.trace,
          "term changed from {} to {} during fencing pid:{} tx_seq:{}",
          synced_term,
          _c->term(),
          pid,
          tx_seq);
        co_return tx_errc::leader_not_found;
    }

    if (is_transaction_ga()) {
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
    _log_state.erase_pid_from_seq_table(pid);

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

    if (_log_state.current_txes.contains(pid)) {
        if (_log_state.current_txes[pid].tx_seq != tx_seq) {
            vlog(
              _ctx_log.warn,
              "expectd tx_seq {} doesn't match gived {} for pid {}",
              _log_state.current_txes[pid].tx_seq,
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
          model::offset(r.value().last_offset()),
          model::timeout_clock::now() + timeout)) {
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
    // catching up with all previous end_tx operations (commit | abort)
    // to avoid writing the same commit | abort marker twice
    if (_mem_state.last_end_tx >= model::offset{0}) {
        if (!co_await wait_no_throw(
              _mem_state.last_end_tx, model::timeout_clock::now() + timeout)) {
            co_return tx_errc::stale;
        }
    }

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
    } else {
        auto preparing_it = _mem_state.preparing.find(pid);
        if (preparing_it != _mem_state.preparing.end()) {
            ss::sstring msg;
            if (preparing_it->second.tx_seq > tx_seq) {
                // - tm_stm & rm_stm failed during prepare
                // - during recovery tm_stm recommits its previous tx
                // - that commit (we're here) collides with "next" failed
                // prepare it may happen only if the commit passed => acking
                co_return tx_errc::none;
            } else if (preparing_it->second.tx_seq == tx_seq) {
                msg = ssx::sformat(
                  "can't commit pid:{} tx_seq:{} - prepare request hasn't "
                  "completed",
                  pid,
                  tx_seq);
            } else {
                msg = ssx::sformat(
                  "can't commit pid:{} tx_seq:{} - it conflicts with observed "
                  "tx_seq:{}",
                  pid,
                  tx_seq,
                  preparing_it->second.tx_seq);
            }

            vlog(_ctx_log.error, "{}", msg);
            co_return tx_errc::request_rejected;
        }

        auto prepare_it = _log_state.prepared.find(pid);
        if (prepare_it == _log_state.prepared.end()) {
            co_return tx_errc::none;
        } else if (prepare_it->second.tx_seq > tx_seq) {
            // rare situation:
            //   * tm_stm prepares (tx_seq+1)
            //   * prepare on this rm passes but tm_stm failed to write to disk
            //   * during recovery tm_stm recommits (tx_seq)
            // existence of {pid, tx_seq+1} implies {pid, tx_seq} is committed
            vlog(
              _ctx_log.trace,
              "Already commited pid:{} tx_seq:{} - a higher tx_seq:{} was "
              "observed",
              pid,
              tx_seq,
              prepare_it->second.tx_seq);
            co_return tx_errc::none;
        } else if (prepare_it->second.tx_seq < tx_seq) {
            vlog(
              _ctx_log.error,
              "Can't commit pid:{} tx_seq:{} - it conflicts with observed "
              "tx_seq:{}",
              pid,
              tx_seq,
              prepare_it->second.tx_seq);
            co_return tx_errc::request_rejected;
        }
        _mem_state.preparing.erase(pid);
    }

    _mem_state.expected.erase(pid);

    auto batch = make_tx_control_batch(
      pid, model::control_record_type::tx_commit);
    auto reader = model::make_memory_record_batch_reader(std::move(batch));
    auto r = co_await _c->replicate(
      synced_term,
      std::move(reader),
      raft::replicate_options(raft::consistency_level::quorum_ack));

    if (!r) {
        vlog(
          _ctx_log.warn,
          "Error \"{}\" on replicating pid:{} commit batch",
          r.error(),
          pid);
        if (_c->is_leader() && _c->term() == synced_term) {
            co_await _c->step_down("do_commit_tx replication error");
        }
        co_return tx_errc::timeout;
    }
    if (!co_await wait_no_throw(
          r.value().last_offset, model::timeout_clock::now() + timeout)) {
        if (_c->is_leader() && _c->term() == synced_term) {
            co_await _c->step_down("do_commit_tx wait error");
        }
        co_return tx_errc::timeout;
    }

    if (_mem_state.last_end_tx < r.value().last_offset) {
        _mem_state.last_end_tx = r.value().last_offset;
    }

    co_return tx_errc::none;
}

abort_origin rm_stm::get_abort_origin(
  const model::producer_identity& pid, model::tx_seq tx_seq) const {
    if (is_transaction_ga()) {
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
    // catching up with all previous end_tx operations (commit | abort)
    // to avoid writing the same commit | abort marker twice
    if (_mem_state.last_end_tx >= model::offset{0}) {
        if (!co_await wait_no_throw(
              _mem_state.last_end_tx, model::timeout_clock::now() + timeout)) {
            vlog(
              _ctx_log.trace,
              "Can't catch up to abort pid:{} tx_seq:{}",
              pid,
              tx_seq.value_or(model::tx_seq(-1)));
            co_return tx_errc::stale;
        }
    }

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
          _ctx_log.info,
          "Error \"{}\" on replicating pid:{} tx_seq:{} abort batch",
          r.error(),
          pid,
          tx_seq.value_or(model::tx_seq(-1)));
        if (_c->is_leader() && _c->term() == synced_term) {
            co_await _c->step_down("abort_tx replication error");
        }
        co_return tx_errc::timeout;
    }
    if (_mem_state.last_end_tx < r.value().last_offset) {
        _mem_state.last_end_tx = r.value().last_offset;
    }

    if (!co_await wait_no_throw(
          r.value().last_offset, model::timeout_clock::now() + timeout)) {
        vlog(
          _ctx_log.trace,
          "timeout on waiting until {} is applied (abort_tx pid:{} tx_seq:{})",
          r.value().last_offset,
          pid,
          tx_seq.value_or(model::tx_seq(-1)));
        if (_c->is_leader() && _c->term() == synced_term) {
            co_await _c->step_down("abort_tx apply error");
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
    _log_stats_timer.cancel();
    return raft::state_machine::stop();
}

ss::future<> rm_stm::start() {
    _translator = _c->get_offset_translator_state();
    return persisted_stm::start();
}

rm_stm::transaction_info::status_t
rm_stm::get_tx_status(model::producer_identity pid) const {
    if (is_transaction_ga()) {
        return transaction_info::status_t::ongoing;
    }

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
    auto it = _log_state.expiration.find(pid);
    if (it == _log_state.expiration.end()) {
        return std::nullopt;
    }

    return it->second;
}

std::optional<int32_t>
rm_stm::get_seq_number(model::producer_identity pid) const {
    auto it = _log_state.seq_table.find(pid);
    if (it == _log_state.seq_table.end()) {
        return std::nullopt;
    }

    return it->second.entry.seq;
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

    if (!is_transaction_ga()) {
        for (auto& [id, offset] : _mem_state.tx_start) {
            transaction_info tx_info;
            tx_info.lso_bound = offset;
            tx_info.status = get_tx_status(id);
            tx_info.info = get_expiration_info(id);
            tx_info.seq = get_seq_number(id);
            ans.emplace(id, tx_info);
        }
    }

    for (auto& [id, offset] : _log_state.ongoing_map) {
        transaction_info tx_info;
        tx_info.lso_bound = offset.first;
        tx_info.status = get_tx_status(id);
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

bool rm_stm::check_seq(model::batch_identity bid, model::term_id term) {
    auto& seq_it = _log_state.seq_table[bid.pid];
    auto last_write_timestamp = model::timestamp::now().value();

    if (!is_sequence(seq_it.entry.seq, bid.first_seq)) {
        vlog(
          _ctx_log.warn,
          "provided seq:{} for pid:{} isn't a continuation of the last known "
          "seq:{}",
          bid.first_seq,
          bid.pid,
          seq_it.entry.seq);
        return false;
    }

    seq_it.entry.update(bid.last_seq, kafka::offset{-1});

    seq_it.entry.pid = bid.pid;
    seq_it.entry.last_write_timestamp = last_write_timestamp;
    seq_it.term = term;
    _oldest_session = std::min(
      _oldest_session, model::timestamp(seq_it.entry.last_write_timestamp));

    return true;
}

std::optional<kafka::offset>
rm_stm::known_seq(model::batch_identity bid) const {
    auto pid_seq = _log_state.seq_table.find(bid.pid);
    if (pid_seq == _log_state.seq_table.end()) {
        return std::nullopt;
    }
    if (pid_seq->second.entry.seq == bid.last_seq) {
        return pid_seq->second.entry.last_offset;
    }
    for (auto& entry : pid_seq->second.entry.seq_cache) {
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
    return pid_seq->second.entry.seq;
}

void rm_stm::set_seq(model::batch_identity bid, kafka::offset last_offset) {
    auto pid_seq = _log_state.seq_table.find(bid.pid);
    if (pid_seq != _log_state.seq_table.end()) {
        if (pid_seq->second.entry.seq == bid.last_seq) {
            pid_seq->second.entry.last_offset = last_offset;
        }
    }
}

void rm_stm::reset_seq(model::batch_identity bid, model::term_id term) {
    _log_state.erase_pid_from_seq_table(bid.pid);
    auto& seq_it = _log_state.seq_table[bid.pid];
    seq_it.term = term;
    seq_it.entry.seq = bid.last_seq;
    seq_it.entry.last_offset = kafka::offset{-1};
    seq_it.entry.pid = bid.pid;
    seq_it.entry.last_write_timestamp = model::timestamp::now().value();
    _oldest_session = std::min(
      _oldest_session, model::timestamp(seq_it.entry.last_write_timestamp));
}

ss::future<result<kafka_result>>
rm_stm::replicate_tx(model::batch_identity bid, model::record_batch_reader br) {
    if (!check_tx_permitted()) {
        co_return errc::generic_tx_error;
    }

    if (!co_await sync(_sync_timeout)) {
        vlog(
          _ctx_log.trace,
          "processing name:replicate_tx pid:{} => stale leader",
          bid.pid);
        co_return errc::not_leader;
    }
    auto synced_term = _insync_term;
    vlog(
      _ctx_log.trace,
      "processing name:replicate_tx pid:{} in term:{}",
      bid.pid,
      synced_term);

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

    auto is_txn_ga = is_transaction_ga();

    if (is_txn_ga) {
        if (!_log_state.current_txes.contains(bid.pid)) {
            vlog(_ctx_log.warn, "can't find ongoing tx for pid:{}", bid.pid);
            co_return errc::invalid_producer_epoch;
        }
        auto tx_seq = _log_state.current_txes[bid.pid].tx_seq;
        vlog(_ctx_log.trace, "found tx_seq:{} for pid:{}", tx_seq, bid.pid);
    } else {
        if (!_mem_state.expected.contains(bid.pid)) {
            // there is an inflight abort
            // or this partition lost leadership => can't continue tx since
            // there is
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
              "Client keeps producing after it attempted commit pid:{}",
              bid.pid);
            co_return errc::invalid_request;
        }
    }

    if (_mem_state.estimated.contains(bid.pid)) {
        // we received second produce request while the first is still
        // being processed.
        vlog(_ctx_log.warn, "Too frequent produce with same pid:{}", bid.pid);
        co_return errc::generic_tx_error;
    }

    if (_log_state.ongoing_map.contains(bid.pid)) {
        // this isn't the first attempt in the tx we should try dedupe
        auto pid_seq = _log_state.seq_table.find(bid.pid);
        if (pid_seq == _log_state.seq_table.end()) {
            if (!check_seq(bid, synced_term)) {
                co_return errc::sequence_out_of_order;
            }
        } else if (pid_seq->second.entry.seq == bid.last_seq) {
            if (pid_seq->second.entry.last_offset() == -1) {
                if (pid_seq->second.term == synced_term) {
                    vlog(
                      _ctx_log.debug,
                      "Status of the original attempt pid:{} seq:{} is "
                      "unknown. Returning not_leader_for_partition to trigger "
                      "retry.",
                      bid.pid,
                      bid.last_seq);
                    co_return errc::not_leader;
                } else {
                    // when the term a tx originated on doesn't match the
                    // current term it means that the re-election happens; upon
                    // re-election we sync and it catches up with all records
                    // replicated in previous term. since the cached offset for
                    // a given seq is still -1 it means that the replication
                    // hasn't passed so we updating the term to pretent that the
                    // retry (seqs match) put it there
                    pid_seq->second.term = synced_term;
                }
            } else {
                vlog(
                  _ctx_log.trace,
                  "cache hit for pid:{} seq:{}",
                  bid.pid,
                  bid.last_seq);
                co_return kafka_result{
                  .last_offset = pid_seq->second.entry.last_offset};
            }
        } else {
            for (auto& entry : pid_seq->second.entry.seq_cache) {
                if (entry.seq == bid.last_seq) {
                    if (entry.offset() == -1) {
                        vlog(
                          _ctx_log.error,
                          "cache hit for pid:{} seq:{} resolves to -1 offset",
                          bid.pid,
                          entry.seq);
                    }
                    vlog(
                      _ctx_log.trace,
                      "cache hit for pid:{} seq:{}",
                      bid.pid,
                      entry.seq);
                    co_return kafka_result{.last_offset = entry.offset};
                }
            }
            if (!check_seq(bid, synced_term)) {
                co_return errc::sequence_out_of_order;
            }
        }
    } else {
        // this is the first attempt in the tx, reset dedupe cache
        reset_seq(bid, synced_term);

        _mem_state.estimated[bid.pid] = _insync_offset;
    }

    auto expiration_it = _log_state.expiration.find(bid.pid);
    if (expiration_it == _log_state.expiration.end()) {
        vlog(_ctx_log.warn, "Can not find expiration info for pid:{}", bid.pid);
        co_return errc::generic_tx_error;
    }
    expiration_it->second.last_update = clock_type::now();
    expiration_it->second.is_expiration_requested = false;

    auto r = co_await _c->replicate(
      synced_term,
      std::move(br),
      raft::replicate_options(raft::consistency_level::quorum_ack));
    if (!r) {
        vlog(
          _ctx_log.info,
          "got {} on replicating tx data batch for pid:{}",
          r.error(),
          bid.pid);
        if (_mem_state.estimated.contains(bid.pid)) {
            // an error during replication, preventin tx from progress
            _mem_state.expected.erase(bid.pid);
        }
        if (_c->is_leader() && _c->term() == synced_term) {
            co_await _c->step_down("replicate_tx replication error");
        }
        co_return r.error();
    }
    if (!co_await wait_no_throw(
          model::offset(r.value().last_offset()),
          model::timeout_clock::now() + _sync_timeout)) {
        vlog(
          _ctx_log.warn,
          "application of the replicated tx batch has timed out pid:{}",
          bid.pid);
        if (_c->is_leader() && _c->term() == synced_term) {
            co_await _c->step_down("replicate_tx wait error");
        }
        co_return tx_errc::timeout;
    }

    auto old_offset = r.value().last_offset;
    auto new_offset = from_log_offset(old_offset);

    set_seq(bid, new_offset);

    _mem_state.estimated.erase(bid.pid);

    if (!is_txn_ga) {
        if (!_mem_state.tx_start.contains(bid.pid)) {
            auto base_offset = model::offset(
              old_offset() - (bid.record_count - 1));
            _mem_state.tx_start.emplace(bid.pid, base_offset);
            _mem_state.tx_starts.insert(base_offset);
        }
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
          "[pid: {}] first_seq: {} of the batch should be less or equal to "
          "last_seq: {}",
          bid.pid,
          bid.first_seq,
          bid.last_seq);
        co_return errc::invalid_request;
    }

    auto idempotent_lock = get_idempotent_producer_lock(bid.pid);
    auto units = co_await idempotent_lock->hold_read_lock();

    // Double check that after hold read lock rw_lock still exists in rw map.
    // Becasue we can not continue replicate_seq if pid was deleted from state
    // after we lock mutex
    if (!_idempotent_producer_locks.contains(bid.pid)) {
        vlog(
          _ctx_log.error,
          "[pid: {}] Lock for pid was deleted from map after we hold it. "
          "Cleaning process was during replicate_seq",
          bid.pid);
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
        vlog(
          _ctx_log.debug,
          "[pid: {}] resource manager sync failed, not leader",
          bid.pid);
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
        vlog(
          _ctx_log.debug,
          "[pid: {}] session term {} is greater than synced term {}",
          bid.pid,
          session->term,
          synced_term);
        co_return errc::not_leader;
    }

    // checking if the request (identified by seq) is already resolved
    // checking among the pending requests
    auto cached_r = session->known_seq(bid.last_seq);
    if (cached_r) {
        vlog(
          _ctx_log.trace,
          "[pid: {}] request with last sequence: {}, already resolved",
          bid.pid,
          bid.last_seq);
        co_return cached_r.value();
    }
    // checking among the responded requests
    auto cached_offset = known_seq(bid);
    if (cached_offset) {
        vlog(
          _ctx_log.trace,
          "[pid: {}] request with last sequence: {}, already resolved in "
          "log_state",
          bid.pid,
          bid.last_seq);
        co_return kafka_result{.last_offset = cached_offset.value()};
    }

    // checking if the request is already being processed
    for (auto& inflight : session->cache) {
        if (inflight->last_seq == bid.last_seq && inflight->is_processing) {
            vlog(
              _ctx_log.trace,
              "[pid: {}] request with last sequence: {}, already resolved "
              "being processed",
              bid.pid,
              bid.last_seq);
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
                vlog(
                  _ctx_log.warn,
                  "[pid: {}] sequence number gap detected. batch first "
                  "sequence: {}, last stored sequence: {}",
                  bid.pid,
                  bid.first_seq,
                  tail.value());
                co_return errc::sequence_out_of_order;
            }
        } else {
            if (bid.first_seq != 0) {
                vlog(
                  _ctx_log.warn,
                  "[pid: {}] first sequence number in session must be zero, "
                  "current seq: {}",
                  bid.pid,
                  bid.first_seq);
                // first request in a session should have seq=0, client is
                // misbehaving
                co_return errc::sequence_out_of_order;
            }
        }
    } else {
        if (!is_sequence(session->tail_seq, bid.first_seq)) {
            // there is a gap between the request's seq number
            // and the seq number of the latest inflight request
            // may happen when:
            //   - batches were reordered on the wire
            //   - client is misbehaving
            vlog(
              _ctx_log.warn,
              "[pid: {}] sequence number gap detected. batch first sequence: "
              "{}, last inflight sequence: {}",
              bid.pid,
              bid.first_seq,
              session->tail_seq);
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
          "[pid: {}] replication failed with {}",
          bid.pid,
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
            co_await _c->step_down("replicate_seq replication error");
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
              "[pid: {}] replication failed with {}",
              bid.pid,
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
        vlog(
          _ctx_log.warn,
          "[pid: {}] replication of batch with first seq: {} failed at "
          "consensus level - error: {}",
          bid.pid,
          bid.first_seq,
          request->r.error().message());
        // if r was failed at the consensus level (not because has_failed)
        // it should guarantee that all follow up replication requests fail
        // too but just in case stepping down to minimize the risk
        if (_c->is_leader() && _c->term() == synced_term) {
            co_await _c->step_down("replicate_seq replication finished error");
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
            vlog(
              _ctx_log.debug,
              "[pid: {}] session cache front failed - error: {}",
              bid.pid,
              front->r.error().message());
            // just encountered a failed request it but when a request
            // fails it causes a step down so we may not care about
            // updating seq_table and stop doing it; the next leader's
            // apply will do it for us
            break;
        }
        auto [seq_it, inserted] = _log_state.seq_table.try_emplace(bid.pid);
        if (inserted) {
            seq_it->second.entry.pid = bid.pid;
            seq_it->second.entry.seq = front->last_seq;
            seq_it->second.entry.last_offset = front->r.value().last_offset;
        } else {
            seq_it->second.entry.update(
              front->last_seq, front->r.value().last_offset);
        }

        if (!bid.is_transactional) {
            _log_state.unlink_lru_pid(seq_it->second);
            _log_state.lru_idempotent_pids.push_back(seq_it->second);
        }

        session->cache.pop_front();
    }

    // We can not do any async work after replication is finished and we marked
    // request as finished. Becasue it can do reordering for requests. So we
    // need to spawn background cleaning thread
    spawn_background_clean_for_pids(rm_stm::clear_type::idempotent_pids);

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

        if (!is_transaction_ga()) {
            if (!_mem_state.tx_starts.empty()) {
                first_tx_start = std::min(
                  first_tx_start, *_mem_state.tx_starts.begin());
            }
        }

        for (auto& entry : _mem_state.estimated) {
            first_tx_start = std::min(first_tx_start, entry.second);
        }
    }

    auto last_visible_index = _c->last_visible_index();
    auto next_to_apply = model::next_offset(last_applied);
    if (first_tx_start <= last_visible_index) {
        // There are in flight transactions < high water mark that may
        // not be applied yet. We still need to consider only applied
        // transactions.
        return std::min(first_tx_start, next_to_apply);
    }
    // no inflight transactions.
    return model::next_offset(last_visible_index);
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

    fragmented_vector<model::timestamp::type> lw_tss;
    for (const auto& it : _log_state.seq_table) {
        lw_tss.push_back(it.second.entry.last_write_timestamp);
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
          && it->second.entry.last_write_timestamp <= cutoff_timestamp) {
            size--;
            _log_state.erase_pid_from_seq_table(it->first);
            it++;
        } else {
            next_oldest_session = std::min(
              next_oldest_session,
              model::timestamp(it->second.entry.last_write_timestamp));
            ++it;
        }
    }
    _oldest_session = next_oldest_session;
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

ss::future<> rm_stm::do_abort_old_txes() {
    if (!co_await sync(_sync_timeout)) {
        co_return;
    }

    fragmented_vector<model::producer_identity> pids;
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
        auto expiration_it = _log_state.expiration.find(pid);
        if (expiration_it != _log_state.expiration.end()) {
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
    // catching up with all previous end_tx operations (commit | abort)
    // to avoid writing the same commit | abort marker twice
    if (_mem_state.last_end_tx >= model::offset{0}) {
        if (!co_await wait_no_throw(
              _mem_state.last_end_tx,
              model::timeout_clock::now() + _sync_timeout)) {
            co_return tx_errc::timeout;
        }
    }

    if (!is_known_session(pid)) {
        co_return tx_errc::pid_not_found;
    }

    auto expiration_it = _log_state.expiration.find(pid);
    if (expiration_it != _log_state.expiration.end()) {
        if (!expiration_it->second.is_expired(clock_type::now())) {
            co_return tx_errc::stale;
        }
    }

    _mem_state.expected.erase(pid);
    auto is_txn_ga = is_transaction_ga();

    std::optional<model::tx_seq> tx_seq;
    if (!is_txn_ga) {
        auto prepare_it = _log_state.prepared.find(pid);
        if (prepare_it != _log_state.prepared.end()) {
            tx_seq = prepare_it->second.tx_seq;
        } else {
            prepare_it = _mem_state.preparing.find(pid);
            if (prepare_it != _mem_state.preparing.end()) {
                tx_seq = prepare_it->second.tx_seq;
            }
        }
    }

    if (!tx_seq) {
        tx_seq = get_tx_seq(pid);
    }

    if (is_txn_ga && !tx_seq) {
        vlog(
          _ctx_log.error,
          "Can not find tx_seq for pid({}) to expire old tx",
          pid);
    }

    if (tx_seq) {
        vlog(_ctx_log.trace, "trying to exprire pid:{} tx_seq:{}", pid, tx_seq);
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
        auto r = co_await _tx_gateway_frontend.local().try_abort(
          tm_partition, pid, *tx_seq, _sync_timeout);
        if (r.ec == tx_errc::none) {
            if (r.commited) {
                vlog(
                  _ctx_log.trace, "pid:{} tx_seq:{} is committed", pid, tx_seq);
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
                      _ctx_log.warn,
                      "Error \"{}\" on replicating pid:{} tx_seq:{} "
                      "autoabort/commit batch",
                      cr.error(),
                      pid,
                      tx_seq);
                    co_return tx_errc::unknown_server_error;
                }
                if (_mem_state.last_end_tx < cr.value().last_offset) {
                    _mem_state.last_end_tx = cr.value().last_offset;
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
                auto cr = co_await _c->replicate(
                  synced_term,
                  std::move(reader),
                  raft::replicate_options(raft::consistency_level::quorum_ack));
                if (!cr) {
                    vlog(
                      _ctx_log.warn,
                      "Error \"{}\" on replicating pid:{} tx_seq:{} "
                      "autoabort/abort "
                      "batch",
                      cr.error(),
                      pid,
                      tx_seq);
                    co_return tx_errc::unknown_server_error;
                }
                if (_mem_state.last_end_tx < cr.value().last_offset) {
                    _mem_state.last_end_tx = cr.value().last_offset;
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
        vlog(_ctx_log.trace, "expiring pid:{}", pid);
        auto batch = make_tx_control_batch(
          pid, model::control_record_type::tx_abort);
        auto reader = model::make_memory_record_batch_reader(std::move(batch));
        auto cr = co_await _c->replicate(
          _insync_term,
          std::move(reader),
          raft::replicate_options(raft::consistency_level::quorum_ack));
        if (!cr) {
            vlog(
              _ctx_log.warn,
              "Error \"{}\" on replicating pid:{} autoabort/abort batch",
              cr.error(),
              pid);
            co_return tx_errc::unknown_server_error;
        }
        if (_mem_state.last_end_tx < cr.value().last_offset) {
            _mem_state.last_end_tx = cr.value().last_offset;
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
    auto batch_data = read_fence_batch(std::move(b));

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

ss::future<> rm_stm::apply(model::record_batch b) {
    if (unlikely(!_bootstrap_committed_offset)) {
        _bootstrap_committed_offset = _c->committed_offset();
    }
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
        _log_state.prepared.erase(pid);
        _log_state.current_txes.erase(pid);
        auto offset_it = _log_state.ongoing_map.find(pid);
        if (offset_it != _log_state.ongoing_map.end()) {
            _log_state.ongoing_set.erase(offset_it->second.first);
            _log_state.ongoing_map.erase(pid);
        }

        _mem_state.forget(pid);
        _log_state.expiration.erase(pid);
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
            seq_it->second.entry.pid = bid.pid;
            seq_it->second.entry.seq = bid.last_seq;
            seq_it->second.entry.last_offset = translated;
        } else {
            seq_it->second.entry.update(bid.last_seq, translated);
        }

        if (!bid.is_transactional) {
            _log_state.unlink_lru_pid(seq_it->second);
            _log_state.lru_idempotent_pids.push_back(seq_it->second);
            spawn_background_clean_for_pids(
              rm_stm::clear_type::idempotent_pids);
        }

        seq_it->second.entry.last_write_timestamp = bid.first_timestamp.value();
        _oldest_session = std::min(_oldest_session, bid.first_timestamp);
    }

    if (bid.is_transactional) {
        if (_log_state.prepared.contains(bid.pid)) {
            vlog(
              _ctx_log.error,
              "Adding a record with pid:{} to a tx after it was prepared",
              bid.pid);
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
        spawn_background_clean_for_pids(rm_stm::clear_type::tx_pids);
    }
}

template<class T>
static void move_snapshot_wo_seqs(rm_stm::tx_snapshot& target, T& source) {
    target.fenced = std::move(source.fenced);
    target.ongoing = std::move(source.ongoing);
    target.prepared = std::move(source.prepared);
    target.aborted = std::move(source.aborted);
    target.abort_indexes = std::move(source.abort_indexes);
    target.offset = std::move(source.offset);
}

ss::future<>
rm_stm::apply_snapshot(stm_snapshot_header hdr, iobuf&& tx_ss_buf) {
    tx_snapshot data;
    iobuf_parser data_parser(std::move(tx_ss_buf));
    if (hdr.version == tx_snapshot::version) {
        data = reflection::adl<tx_snapshot>{}.from(data_parser);
    } else if (hdr.version == tx_snapshot_v3::version) {
        auto data_v3 = reflection::adl<tx_snapshot_v3>{}.from(data_parser);
        move_snapshot_wo_seqs(data, data_v3);
        data.seqs = std::move(data_v3.seqs);
        data.expiration = std::move(data_v3.expiration);
        for (auto& entry : data_v3.tx_seqs) {
            data.tx_data.push_back(tx_snapshot::tx_data_snapshot{
              .pid = entry.pid,
              .tx_seq = entry.tx_seq,
              .tm = model::legacy_tm_ntp.tp.partition});
        }

    } else if (hdr.version == tx_snapshot_v2::version) {
        auto data_v2 = reflection::adl<tx_snapshot_v2>{}.from(data_parser);
        move_snapshot_wo_seqs(data, data_v2);
        data.seqs = std::move(data_v2.seqs);

        for (auto& entry : data_v2.prepared) {
            data.tx_data.push_back(tx_snapshot::tx_data_snapshot{
              .pid = entry.pid,
              .tx_seq = entry.tx_seq,
              .tm = model::legacy_tm_ntp.tp.partition});
        }
    } else if (hdr.version == tx_snapshot_v1::version) {
        auto data_v1 = reflection::adl<tx_snapshot_v1>{}.from(data_parser);
        move_snapshot_wo_seqs(data, data_v1);
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

        for (auto& entry : data_v1.prepared) {
            data.tx_data.push_back(tx_snapshot::tx_data_snapshot{
              .pid = entry.pid,
              .tx_seq = entry.tx_seq,
              .tm = model::legacy_tm_ntp.tp.partition});
        }
    } else if (hdr.version == tx_snapshot_v0::version) {
        auto data_v0 = reflection::adl<tx_snapshot_v0>{}.from(data_parser);
        move_snapshot_wo_seqs(data, data_v0);
        for (auto seq_v0 : data_v0.seqs) {
            auto seq = seq_entry{
              .pid = seq_v0.pid,
              .seq = seq_v0.seq,
              .last_offset = kafka::offset{-1},
              .last_write_timestamp = seq_v0.last_write_timestamp};
            data.seqs.push_back(std::move(seq));
        }
        for (auto& entry : data_v0.prepared) {
            data.tx_data.push_back(tx_snapshot::tx_data_snapshot{
              .pid = entry.pid,
              .tx_seq = entry.tx_seq,
              .tm = model::legacy_tm_ntp.tp.partition});
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
    for (auto it = std::make_move_iterator(data.aborted.begin());
         it != std::make_move_iterator(data.aborted.end());
         it++) {
        _log_state.aborted.push_back(*it);
    }
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
    for (auto& entry : data.seqs) {
        const auto pid = entry.pid;
        auto it = _log_state.seq_table.find(pid);
        if (it == _log_state.seq_table.end()) {
            _log_state.seq_table.try_emplace(
              it, pid, seq_entry_wrapper{.entry = std::move(entry)});
        } else if (it->second.entry.seq < entry.seq) {
            it->second.entry = std::move(entry);
            it->second.term = model::term_id(-1);
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

    // We need to fill order for idempotent requests. So pid is from idempotent
    // request if it is not inside fence_pid_epoch. For order we just need to
    // check last_write_timestamp. It contains time last apply for log record.
    fragmented_vector<log_state::seq_map::iterator> sorted_pids;
    for (auto it = _log_state.seq_table.begin();
         it != _log_state.seq_table.end();
         ++it) {
        if (!_log_state.fence_pid_epoch.contains(
              it->second.entry.pid.get_id())) {
            sorted_pids.push_back(it);
        }
    }

    std::sort(sorted_pids.begin(), sorted_pids.end(), [](auto& lhs, auto& rhs) {
        return lhs->second.entry.last_write_timestamp
               < rhs->second.entry.last_write_timestamp;
    });

    for (auto it : sorted_pids) {
        _log_state.lru_idempotent_pids.push_back(it->second);
    }

    _last_snapshot_offset = data.offset;
    _insync_offset = data.offset;
}

uint8_t rm_stm::active_snapshot_version() {
    if (is_transaction_partitioning()) {
        return tx_snapshot::version;
    }

    if (is_transaction_ga()) {
        return tx_snapshot_v3::version;
    }

    if (_feature_table.local().is_active(
          features::feature::rm_stm_kafka_cache)) {
        return tx_snapshot_v2::version;
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
            co_await save_abort_snapshot(std::move(snapshot));
            snapshot = abort_snapshot{
              .first = model::offset::max(), .last = model::offset::min()};
        }
    }
    _log_state.aborted = std::move(snapshot.aborted);
}

// DO NOT coroutinize this method as it may cause issues on ARM:
// https://github.com/redpanda-data/redpanda/issues/6768
ss::future<stm_snapshot> rm_stm::take_snapshot() {
    auto start_offset = _raft->start_offset();

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

    return f.then([this]() mutable {
        iobuf tx_ss_buf;
        auto version = active_snapshot_version();
        if (version == tx_snapshot::version) {
            tx_snapshot tx_ss;
            fill_snapshot_wo_seqs(tx_ss);
            for (const auto& entry : _log_state.seq_table) {
                tx_ss.seqs.push_back(entry.second.entry.copy());
            }
            tx_ss.offset = _insync_offset;

            for (const auto& entry : _log_state.current_txes) {
                tx_ss.tx_data.push_back(tx_snapshot::tx_data_snapshot{
                  .pid = entry.first,
                  .tx_seq = entry.second.tx_seq,
                  .tm = entry.second.tm_partition});
            }

            for (const auto& entry : _log_state.expiration) {
                tx_ss.expiration.push_back(tx_snapshot::expiration_snapshot{
                  .pid = entry.first, .timeout = entry.second.timeout});
            }

            reflection::adl<tx_snapshot>{}.to(tx_ss_buf, std::move(tx_ss));
        } else if (version == tx_snapshot_v3::version) {
            tx_snapshot_v3 tx_ss;
            fill_snapshot_wo_seqs(tx_ss);
            for (const auto& entry : _log_state.seq_table) {
                tx_ss.seqs.push_back(entry.second.entry.copy());
            }
            tx_ss.offset = _insync_offset;

            for (const auto& entry : _log_state.current_txes) {
                tx_ss.tx_seqs.push_back(tx_snapshot_v3::tx_seqs_snapshot{
                  .pid = entry.first, .tx_seq = entry.second.tx_seq});
            }

            for (const auto& entry : _log_state.expiration) {
                tx_ss.expiration.push_back(tx_snapshot::expiration_snapshot{
                  .pid = entry.first, .timeout = entry.second.timeout});
            }

            reflection::adl<tx_snapshot_v3>{}.to(tx_ss_buf, std::move(tx_ss));
        } else if (version == tx_snapshot_v2::version) {
            tx_snapshot_v2 tx_ss;
            fill_snapshot_wo_seqs(tx_ss);
            for (const auto& entry : _log_state.seq_table) {
                tx_ss.seqs.push_back(entry.second.entry.copy());
            }
            tx_ss.offset = _insync_offset;
            reflection::adl<tx_snapshot_v2>{}.to(tx_ss_buf, std::move(tx_ss));
        } else if (version == tx_snapshot_v1::version) {
            tx_snapshot_v1 tx_ss;
            fill_snapshot_wo_seqs(tx_ss);
            for (const auto& it : _log_state.seq_table) {
                auto& entry = it.second.entry;
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
                          .seq = item.seq,
                          .offset = to_log_offset(item.offset)});
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
        return stm_snapshot::create(
          version, _insync_offset, std::move(tx_ss_buf));
    });
}

uint64_t rm_stm::get_snapshot_size() const {
    uint64_t abort_snapshots_size = 0;
    for (const auto& snapshot_size : _abort_snapshot_sizes) {
        abort_snapshots_size += snapshot_size.second;
    }
    return persisted_stm::get_snapshot_size() + abort_snapshots_size;
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

ss::future<> rm_stm::handle_eviction() {
    return _state_lock.hold_write_lock().then(
      [this]([[maybe_unused]] ss::basic_rwlock<>::holder unit) {
          _log_state.reset();
          _mem_state = mem_state{_tx_root_tracker};
          set_next(_c->start_offset());
          _insync_offset = model::prev_offset(_raft->start_offset());
          return ss::now();
      });
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

void rm_stm::spawn_background_clean_for_pids(rm_stm::clear_type type) {
    switch (type) {
    case rm_stm::clear_type::tx_pids: {
        if (
          _log_state.fence_pid_epoch.size() <= _max_concurrent_producer_ids()) {
            return;
        }
        break;
    }
    case rm_stm::clear_type::idempotent_pids: {
        if (
          _log_state.lru_idempotent_pids.size()
          <= _max_concurrent_producer_ids()) {
            return;
        }
        break;
    }
    }

    ssx::spawn_with_gate(_gate, [this, type] { return clear_old_pids(type); });
}

ss::future<> rm_stm::clear_old_pids(clear_type type) {
    auto try_lock = _clean_old_pids_mtx.try_get_units();
    if (!try_lock) {
        co_return;
    }

    auto read_lock = co_await _state_lock.hold_read_lock();

    switch (type) {
    case rm_stm::clear_type::tx_pids: {
        co_return co_await clear_old_tx_pids();
    }
    case rm_stm::clear_type::idempotent_pids: {
        co_return co_await clear_old_idempotent_pids();
    }
    }
}

ss::future<> rm_stm::clear_old_tx_pids() {
    if (_log_state.fence_pid_epoch.size() < _max_concurrent_producer_ids()) {
        co_return;
    }

    fragmented_vector<model::producer_identity> pids_for_delete;
    for (auto [id, epoch] : _log_state.fence_pid_epoch) {
        auto pid = model::producer_identity(id, epoch);
        // If pid is not inside tx_seqs it means we do not have transaction for
        // it right now
        if (!_log_state.current_txes.contains(pid)) {
            pids_for_delete.push_back(pid);
        }
    }

    vlog(
      _ctx_log.debug,
      "Found {} old transaction pids for delete",
      pids_for_delete.size());

    co_await ss::max_concurrent_for_each(
      pids_for_delete, 32, [this](auto pid) -> ss::future<> {
          // We have transaction for this pid
          if (_log_state.current_txes.contains(pid)) {
              return ss::now();
          }
          _mem_state.forget(pid);
          _log_state.forget(pid);
          return ss::now();
      });
}

ss::future<> rm_stm::clear_old_idempotent_pids() {
    if (
      _log_state.lru_idempotent_pids.size() < _max_concurrent_producer_ids()) {
        co_return;
    }

    vlog(
      _ctx_log.debug,
      "Found {} old idempotent pids for delete",
      _log_state.lru_idempotent_pids.size() - _max_concurrent_producer_ids());

    while (_log_state.lru_idempotent_pids.size()
           > _max_concurrent_producer_ids()) {
        auto pid_for_delete = _log_state.lru_idempotent_pids.front().entry.pid;
        auto rw_lock = get_idempotent_producer_lock(pid_for_delete);
        auto lock = rw_lock->try_write_lock();
        if (lock) {
            _log_state.lru_idempotent_pids.pop_front();
            _log_state.seq_table.erase(pid_for_delete);
            _inflight_requests.erase(pid_for_delete);
            _idempotent_producer_locks.erase(pid_for_delete);
            rw_lock->write_unlock();
        }

        co_await ss::maybe_yield();
    }
}

std::ostream&
operator<<(std::ostream& o, const rm_stm::inflight_requests& reqs) {
    fmt::print(
      o,
      "{{ tail: {}, term: {}, request cache size: {} }}",
      reqs.tail_seq,
      reqs.term,
      reqs.cache.size());
    return o;
}

std::ostream& operator<<(std::ostream& o, const rm_stm::mem_state& state) {
    fmt::print(
      o,
      "{{ estimated: {}, tx_start: {}, tx_starts: {}, expected: {}, preparing: "
      "{} }}",
      state.estimated.size(),
      state.tx_start.size(),
      state.tx_starts.size(),
      state.expected.size(),
      state.preparing.size());
    return o;
}

std::ostream& operator<<(std::ostream& o, const rm_stm::log_state& state) {
    fmt::print(
      o,
      "{{ fence_epochs: {}, ongoing_m: {}, ongoing_set: {}, prepared: {}, "
      "aborted: {}, abort_indexes: {}, seq_table: {}, tx_seqs: {}, expiration: "
      "{}}}",
      state.fence_pid_epoch.size(),
      state.ongoing_map.size(),
      state.ongoing_set.size(),
      state.prepared.size(),
      state.aborted.size(),
      state.abort_indexes.size(),
      state.seq_table.size(),
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
    auto aggregate_labels = config::shard_local_cfg().aggregate_metrics()
                              ? std::vector<sm::label>{sm::shard_label}
                              : std::vector<sm::label>{};

    const auto& ntp = _c->ntp();
    const std::vector<sm::label_instance> labels = {
      ns_label(ntp.ns()),
      topic_label(ntp.tp.topic()),
      partition_label(ntp.tp.partition()),
    };

    _metrics.add_group(
      prometheus_sanitize::metrics_name("tx:partition"),
      {
        sm::make_gauge(
          "idempotency_num_pids_inflight",
          [this] { return _inflight_requests.size(); },
          sm::description(
            "Number of pids with in flight idempotent produce requests."),
          labels)
          .aggregate(aggregate_labels),
        sm::make_gauge(
          "tx_num_inflight_requests",
          [this] { return _log_state.ongoing_map.size(); },
          sm::description("Number of ongoing transactional requests."),
          labels)
          .aggregate(aggregate_labels),
        sm::make_gauge(
          "tx_mem_tracker_consumption_bytes",
          [this] { return _tx_root_tracker.consumption(); },
          sm::description("Total memory bytes in use by tx subsystem."),
          labels)
          .aggregate(aggregate_labels),
      });
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
      "{} inflight_requests: {} }}",
      _mem_state,
      _log_state,
      _inflight_requests.size());
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
} // namespace cluster

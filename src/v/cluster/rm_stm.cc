// Copyright 2020 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cluster/rm_stm.h"

#include "cluster/logger.h"
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

static const model::violation_recovery_policy crash{
  model::violation_recovery_policy::crash};
static const model::violation_recovery_policy best_effort{
  model::violation_recovery_policy::best_effort};

static bool is_sequence(int32_t last_seq, int32_t next_seq) {
    return (last_seq + 1 == next_seq)
           || (next_seq == 0 && last_seq == std::numeric_limits<int32_t>::max());
}

static model::record_batch make_control_batch(
  model::producer_identity pid, model::control_record_type crt) {
    iobuf key;
    kafka::response_writer w(key);
    w.write(model::current_control_record_version());
    w.write(static_cast<int16_t>(crt));

    storage::record_batch_builder builder(
      raft::data_batch_type, model::offset(0));
    builder.set_producer_identity(pid.id, pid.epoch);
    builder.set_control_type();
    builder.add_raw_kw(
      std::move(key), std::nullopt, std::vector<model::record_header>());

    return std::move(builder).build();
}

static model::record_batch make_prepare_batch(rm_stm::prepare_marker record) {
    iobuf key;
    kafka::response_writer w(key);
    w.write(rm_stm::prepare_control_record_version());
    w.write(record.tm_partition());
    w.write(record.tx_seq());

    storage::record_batch_builder builder(
      tx_prepare_batch_type, model::offset(0));
    builder.set_producer_identity(record.pid.id, record.pid.epoch);
    builder.set_control_type();
    builder.add_raw_kw(
      std::move(key), std::nullopt, std::vector<model::record_header>());

    return std::move(builder).build();
}

static model::record_batch make_fence_batch(model::producer_identity pid) {
    iobuf key;
    kafka::response_writer w(key);
    w.write(rm_stm::fence_control_record_version());

    storage::record_batch_builder builder(
      tx_fence_batch_type, model::offset(0));
    builder.set_producer_identity(pid.id, pid.epoch);
    builder.set_control_type();
    builder.add_raw_kw(
      std::move(key), std::nullopt, std::vector<model::record_header>());

    return std::move(builder).build();
}

static rm_stm::prepare_marker parse_prepare_batch(model::record_batch& b) {
    const auto& hdr = b.header();

    vassert(
      hdr.type == tx_prepare_batch_type,
      "expect prepare batch type got {}",
      hdr.type);
    vassert(
      b.record_count() == 1, "prepare batch must contain a single record");

    auto bid = model::batch_identity::from(hdr);
    auto r = b.copy_records();
    auto& record = *r.begin();
    auto buf = record.release_key();
    kafka::request_reader buf_reader(std::move(buf));
    auto version = model::control_record_version(buf_reader.read_int16());
    vassert(
      version == rm_stm::prepare_control_record_version,
      "unknown prepare record version: {} expected: {}",
      version,
      rm_stm::prepare_control_record_version);
    return rm_stm::prepare_marker{
      .tm_partition = model::partition_id(buf_reader.read_int32()),
      .tx_seq = model::tx_seq(buf_reader.read_int64()),
      .pid = bid.pid,
    };
}

static model::control_record_type parse_control_batch(model::record_batch& b) {
    const auto& hdr = b.header();

    vassert(
      hdr.type == raft::data_batch_type,
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

static inline rm_stm::producer_id id(model::producer_identity pid) {
    return rm_stm::producer_id(pid.id);
}

static inline rm_stm::producer_epoch epoch(model::producer_identity pid) {
    return rm_stm::producer_epoch(pid.epoch);
}

rm_stm::rm_stm(ss::logger& logger, raft::consensus* c)
  : persisted_stm("rm", logger, c)
  , _oldest_session(model::timestamp::now())
  , _sync_timeout(config::shard_local_cfg().rm_sync_timeout_ms.value())
  , _recovery_policy(
      config::shard_local_cfg().rm_violation_recovery_policy.value())
  , _transactional_id_expiration(
      config::shard_local_cfg().transactional_id_expiration_ms.value()) {
    if (_recovery_policy != crash && _recovery_policy != best_effort) {
        vassert(false, "Unknown recovery policy: {}", _recovery_policy);
    }
}

ss::future<checked<model::term_id, tx_errc>>
rm_stm::begin_tx(model::producer_identity pid) {
    auto is_ready = co_await sync(_sync_timeout);
    if (!is_ready) {
        co_return tx_errc::stale;
    }
    if (_mem_state.term != _insync_term) {
        _mem_state = mem_state{.term = _insync_term};
    }

    // checking / setting pid fencing
    auto fence_it = _log_state.fence_pid_epoch.find(id(pid));
    if (
      fence_it == _log_state.fence_pid_epoch.end()
      || epoch(pid) > fence_it->second) {
        auto batch = make_fence_batch(pid);
        auto reader = model::make_memory_record_batch_reader(std::move(batch));
        auto r = co_await _c->replicate(
          _insync_term,
          std::move(reader),
          raft::replicate_options(raft::consistency_level::quorum_ack));
        if (!r) {
            vlog(
              clusterlog.error,
              "Error \"{}\" on replicating pid:{} fencing batch",
              r.error(),
              pid);
            co_return tx_errc::timeout;
        }
        if (!co_await wait_no_throw(
              model::offset(r.value().last_offset()), _sync_timeout)) {
            co_return tx_errc::timeout;
        }
        fence_it = _log_state.fence_pid_epoch.find(id(pid));
        if (fence_it == _log_state.fence_pid_epoch.end()) {
            vlog(
              clusterlog.error,
              "Unexpected state: can't find fencing token by id after "
              "replicating {}",
              pid);
            co_return tx_errc::timeout;
        }
    }
    if (epoch(pid) != fence_it->second) {
        vlog(
          clusterlog.error,
          "pid {} fenced out by epoch {}",
          pid,
          fence_it->second);
        co_return tx_errc::fenced;
    }

    auto [_, inserted] = _mem_state.expected.insert(pid);
    if (!inserted) {
        // tm_stm forgot that it had already begun a transaction
        // (it may happen when it crashes)
        // it's ok we fail this request, a client will abort a
        // tx bumping its producer id's epoch
        vlog(
          clusterlog.error,
          "there is already an ongoing transaction within {} session",
          pid);
        co_return tx_errc::timeout;
    }

    co_return _mem_state.term;
}

ss::future<tx_errc> rm_stm::prepare_tx(
  model::term_id etag,
  model::partition_id tm,
  model::producer_identity pid,
  model::tx_seq tx_seq,
  model::timeout_clock::duration timeout) {
    auto is_ready = co_await sync(timeout);
    if (!is_ready) {
        co_return tx_errc::stale;
    }
    if (_mem_state.term != _insync_term) {
        _mem_state = mem_state{.term = _insync_term};
    }

    if (_log_state.prepared.contains(pid)) {
        // a local tx was already prepared
        co_return tx_errc::none;
    }

    // checking fencing
    auto fence_it = _log_state.fence_pid_epoch.find(id(pid));
    if (fence_it != _log_state.fence_pid_epoch.end()) {
        if (epoch(pid) < fence_it->second) {
            vlog(
              clusterlog.error,
              "Can't prepare pid:{} - fenced out by epoch {}",
              pid,
              fence_it->second);
            co_return tx_errc::fenced;
        }
    }

    if (_mem_state.term != etag) {
        vlog(
          clusterlog.warn,
          "Can't prepare pid:{} - partition lost leadership current term: {} "
          "expected term: {}",
          pid,
          _mem_state.term,
          etag);
        // current partition changed leadership since a transaction started
        // there is a chance that not all writes were replicated
        // rejecting a tx to prevent data loss
        co_return tx_errc::timeout;
    }

    if (!_mem_state.expected.contains(pid)) {
        // impossible situation, a transaction coordinator tries
        // to prepare a transaction whic wasn't started
        vlog(clusterlog.error, "Can't prepare pid:{} - unknown session", pid);
        co_return tx_errc::timeout;
    }

    auto [_, inserted] = _mem_state.has_prepare_applied.try_emplace(pid, false);
    if (!inserted) {
        vlog(
          clusterlog.error,
          "Can't prepare pid:{} - concurrent operation on the same session",
          pid);
        co_return tx_errc::conflict;
    }

    auto batch = make_prepare_batch(
      prepare_marker{.tm_partition = tm, .tx_seq = tx_seq, .pid = pid});
    auto reader = model::make_memory_record_batch_reader(std::move(batch));
    auto r = co_await _c->replicate(
      etag,
      std::move(reader),
      raft::replicate_options(raft::consistency_level::quorum_ack));

    if (!r) {
        vlog(
          clusterlog.error,
          "Error \"{}\" on replicating pid:{} prepare batch",
          r.error(),
          pid);
        co_return tx_errc::timeout;
    }

    if (!co_await wait_no_throw(
          model::offset(r.value().last_offset()), timeout)) {
        co_return tx_errc::timeout;
    }

    auto has_applied_it = _mem_state.has_prepare_applied.find(pid);
    if (has_applied_it == _mem_state.has_prepare_applied.end()) {
        vlog(
          clusterlog.warn,
          "Can't prepare pid:{} - already aborted or it's an invariant "
          "violation",
          pid);
        co_return tx_errc::timeout;
    }
    auto applied = has_applied_it->second;
    _mem_state.has_prepare_applied.erase(pid);

    if (!applied) {
        vlog(
          clusterlog.warn,
          "Can't prepare pid:{} - already aborted or it's an invariant "
          "violation",
          pid);
        co_return tx_errc::conflict;
    }

    co_return tx_errc::none;
}

ss::future<tx_errc> rm_stm::commit_tx(
  model::producer_identity pid,
  model::tx_seq tx_seq,
  model::timeout_clock::duration timeout) {
    // doesn't make sense to fence off a commit because transaction
    // manager has already decided to commit and acked to a client

    auto is_ready = co_await sync(timeout);
    if (!is_ready) {
        co_return tx_errc::stale;
    }
    if (_mem_state.term != _insync_term) {
        _mem_state = mem_state{.term = _insync_term};
    }

    if (_mem_state.has_prepare_applied.contains(pid)) {
        // it looks like a violation since tm_stm commits only after
        // prepare succeeds but it's a legit rare situation:
        //   * tm_stm failed during prepare
        //   * its log update didn't pass
        //   * on recovery it recommits its previous tx
        //   * this commit (we're here) collides with "next" prepare
        vlog(
          clusterlog.error,
          "Can't commit pid:{} - concurrent prepare operation on the same "
          "session",
          pid);
        co_return tx_errc::conflict;
    }

    auto prepare_it = _log_state.prepared.find(pid);
    if (prepare_it == _log_state.prepared.end()) {
        vlog(
          clusterlog.trace,
          "Can't find prepare for pid:{} => replaying already comitted commit",
          pid);
        co_return tx_errc::none;
    }

    if (prepare_it->second.tx_seq > tx_seq) {
        // rare situation:
        //   * tm_stm prepares (tx_seq+1)
        //   * prepare on this rm passes but tm_stm failed to write to disk
        //   * during recovery tm_stm recommits (tx_seq)
        // existence of {pid, tx_seq+1} implies {pid, tx_seq} is committed
        vlog(
          clusterlog.trace,
          "prepare for pid:{} has higher tx_seq:{} than given: {} => replaying "
          "already comitted commit",
          pid,
          prepare_it->second.tx_seq,
          tx_seq);
        co_return tx_errc::none;
    }

    if (prepare_it->second.tx_seq < tx_seq) {
        if (_recovery_policy == best_effort) {
            vlog(
              clusterlog.error,
              "Rejecting commit with tx_seq:{} since prepare with lesser "
              "tx_seq:{} exists",
              tx_seq,
              prepare_it->second.tx_seq);
            co_return tx_errc::timeout;
        } else {
            vassert(
              false,
              "Received commit with tx_seq:{} while prepare with lesser "
              "tx_seq:{} exists",
              tx_seq,
              prepare_it->second.tx_seq);
        }
    }

    auto batch = make_control_batch(pid, model::control_record_type::tx_commit);
    auto reader = model::make_memory_record_batch_reader(std::move(batch));
    auto r = co_await _c->replicate(
      _insync_term,
      std::move(reader),
      raft::replicate_options(raft::consistency_level::quorum_ack));

    if (!r) {
        vlog(
          clusterlog.error,
          "Error \"{}\" on replicating pid:{} commit batch",
          r.error(),
          pid);
        co_return tx_errc::timeout;
    }
    if (!co_await wait_no_throw(
          model::offset(r.value().last_offset()), timeout)) {
        co_return tx_errc::timeout;
    }

    co_return tx_errc::none;
}

// abort_tx is invoked strictly after a tx is canceled on the tm_stm
// the purpose of abort is to put tx_range into the list of aborted txes
// and to fence off the old epoch
ss::future<tx_errc> rm_stm::abort_tx(
  model::producer_identity pid, model::timeout_clock::duration timeout) {
    // doesn't make sense to fence off an abort because transaction
    // manager has already decided to abort and acked to a client

    auto is_ready = co_await sync(timeout);
    if (!is_ready) {
        co_return tx_errc::timeout;
    }
    if (_mem_state.term != _insync_term) {
        _mem_state = mem_state{.term = _insync_term};
    }

    // preventing prepare and replicte once we
    // know we're going to abort tx and abandon pid
    _mem_state.expected.erase(pid);

    auto batch = make_control_batch(pid, model::control_record_type::tx_abort);
    auto reader = model::make_memory_record_batch_reader(std::move(batch));
    auto r = co_await _c->replicate(
      _insync_term,
      std::move(reader),
      raft::replicate_options(raft::consistency_level::quorum_ack));

    if (!r) {
        vlog(
          clusterlog.error,
          "Error \"{}\" on replicating pid:{} abort batch",
          r.error(),
          pid);
        co_return tx_errc::timeout;
    }

    // don't need to wait for apply because tx is already aborted on the
    // coordinator level - nothing can go wrong
    co_return tx_errc::none;
}

ss::future<checked<raft::replicate_result, kafka::error_code>>
rm_stm::replicate(
  model::batch_identity bid,
  model::record_batch_reader b,
  raft::replicate_options opts) {
    if (bid.is_transactional) {
        co_return co_await replicate_tx(bid, std::move(b));
    } else if (bid.has_idempotent() && bid.first_seq <= bid.last_seq) {
        co_return co_await replicate_seq(bid, std::move(b), opts);
    } else {
        auto r = co_await _c->replicate(std::move(b), std::move(opts));
        if (!r) {
            co_return kafka::error_code::unknown_server_error;
        }
        co_return r.value();
    }
}

bool rm_stm::check_seq(model::batch_identity bid) {
    auto pid_seq = _log_state.seq_table.find(bid.pid);
    auto last_write_timestamp = model::timestamp::now().value();
    if (pid_seq == _log_state.seq_table.end()) {
        if (bid.first_seq != 0) {
            return false;
        }
        seq_entry entry{
          .pid = bid.pid,
          .seq = bid.last_seq,
          .last_write_timestamp = last_write_timestamp};
        _oldest_session = std::min(
          _oldest_session, model::timestamp(entry.last_write_timestamp));
        _log_state.seq_table.emplace(entry.pid, entry);
    } else {
        if (!is_sequence(pid_seq->second.seq, bid.first_seq)) {
            return false;
        }
        pid_seq->second.seq = bid.last_seq;
        pid_seq->second.last_write_timestamp = last_write_timestamp;
        _oldest_session = std::min(
          _oldest_session,
          model::timestamp(pid_seq->second.last_write_timestamp));
    }
    return true;
}

ss::future<checked<raft::replicate_result, kafka::error_code>>
rm_stm::replicate_tx(model::batch_identity bid, model::record_batch_reader br) {
    auto is_ready = co_await sync(_sync_timeout);
    if (!is_ready) {
        co_return kafka::error_code::not_leader_for_partition;
    }
    if (_mem_state.term != _insync_term) {
        _mem_state = mem_state{.term = _insync_term};
    }

    // fencing
    auto fence_it = _log_state.fence_pid_epoch.find(id(bid.pid));
    if (fence_it != _log_state.fence_pid_epoch.end()) {
        if (epoch(bid.pid) < fence_it->second) {
            co_return kafka::error_code::invalid_producer_epoch;
        }
    }

    if (!_mem_state.expected.contains(bid.pid)) {
        // there is an inflight abort
        // or this partition lost leadership => can't continue tx since there is
        //  risk that ack=1 writes are lost
        // or it's a client bug and it keeps producing after commit / abort
        // or a replication of the first batch in a transaction has failed
        vlog(
          clusterlog.warn,
          "Partition doesn't expect record with pid:{}",
          bid.pid);
        co_return kafka::error_code::invalid_producer_epoch;
    }

    if (_mem_state.has_prepare_applied.contains(bid.pid)) {
        vlog(
          clusterlog.warn,
          "Client keeps producing after initiating a prepare for pid:{}",
          bid.pid);
        co_return kafka::error_code::unknown_server_error;
    }

    if (_mem_state.estimated.contains(bid.pid)) {
        // we recieved second produce request while the first is still
        // being processed. this is highly unlikely situation because
        // we replicate with ack=1 and it should be fast
        vlog(clusterlog.warn, "Too frequent produce with same pid:{}", bid.pid);
        co_return kafka::error_code::unknown_server_error;
    }

    if (!check_seq(bid)) {
        co_return kafka::error_code::out_of_order_sequence_number;
    }

    if (!_mem_state.tx_start.contains(bid.pid)) {
        _mem_state.estimated.emplace(bid.pid, _insync_offset);
    }

    // after the replicate continuation _mem_state may change so caching term
    // to invalidate the post processing
    auto term = _mem_state.term;

    auto r = co_await _c->replicate(
      _mem_state.term,
      std::move(br),
      raft::replicate_options(raft::consistency_level::leader_ack));
    if (!r) {
        if (_mem_state.estimated.contains(bid.pid)) {
            // an error during replication, preventin tx from progress
            _mem_state.expected.erase(bid.pid);
        }
        co_return kafka::error_code::unknown_server_error;
    }

    auto replicated = r.value();

    if (_mem_state.term != term) {
        // mem state already changed no need to clean it
        co_return replicated;
    }

    auto last_offset = model::offset(replicated.last_offset());
    if (!_mem_state.tx_start.contains(bid.pid)) {
        auto base_offset = model::offset(
          last_offset() - (bid.record_count - 1));
        _mem_state.tx_start.emplace(bid.pid, base_offset);
        _mem_state.tx_starts.insert(base_offset);
        _mem_state.estimated.erase(bid.pid);
    }
    co_return replicated;
}

ss::future<checked<raft::replicate_result, kafka::error_code>>
rm_stm::replicate_seq(
  model::batch_identity bid,
  model::record_batch_reader br,
  raft::replicate_options opts) {
    auto is_ready = co_await sync(_sync_timeout);
    if (!is_ready) {
        co_return kafka::error_code::not_leader_for_partition;
    }
    if (_mem_state.term != _insync_term) {
        _mem_state = mem_state{.term = _insync_term};
    }
    if (!check_seq(bid)) {
        co_return kafka::error_code::out_of_order_sequence_number;
    }
    auto r = co_await _c->replicate(_insync_term, std::move(br), opts);
    if (!r) {
        co_return kafka::error_code::unknown_server_error;
    }
    co_return r.value();
}

model::offset rm_stm::last_stable_offset() {
    auto first_tx_start = model::offset::max();

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

    auto last_visible_index = _c->last_visible_index();
    if (first_tx_start <= last_visible_index) {
        return first_tx_start;
    }

    return raft::details::next_offset(last_visible_index);
}

ss::future<std::vector<rm_stm::tx_range>>
rm_stm::aborted_transactions(model::offset from, model::offset to) {
    std::vector<rm_stm::tx_range> result;
    for (auto& range : _log_state.aborted) {
        if (range.second.last < from) {
            continue;
        }
        if (range.second.first > to) {
            continue;
        }
        result.push_back(range.second);
    }
    co_return result;
}

void rm_stm::compact_snapshot() {
    auto cutoff_timestamp = model::timestamp::now().value()
                            - _transactional_id_expiration.count();
    if (cutoff_timestamp <= _oldest_session.value()) {
        return;
    }
    auto next_oldest_session = model::timestamp::now();
    for (auto it = _log_state.seq_table.cbegin();
         it != _log_state.seq_table.cend();) {
        if (it->second.last_write_timestamp < cutoff_timestamp) {
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

ss::future<> rm_stm::apply(model::record_batch b) {
    auto last_offset = b.last_offset();

    const auto& hdr = b.header();
    auto bid = model::batch_identity::from(hdr);

    if (hdr.type == tx_fence_batch_type) {
        auto [fence_it, _] = _log_state.fence_pid_epoch.try_emplace(
          id(bid.pid), epoch(bid.pid));
        if (fence_it->second < epoch(bid.pid)) {
            fence_it->second = epoch(bid.pid);
        }
    } else if (hdr.type == tx_prepare_batch_type) {
        apply_prepare(parse_prepare_batch(b));
    } else if (hdr.type == raft::data_batch_type) {
        if (hdr.attrs.is_control()) {
            apply_control(bid.pid, parse_control_batch(b));
        } else {
            apply_data(bid, last_offset);
        }
    }

    compact_snapshot();
    _insync_offset = last_offset;
    return ss::now();
}

void rm_stm::apply_prepare(rm_stm::prepare_marker prepare) {
    auto pid = prepare.pid;

    auto fence_it = _log_state.fence_pid_epoch.find(id(pid));
    if (fence_it == _log_state.fence_pid_epoch.end()) {
        // impossible situation, tm_stm invokes prepare strictly
        // after begin which adds a fence (if it's necessary)

        vlog(
          clusterlog.error,
          "Can find an expected fence for an observed prepare with pid:{}",
          pid);
        if (_recovery_policy == best_effort) {
            _log_state.fence_pid_epoch.emplace(id(pid), epoch(pid));
        } else {
            vassert(
              false,
              "Can find an expected fence for an observed prepare with pid:{}",
              pid);
        }
    } else if (fence_it->second < epoch(pid)) {
        // impossible situation, tm_stm invokes prepare strictly
        // after begin which adds a fence (if it's necessary)
        // so fence should be at least as prepare's pid

        vlog(
          clusterlog.error,
          "Set fence {} is lesser that prepare's epoch: {}",
          fence_it->second,
          pid);
        if (_recovery_policy == best_effort) {
            fence_it->second = epoch(pid);
        } else {
            vassert(
              false,
              "Set fence {} is lesser that prepare's epoch: {}",
              fence_it->second,
              pid);
        }
    } else if (fence_it->second > epoch(pid)) {
        // during a data race between replicating
        //  - a prepare record and
        //  - a record wth higher epoch for the same pid.id e.g abort
        // the latter request won and prepare got fenced off

        // cleaning the state since this pid is fenced off
        vlog(
          clusterlog.warn,
          "Prepare with pid:{} is fenced off by {}",
          pid,
          fence_it->second);
        _mem_state.expected.erase(pid);
        return;
    }

    if (_log_state.aborted.contains(pid)) {
        // during a data race between replicating a prepare record
        // and an abort record, the abort record won
        vlog(clusterlog.warn, "Can't prepare an aborted tx (pid:{})", pid);
        _mem_state.expected.erase(pid);
        return;
    }

    _log_state.prepared.try_emplace(pid, prepare);
    _mem_state.expected.erase(pid);

    auto has_applied_it = _mem_state.has_prepare_applied.find(pid);
    if (has_applied_it != _mem_state.has_prepare_applied.end()) {
        // _mem_state.has_prepare_applied may lack pid if
        // this is processing og the historic prepare
        has_applied_it->second = true;
    }
}

void rm_stm::apply_control(
  model::producer_identity pid, model::control_record_type crt) {
    auto fence_it = _log_state.fence_pid_epoch.find(id(pid));
    if (fence_it == _log_state.fence_pid_epoch.end()) {
        _log_state.fence_pid_epoch.emplace(id(pid), epoch(pid));
    } else if (fence_it->second < epoch(pid)) {
        fence_it->second = epoch(pid);
    }

    // either epoch is the same as fencing or it's lesser in the latter
    // case we don't fence off aborts and commits because transactional
    // manager already decided a tx's outcome and acked it to the client

    if (crt == model::control_record_type::tx_abort) {
        if (_log_state.aborted.contains(pid)) {
            // already aborted; but it's fine - a transaction
            // coordinator may re(abort) a transaction on recovery
            // or during concurrent init_tx
            return;
        }

        _log_state.prepared.erase(pid);
        auto offset_it = _log_state.ongoing_map.find(pid);
        if (offset_it != _log_state.ongoing_map.end()) {
            _log_state.aborted.emplace(pid, offset_it->second);
            _log_state.ongoing_set.erase(offset_it->second.first);
            _log_state.ongoing_map.erase(pid);
        }

        _mem_state.forget(pid);
    } else if (crt == model::control_record_type::tx_commit) {
        if (_log_state.aborted.contains(pid)) {
            vlog(
              clusterlog.error,
              "Commit request with pid:{} came after tx is already aborted",
              pid);
            if (_recovery_policy != best_effort) {
                vassert(
                  false,
                  "Commit request with pid:{} came after tx is already aborted",
                  pid);
            }
        }

        if (!_log_state.prepared.contains(pid)) {
            // trying to commit a tx which wasn't prepare

            if (_log_state.ongoing_map.contains(pid)) {
                vlog(
                  clusterlog.error,
                  "Committing an ongoing tx with pid:{} which isn't in a "
                  "prepared state",
                  pid);

                if (_recovery_policy != best_effort) {
                    vassert(
                      false,
                      "Committing an ongoing tx with pid:{} which isn't in a "
                      "prepared state",
                      pid);
                }
            } else {
                // it's a (re)commit of an already committed tx during
                // tm_stm recovery or caused by a concurrent init_tx
                vlog(
                  clusterlog.warn,
                  "Committing a tx with pid:{} which isn't in a prepared state",
                  pid);
            }
        }

        _log_state.prepared.erase(pid);
        auto offset_it = _log_state.ongoing_map.find(pid);
        if (offset_it != _log_state.ongoing_map.end()) {
            _log_state.ongoing_set.erase(offset_it->second.first);
            _log_state.ongoing_map.erase(pid);
        }

        _mem_state.forget(pid);
    }
}

void rm_stm::apply_data(model::batch_identity bid, model::offset last_offset) {
    if (bid.has_idempotent()) {
        auto pid_seq = _log_state.seq_table.find(bid.pid);
        if (pid_seq == _log_state.seq_table.end()) {
            seq_entry entry{
              .pid = bid.pid,
              .seq = bid.last_seq,
              .last_write_timestamp = bid.max_timestamp.value()};
            _log_state.seq_table.emplace(bid.pid, entry);
            _oldest_session = std::min(
              _oldest_session, model::timestamp(entry.last_write_timestamp));
        } else if (pid_seq->second.seq < bid.last_seq) {
            pid_seq->second.seq = bid.last_seq;
            pid_seq->second.last_write_timestamp = bid.max_timestamp.value();
            _oldest_session = std::min(_oldest_session, bid.max_timestamp);
        }
    }

    if (bid.is_transactional) {
        if (_log_state.aborted.contains(bid.pid)) {
            vlog(
              clusterlog.error,
              "Adding a record with pid:{} to a tx after it was aborted",
              bid.pid);
            if (_recovery_policy != best_effort) {
                vassert(
                  false,
                  "Adding a record with pid:{} to a tx after it was aborted",
                  bid.pid);
            }
            return;
        }

        if (_log_state.prepared.contains(bid.pid)) {
            vlog(
              clusterlog.error,
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

void rm_stm::load_snapshot(stm_snapshot_header hdr, iobuf&& tx_ss_buf) {
    vassert(
      hdr.version == tx_snapshot_version,
      "unsupported seq_snapshot_header version {}",
      hdr.version);
    iobuf_parser data_parser(std::move(tx_ss_buf));
    auto data = reflection::adl<tx_snapshot>{}.from(data_parser);

    for (auto& entry : data.fenced) {
        _log_state.fence_pid_epoch.emplace(id(entry), epoch(entry));
    }
    for (auto& entry : data.ongoing) {
        _log_state.ongoing_map.emplace(entry.pid, entry);
        _log_state.ongoing_set.insert(entry.first);
    }
    for (auto& entry : data.prepared) {
        _log_state.prepared.emplace(entry.pid, entry);
    }
    for (auto& entry : data.aborted) {
        _log_state.aborted.emplace(entry.pid, entry);
    }
    for (auto& entry : data.seqs) {
        auto [seq_it, _] = _log_state.seq_table.try_emplace(entry.pid, entry);
        if (seq_it->second.seq < entry.seq) {
            seq_it->second = entry;
        }
    }

    _last_snapshot_offset = data.offset;
    _insync_offset = data.offset;
}

stm_snapshot rm_stm::take_snapshot() {
    tx_snapshot tx_ss;

    for (auto const& [k, v] : _log_state.fence_pid_epoch) {
        tx_ss.fenced.push_back(
          model::producer_identity{.id = k(), .epoch = v()});
    }
    for (auto& entry : _log_state.ongoing_map) {
        tx_ss.ongoing.push_back(entry.second);
    }
    for (auto& entry : _log_state.prepared) {
        tx_ss.prepared.push_back(entry.second);
    }
    for (auto& entry : _log_state.aborted) {
        tx_ss.aborted.push_back(entry.second);
    }
    for (auto& entry : _log_state.seq_table) {
        tx_ss.seqs.push_back(entry.second);
    }
    tx_ss.offset = _insync_offset;

    iobuf tx_ss_buf;
    reflection::adl<tx_snapshot>{}.to(tx_ss_buf, tx_ss);

    stm_snapshot_header header;
    header.version = tx_snapshot_version;
    header.snapshot_size = tx_ss_buf.size_bytes();

    stm_snapshot stx_ss;
    stx_ss.header = header;
    stx_ss.offset = _insync_offset;
    stx_ss.data = std::move(tx_ss_buf);
    return stx_ss;
}

} // namespace cluster

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

static const model::violation_recovery_policy crash{
  model::violation_recovery_policy::crash};
static const model::violation_recovery_policy best_effort{
  model::violation_recovery_policy::best_effort};

static bool is_sequence(int32_t last_seq, int32_t next_seq) {
    return (last_seq + 1 == next_seq)
           || (next_seq == 0 && last_seq == std::numeric_limits<int32_t>::max());
}

static model::record_batch
make_fence_batch(int8_t version, model::producer_identity pid) {
    iobuf key;
    auto pid_id = pid.id;
    reflection::serialize(key, model::record_batch_type::tx_fence, pid_id);

    iobuf value;
    reflection::serialize(value, version);

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
    kafka::response_writer w(key);
    w.write(model::current_control_record_version());
    w.write(static_cast<int16_t>(crt));

    storage::record_batch_builder builder(
      model::record_batch_type::raft_data, model::offset(0));
    builder.set_producer_identity(pid.id, pid.epoch);
    builder.set_control_type();
    builder.set_transactional_type();
    builder.add_raw_kw(
      std::move(key), std::nullopt, std::vector<model::record_header>());

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

static model::control_record_type parse_control_batch(model::record_batch& b) {
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

rm_stm::rm_stm(
  ss::logger& logger,
  raft::consensus* c,
  ss::sharded<cluster::tx_gateway_frontend>& tx_gateway_frontend)
  : persisted_stm("rm", logger, c)
  , _oldest_session(model::timestamp::now())
  , _sync_timeout(config::shard_local_cfg().rm_sync_timeout_ms.value())
  , _tx_timeout_delay(config::shard_local_cfg().tx_timeout_delay_ms.value())
  , _recovery_policy(
      config::shard_local_cfg().rm_violation_recovery_policy.value())
  , _transactional_id_expiration(
      config::shard_local_cfg().transactional_id_expiration_ms.value())
  , _tx_gateway_frontend(tx_gateway_frontend) {
    if (_recovery_policy != crash && _recovery_policy != best_effort) {
        vassert(false, "Unknown recovery policy: {}", _recovery_policy);
    }
    auto_abort_timer.set_callback([this] { abort_old_txes(); });
}

ss::future<checked<model::term_id, tx_errc>> rm_stm::begin_tx(
  model::producer_identity pid,
  model::tx_seq tx_seq,
  std::chrono::milliseconds transaction_timeout_ms) {
    return get_tx_lock(pid.get_id())
      ->with([this, pid, tx_seq, transaction_timeout_ms]() {
          return do_begin_tx(pid, tx_seq, transaction_timeout_ms);
      });
}

ss::future<checked<model::term_id, tx_errc>> rm_stm::do_begin_tx(
  model::producer_identity pid,
  model::tx_seq tx_seq,
  std::chrono::milliseconds transaction_timeout_ms) {
    if (!co_await sync(_sync_timeout)) {
        co_return tx_errc::stale;
    }

    // checking / setting pid fencing
    auto fence_it = _log_state.fence_pid_epoch.find(pid.get_id());
    if (
      fence_it == _log_state.fence_pid_epoch.end()
      || pid.get_epoch() > fence_it->second) {
        auto batch = make_fence_batch(
          rm_stm::fence_control_record_version, pid);
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
            co_return tx_errc::unknown_server_error;
        }
        if (!co_await wait_no_throw(
              model::offset(r.value().last_offset()), _sync_timeout)) {
            co_return tx_errc::unknown_server_error;
        }
    } else if (pid.get_epoch() < fence_it->second) {
        vlog(
          clusterlog.error,
          "pid {} fenced out by epoch {}",
          pid,
          fence_it->second);
        co_return tx_errc::fenced;
    }

    auto [_, inserted] = _mem_state.expected.emplace(pid, tx_seq);
    if (!inserted) {
        // TODO: https://app.clubhouse.io/vectorized/story/2194
        // tm_stm forgot that it had already begun a transaction
        // (it may happen when it crashes)
        // it's ok we fail this request, a client will abort a
        // tx bumping its producer id's epoch
        vlog(
          clusterlog.error,
          "there is already an ongoing transaction within {} session",
          pid);
        co_return tx_errc::unknown_server_error;
    }

    track_tx(pid, transaction_timeout_ms);

    co_return _mem_state.term;
}

ss::future<tx_errc> rm_stm::prepare_tx(
  model::term_id etag,
  model::partition_id tm,
  model::producer_identity pid,
  model::tx_seq tx_seq,
  model::timeout_clock::duration timeout) {
    return get_tx_lock(pid.get_id())
      ->with([this, etag, tm, pid, tx_seq, timeout]() {
          return do_prepare_tx(etag, tm, pid, tx_seq, timeout);
      });
}

ss::future<tx_errc> rm_stm::do_prepare_tx(
  model::term_id etag,
  model::partition_id tm,
  model::producer_identity pid,
  model::tx_seq tx_seq,
  model::timeout_clock::duration timeout) {
    if (!co_await sync(timeout)) {
        co_return tx_errc::stale;
    }

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
          clusterlog.error,
          "Can't prepare pid:{} - fenced out by epoch {}",
          pid,
          fence_it->second);
        co_return tx_errc::fenced;
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
        co_return tx_errc::request_rejected;
    }

    auto expected_it = _mem_state.expected.find(pid);
    if (expected_it == _mem_state.expected.end()) {
        // impossible situation, a transaction coordinator tries
        // to prepare a transaction which wasn't started
        vlog(clusterlog.error, "Can't prepare pid:{} - unknown session", pid);
        co_return tx_errc::request_rejected;
    }

    if (expected_it->second != tx_seq) {
        // current prepare_tx call is stale, rejecting
        co_return tx_errc::request_rejected;
    }

    auto marker = prepare_marker{
      .tm_partition = tm, .tx_seq = tx_seq, .pid = pid};
    auto [_, inserted] = _mem_state.preparing.try_emplace(pid, marker);
    if (!inserted) {
        vlog(
          clusterlog.error,
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
          clusterlog.error,
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
    return get_tx_lock(pid.get_id())->with([this, pid, tx_seq, timeout]() {
        return do_commit_tx(pid, tx_seq, timeout);
    });
}

ss::future<tx_errc> rm_stm::do_commit_tx(
  model::producer_identity pid,
  model::tx_seq tx_seq,
  model::timeout_clock::duration timeout) {
    // doesn't make sense to fence off a commit because transaction
    // manager has already decided to commit and acked to a client
    if (!co_await sync(timeout)) {
        co_return tx_errc::stale;
    }

    auto preparing_it = _mem_state.preparing.find(pid);
    auto prepare_it = _log_state.prepared.find(pid);

    if (preparing_it != _mem_state.preparing.end()) {
        if (preparing_it->second.tx_seq > tx_seq) {
            // - tm_stm & rm_stm failed during prepare
            // - during recovery tm_stm recommits its previous tx
            // - that commit (we're here) collides with "next" failed prepare
            // it may happen only if the commit passed => acking
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
            vlog(clusterlog.error, "{}", msg);
            co_return tx_errc::request_rejected;
        }
        vassert(false, "{}", msg);
    }

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
    } else if (prepare_it->second.tx_seq < tx_seq) {
        std::string msg = fmt::format(
          "Rejecting commit with tx_seq:{} since prepare with lesser tx_seq:{} "
          "exists",
          tx_seq,
          prepare_it->second.tx_seq);
        if (_recovery_policy == best_effort) {
            vlog(clusterlog.error, "{}", msg);
            co_return tx_errc::request_rejected;
        }
        vassert(false, "{}", msg);
    }

    // we commit only if a provided tx_seq matches prepared tx_seq
    auto batch = make_tx_control_batch(
      pid, model::control_record_type::tx_commit);
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
        co_return tx_errc::unknown_server_error;
    }
    if (!co_await wait_no_throw(
          model::offset(r.value().last_offset()), timeout)) {
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
    return get_tx_lock(pid.get_id())->with([this, pid, tx_seq, timeout]() {
        return do_abort_tx(pid, tx_seq, timeout);
    });
}

// abort_tx is invoked strictly after a tx is canceled on the tm_stm
// the purpose of abort is to put tx_range into the list of aborted txes
// and to fence off the old epoch
ss::future<tx_errc> rm_stm::do_abort_tx(
  model::producer_identity pid,
  model::tx_seq tx_seq,
  model::timeout_clock::duration timeout) {
    // doesn't make sense to fence off an abort because transaction
    // manager has already decided to abort and acked to a client

    if (!co_await sync(timeout)) {
        co_return tx_errc::stale;
    }

    if (!is_known_session(pid)) {
        co_return tx_errc::none;
    }

    auto origin = get_abort_origin(pid, tx_seq);
    if (origin == abort_origin::past) {
        // rejecting a delayed abort command to prevent aborting
        // a wrong transaction
        co_return tx_errc::request_rejected;
    }
    if (origin == abort_origin::future) {
        // impossible situation: before transactional coordinator may issue
        // abort of the current transaction it should begin it and abort all
        // previous transactions with the same pid
        vlog(
          clusterlog.error,
          "Rejecting abort (pid:{}, tx_seq: {}) because it isn't consistent "
          "with the current ongoing transaction",
          pid,
          tx_seq);
        co_return tx_errc::request_rejected;
    }

    // preventing prepare and replicte once we
    // know we're going to abort tx and abandon pid
    _mem_state.expected.erase(pid);

    auto batch = make_tx_control_batch(
      pid, model::control_record_type::tx_abort);
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
        co_return tx_errc::unknown_server_error;
    }

    // don't need to wait for apply because tx is already aborted on the
    // coordinator level - nothing can go wrong
    co_return tx_errc::none;
}

ss::future<result<raft::replicate_result>> rm_stm::replicate(
  model::batch_identity bid,
  model::record_batch_reader b,
  raft::replicate_options opts) {
    if (bid.is_transactional) {
        auto pid = bid.pid.get_id();
        return get_tx_lock(pid)->with([this, bid, b = std::move(b)]() mutable {
            return replicate_tx(bid, std::move(b));
        });
    } else if (bid.has_idempotent() && bid.first_seq <= bid.last_seq) {
        return replicate_seq(bid, std::move(b), opts);
    }

    return _c->replicate(std::move(b), opts);
}

ss::future<> rm_stm::stop() {
    auto_abort_timer.cancel();
    return raft::state_machine::stop();
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

ss::future<result<raft::replicate_result>>
rm_stm::replicate_tx(model::batch_identity bid, model::record_batch_reader br) {
    if (!co_await sync(_sync_timeout)) {
        co_return errc::not_leader;
    }

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
          clusterlog.warn,
          "Partition doesn't expect record with pid:{}",
          bid.pid);
        co_return errc::invalid_producer_epoch;
    }

    if (_mem_state.preparing.contains(bid.pid)) {
        vlog(
          clusterlog.warn,
          "Client keeps producing after initiating a prepare for pid:{}",
          bid.pid);
        co_return errc::generic_tx_error;
    }

    if (_mem_state.estimated.contains(bid.pid)) {
        // we recieved second produce request while the first is still
        // being processed. this is highly unlikely situation because
        // we replicate with ack=1 and it should be fast
        vlog(clusterlog.warn, "Too frequent produce with same pid:{}", bid.pid);
        co_return errc::generic_tx_error;
    }

    if (!check_seq(bid)) {
        co_return errc::sequence_out_of_order;
    }

    if (!_mem_state.tx_start.contains(bid.pid)) {
        _mem_state.estimated.emplace(bid.pid, _insync_offset);
    }

    auto r = co_await _c->replicate(
      _mem_state.term,
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

    auto replicated = r.value();

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

ss::future<result<raft::replicate_result>> rm_stm::replicate_seq(
  model::batch_identity bid,
  model::record_batch_reader br,
  raft::replicate_options opts) {
    if (!co_await sync(_sync_timeout)) {
        co_return errc::not_leader;
    }
    if (!check_seq(bid)) {
        co_return errc::sequence_out_of_order;
    }
    co_return co_await _c->replicate(_insync_term, std::move(br), opts);
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
        if (range.last < from) {
            continue;
        }
        if (range.first > to) {
            continue;
        }
        result.push_back(range);
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

ss::future<bool> rm_stm::sync(model::timeout_clock::duration timeout) {
    auto ready = co_await persisted_stm::sync(timeout);
    if (ready) {
        if (_mem_state.term != _insync_term) {
            _mem_state = mem_state{.term = _insync_term};
        }
    }
    if (ready && !_is_leader) {
        _is_leader = true;
        became_leader();
    } else if (!ready && _is_leader) {
        _is_leader = false;
        lost_leadership();
    }
    co_return ready;
}

void rm_stm::became_leader() {
    if (_gate.is_closed()) {
        return;
    }
    if (!_is_autoabort_enabled) {
        return;
    }
    if (!auto_abort_timer.armed()) {
        abort_old_txes();
    }
}

void rm_stm::lost_leadership() { auto_abort_timer.cancel(); }

void rm_stm::track_tx(
  model::producer_identity pid,
  std::chrono::milliseconds transaction_timeout_ms) {
    if (_gate.is_closed()) {
        return;
    }
    _mem_state.expiration[pid] = expiration_info{
      .timeout = transaction_timeout_ms, .last_update = clock_type::now()};
    if (!_is_autoabort_enabled) {
        return;
    }
    auto deadline = _mem_state.expiration[pid].deadline();
    if (auto_abort_timer.armed() && auto_abort_timer.get_timeout() > deadline) {
        auto_abort_timer.cancel();
        auto_abort_timer.arm(deadline);
    } else if (!auto_abort_timer.armed()) {
        auto_abort_timer.arm(deadline);
    }
}

void rm_stm::abort_old_txes() {
    (void)ss::with_gate(_gate, [this] {
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
                if (expiration_it->second.deadline() > clock_type::now()) {
                    continue;
                }
            }
            expired.insert(pid);
        }
        return abort_old_txes(std::move(expired));
    });
}

ss::future<>
rm_stm::abort_old_txes(absl::btree_set<model::producer_identity> pids) {
    for (auto pid : pids) {
        co_await try_abort_old_tx(pid);
    }

    std::optional<time_point_type> earliest_deadline;
    for (auto& [_, expiration] : _mem_state.expiration) {
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
        if (
          auto_abort_timer.armed()
          && auto_abort_timer.get_timeout() > deadline) {
            auto_abort_timer.cancel();
            auto_abort_timer.arm(deadline);
        } else if (!auto_abort_timer.armed()) {
            auto_abort_timer.arm(deadline);
        }
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

    if (!is_known_session(pid)) {
        co_return;
    }

    auto expiration_it = _mem_state.expiration.find(pid);
    if (expiration_it != _mem_state.expiration.end()) {
        if (expiration_it->second.deadline() > clock_type::now()) {
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
                co_await _c
                  ->replicate(
                    _insync_term,
                    std::move(reader),
                    raft::replicate_options(
                      raft::consistency_level::quorum_ack))
                  .discard_result();
            } else if (r.aborted) {
                auto batch = make_tx_control_batch(
                  pid, model::control_record_type::tx_abort);
                auto reader = model::make_memory_record_batch_reader(
                  std::move(batch));
                co_await _c
                  ->replicate(
                    _insync_term,
                    std::move(reader),
                    raft::replicate_options(
                      raft::consistency_level::quorum_ack))
                  .discard_result();
            }
        }
    } else {
        auto batch = make_tx_control_batch(
          pid, model::control_record_type::tx_abort);
        auto reader = model::make_memory_record_batch_reader(std::move(batch));
        co_await _c
          ->replicate(
            _insync_term,
            std::move(reader),
            raft::replicate_options(raft::consistency_level::quorum_ack))
          .discard_result();
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
      version == rm_stm::fence_control_record_version,
      "unknown fence record version: {} expected: {}",
      version,
      rm_stm::fence_control_record_version);

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
    if (fence_it->second < bid.pid.get_epoch()) {
        fence_it->second = bid.pid.get_epoch();
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
    _log_state.prepared.try_emplace(pid, prepare);
    _mem_state.expected.erase(pid);
    _mem_state.preparing.erase(pid);
}

void rm_stm::apply_control(
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
    } else if (crt == model::control_record_type::tx_commit) {
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

ss::future<> rm_stm::load_snapshot(stm_snapshot_header hdr, iobuf&& tx_ss_buf) {
    vassert(
      hdr.version == tx_snapshot_version,
      "unsupported seq_snapshot_header version {}",
      hdr.version);
    iobuf_parser data_parser(std::move(tx_ss_buf));
    auto data = reflection::adl<tx_snapshot>{}.from(data_parser);

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
    for (auto& entry : data.seqs) {
        auto [seq_it, _] = _log_state.seq_table.try_emplace(entry.pid, entry);
        if (seq_it->second.seq < entry.seq) {
            seq_it->second = entry;
        }
    }

    _last_snapshot_offset = data.offset;
    _insync_offset = data.offset;

    return ss::now();
}

ss::future<stm_snapshot> rm_stm::take_snapshot() {
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
        tx_ss.aborted.push_back(entry);
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
    co_return stx_ss;
}

} // namespace cluster

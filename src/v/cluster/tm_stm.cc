// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cluster/tm_stm.h"

#include "base/units.h"
#include "cluster/tm_stm_cache_manager.h"
#include "cluster/types.h"
#include "model/record.h"
#include "raft/errc.h"
#include "raft/fundamental.h"
#include "storage/record_batch_builder.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/future.hh>
#include <seastar/util/bool_class.hh>

#include <cstdint>
#include <filesystem>
#include <optional>

namespace cluster {

namespace {

tm_transaction_v1::tx_status
downgrade_status(tm_transaction::tx_status status) {
    switch (status) {
    case tm_transaction::tx_status::ongoing:
        return tm_transaction_v1::tx_status::ongoing;
    case tm_transaction::tx_status::preparing:
        return tm_transaction_v1::tx_status::preparing;
    case tm_transaction::tx_status::prepared:
        return tm_transaction_v1::tx_status::prepared;
    case tm_transaction::tx_status::aborting:
        return tm_transaction_v1::tx_status::aborting;
    case tm_transaction::tx_status::killed:
        return tm_transaction_v1::tx_status::killed;
    case tm_transaction::tx_status::ready:
        return tm_transaction_v1::tx_status::ready;
    case tm_transaction::tx_status::tombstone:
        return tm_transaction_v1::tx_status::tombstone;
    }
    vassert(false, "unknown status: {}", status);
}

tm_transaction_v1 downgrade_tx(const tm_transaction& tx) {
    tm_transaction_v1 result;
    result.id = tx.id;
    result.pid = tx.pid;
    result.tx_seq = tx.tx_seq;
    result.etag = tx.etag;
    result.status = downgrade_status(tx.status);
    result.timeout_ms = tx.timeout_ms;
    result.last_update_ts = tx.last_update_ts;
    for (auto& partition : tx.partitions) {
        result.partitions.push_back(tm_transaction_v1::tx_partition{
          .ntp = partition.ntp, .etag = partition.etag});
    }
    for (auto& group : tx.groups) {
        result.groups.push_back(tm_transaction_v1::tx_group{
          .group_id = group.group_id, .etag = group.etag});
    }
    return result;
}

template<typename T>
model::record_batch do_serialize_tx(T tx) {
    iobuf key;
    reflection::serialize(key, model::record_batch_type::tm_update);
    auto pid_id = tx.pid.id;
    auto tx_id = tx.id;
    reflection::serialize(key, pid_id, tx_id);

    iobuf value;
    reflection::serialize(value, T::version);
    reflection::serialize(value, std::move(tx));

    storage::record_batch_builder b(
      model::record_batch_type::tm_update, model::offset(0));
    b.add_raw_kv(std::move(key), std::move(value));
    return std::move(b).build();
}

} // namespace

ss::future<result<raft::replicate_result>>
tm_stm::replicate_quorum_ack(model::term_id term, model::record_batch&& batch) {
    auto opts = raft::replicate_options{raft::consistency_level::quorum_ack};
    opts.set_force_flush();
    return _raft->replicate(
      term, model::make_memory_record_batch_reader(std::move(batch)), opts);
}

model::record_batch tm_stm::serialize_tx(tm_transaction tx) {
    if (use_new_tx_version()) {
        return do_serialize_tx(tx);
    }
    auto old_tx = downgrade_tx(tx);
    return do_serialize_tx(old_tx);
}

tm_stm::tm_stm(
  ss::logger& logger,
  raft::consensus* c,
  ss::sharded<features::feature_table>& feature_table,
  ss::lw_shared_ptr<cluster::tm_stm_cache> tm_stm_cache)
  : raft::persisted_stm<>(tm_stm_snapshot, logger, c)
  , _sync_timeout(config::shard_local_cfg().tm_sync_timeout_ms.value())
  , _transactional_id_expiration(
      config::shard_local_cfg().transactional_id_expiration_ms.bind())
  , _feature_table(feature_table)
  , _cache(tm_stm_cache)
  , _ctx_log(logger, ssx::sformat("[{}]", _raft->ntp())) {}

ss::future<> tm_stm::start() {
    // clear cache on startup as the stm now owns the state and it will be
    // populated with stm internal mechanics
    _cache->clear_log();
    _cache->clear_mem();
    co_await persisted_stm::start();
}

uint8_t tm_stm::active_snapshot_version() { return tm_snapshot::version; }

std::optional<tm_transaction>
tm_stm::find_tx(const kafka::transactional_id& tx_id) {
    auto tx_opt = _cache->find_mem(tx_id);
    if (tx_opt) {
        return tx_opt;
    }
    return _cache->find_log(tx_id);
}

ss::future<checked<tm_transaction, tm_stm::op_status>>
tm_stm::get_tx(kafka::transactional_id tx_id) {
    auto r = co_await sync(_sync_timeout);
    if (!r.has_value()) {
        co_return r.error();
    }
    auto tx_opt = _cache->find_mem(tx_id);
    if (tx_opt) {
        co_return tx_opt.value();
    }
    tx_opt = _cache->find_log(tx_id);
    if (!tx_opt) {
        co_return tm_stm::op_status::not_found;
    }
    co_return tx_opt.value();
}

ss::future<checked<model::term_id, tm_stm::op_status>> tm_stm::barrier() {
    return ss::with_gate(_gate, [this] { return do_barrier(); });
}

ss::future<checked<model::term_id, tm_stm::op_status>> tm_stm::do_barrier() {
    if (!_raft->is_leader()) {
        return ss::make_ready_future<
          checked<model::term_id, tm_stm::op_status>>(
          tm_stm::op_status::not_leader);
    }
    if (_insync_term != _raft->term()) {
        return ss::make_ready_future<
          checked<model::term_id, tm_stm::op_status>>(
          tm_stm::op_status::not_leader);
    }
    auto term = _insync_term;
    return quorum_write_empty_batch(model::timeout_clock::now() + _sync_timeout)
      .then_wrapped(
        [this, term](ss::future<result<raft::replicate_result>> f)
          -> checked<model::term_id, tm_stm::op_status> {
            try {
                if (!f.get0().has_value()) {
                    return tm_stm::op_status::unknown;
                }
                if (term != _raft->term()) {
                    return tm_stm::op_status::unknown;
                }
                return term;
            } catch (...) {
                vlog(
                  _ctx_log.error,
                  "Error during writing a barrier batch: {}",
                  std::current_exception());
                return tm_stm::op_status::unknown;
            }
        });
}

model::record_batch_reader make_checkpoint() {
    storage::record_batch_builder builder(
      model::record_batch_type::checkpoint, model::offset(0));
    builder.add_raw_kv(iobuf(), iobuf());
    return model::make_memory_record_batch_reader(std::move(builder).build());
}

ss::future<result<raft::replicate_result>>
tm_stm::quorum_write_empty_batch(model::timeout_clock::time_point timeout) {
    using ret_t = result<raft::replicate_result>;
    // replicate checkpoint batch
    return _raft
      ->replicate(
        make_checkpoint(),
        raft::replicate_options(raft::consistency_level::quorum_ack))
      .then([this, timeout](ret_t r) {
          if (!r) {
              return ss::make_ready_future<ret_t>(r);
          }
          return wait(r.value().last_offset, timeout).then([r]() mutable {
              return r;
          });
      });
}

ss::future<> tm_stm::checkpoint_ongoing_txs() {
    if (!use_new_tx_version()) {
        co_return;
    }

    auto txes_to_checkpoint = _cache->checkpoint();
    size_t checkpointed_txes = 0;
    for (auto& tx : txes_to_checkpoint) {
        vlog(
          _ctx_log.trace,
          "transfering tx:{} etag:{} pid:{} tx_seq:{}",
          tx.id,
          tx.etag,
          tx.pid,
          tx.tx_seq);
        tx.transferring = true;
        auto result = co_await update_tx(tx, tx.etag);
        if (!result.has_value()) {
            vlog(
              _ctx_log.warn,
              "Error {} transferring tx {} to new leader, transferred {}/{} "
              "txns.",
              result.error(),
              tx,
              checkpointed_txes,
              txes_to_checkpoint.size());
            // On failure, txn state does not carry over to the new leader
            // and client gets a failure on next RPC and is expected to retry.
            // This does not cause correctness issues.
            co_return;
        }
        checkpointed_txes++;
    }
    vlog(
      _ctx_log.info,
      "Checkpointed all txes: {} to the new leader.",
      txes_to_checkpoint.size());
}

ss::future<ss::basic_rwlock<>::holder> tm_stm::prepare_transfer_leadership() {
    vlog(_ctx_log.trace, "Preparing for leadership transfer");
    auto units = co_await _cache->write_lock();
    // This is a best effort basis, we checkpoint as many as we can
    // and stop at the first error.
    co_await checkpoint_ongoing_txs();
    co_return units;
}

ss::future<checked<model::term_id, tm_stm::op_status>>
tm_stm::sync(model::timeout_clock::duration timeout) {
    return ss::with_gate(_gate, [this, timeout] { return do_sync(timeout); });
}

ss::future<checked<model::term_id, tm_stm::op_status>>
tm_stm::do_sync(model::timeout_clock::duration timeout) {
    if (!_raft->is_leader()) {
        co_return tm_stm::op_status::not_leader;
    }

    auto old_term = _insync_term;
    auto ready = co_await persisted_stm::sync(timeout);
    if (!ready) {
        _cache->clear_mem();
        co_return tm_stm::op_status::unknown;
    }
    if (old_term != _insync_term) {
        _cache->clear_mem();
    }
    co_return _insync_term;
}

ss::future<checked<tm_transaction, tm_stm::op_status>>
tm_stm::update_tx(tm_transaction tx, model::term_id term) {
    return ss::with_gate(
      _gate, [this, tx, term] { return do_update_tx(tx, term); });
}

ss::future<checked<tm_transaction, tm_stm::op_status>>
tm_stm::do_update_tx(tm_transaction tx, model::term_id term) {
    vlog(
      _ctx_log.trace,
      "[tx_id={}] updating transaction: {} in term: {}",
      tx.id,
      tx,
      term);
    auto batch = serialize_tx(tx);

    auto r = co_await replicate_quorum_ack(term, std::move(batch));
    if (!r) {
        vlog(
          _ctx_log.info,
          "[tx_id={}] error updating tx: {} - {}",
          tx.id,
          tx,
          r.error());
        if (_raft->is_leader() && _raft->term() == term) {
            co_await _raft->step_down(
              "txn coordinator update_tx replication error");
        }
        if (r.error() == raft::errc::shutting_down) {
            co_return tm_stm::op_status::timeout;
        }
        co_return tm_stm::op_status::unknown;
    }

    auto offset = model::offset(r.value().last_offset());
    if (!co_await wait_no_throw(
          offset, model::timeout_clock::now() + _sync_timeout)) {
        vlog(
          _ctx_log.info,
          "[tx_id={}] timeout waiting for offset {} to be applied tx: {}",
          tx.id,
          offset,
          tx);
        if (_raft->is_leader() && _raft->term() == term) {
            co_await _raft->step_down("txn coordinator apply timeout");
        }
        co_return tm_stm::op_status::unknown;
    }
    if (_raft->term() != term) {
        vlog(
          _ctx_log.info,
          "[tx_id={}] leadership while waiting until offset {} is applied tx: "
          "{}",
          tx.id,
          offset,
          tx);

        co_return tm_stm::op_status::unknown;
    }

    auto tx_opt = _cache->find_log(tx.id);
    if (!tx_opt) {
        vlog(
          _ctx_log.warn,
          "[tx_id={}] can't find an updated tx: {} in the cache",
          tx.id,
          tx);
        // update_tx must return conflict only in this case, see expire_tx
        co_return tm_stm::op_status::conflict;
    }
    co_return tx_opt.value();
}

ss::future<checked<tm_transaction, tm_stm::op_status>> tm_stm::mark_tx_aborting(
  model::term_id expected_term, kafka::transactional_id tx_id) {
    vlog(
      _ctx_log.trace,
      "[tx_id={}] marking transaction as aborted in term: {}",
      tx_id,
      expected_term);
    auto ptx = co_await get_tx(tx_id);
    if (!ptx.has_value()) {
        co_return ptx;
    }
    auto tx = ptx.value();
    if (tx.status != tm_transaction::tx_status::ongoing) {
        co_return tm_stm::op_status::conflict;
    }
    tx.status = cluster::tm_transaction::tx_status::aborting;
    tx.last_update_ts = clock_type::now();
    co_return co_await update_tx(std::move(tx), expected_term);
}

ss::future<checked<tm_transaction, tm_stm::op_status>> tm_stm::mark_tx_prepared(
  model::term_id expected_term, kafka::transactional_id tx_id) {
    vlog(
      _ctx_log.trace,
      "[tx_id={}] marking transaction as prepared in term: {}",
      tx_id,
      expected_term);
    auto tx_opt = co_await get_tx(tx_id);
    if (!tx_opt.has_value()) {
        vlog(
          _ctx_log.trace,
          "[tx_id={}] error getting transaction - {}",
          tx_id,
          tx_opt.error());
        co_return tx_opt;
    }
    auto tx = tx_opt.value();

    auto check_status = is_transaction_ga()
                          ? tm_transaction::tx_status::ongoing
                          : tm_transaction::tx_status::preparing;
    if (tx.status != check_status) {
        vlog(
          _ctx_log.warn,
          "[tx_id={}] error marking transaction {} as prepared. Incorrect "
          "status {} != {}",
          tx_id,
          tx,
          tx.status,
          check_status);
        co_return tm_stm::op_status::conflict;
    }
    tx.status = cluster::tm_transaction::tx_status::prepared;
    tx.last_update_ts = clock_type::now();
    co_return co_await update_tx(std::move(tx), expected_term);
}

ss::future<checked<tm_transaction, tm_stm::op_status>> tm_stm::mark_tx_killed(
  model::term_id expected_term, kafka::transactional_id tx_id) {
    vlog(
      _ctx_log.trace,
      "[tx_id={}] marking transaction as killed in term: {}",
      tx_id,
      expected_term);
    auto tx_opt = co_await get_tx(tx_id);
    if (!tx_opt.has_value()) {
        co_return tx_opt;
    }
    auto tx = tx_opt.value();
    if (
      tx.status != tm_transaction::tx_status::ongoing
      && tx.status != tm_transaction::tx_status::preparing) {
        co_return tm_stm::op_status::conflict;
    }
    tx.status = cluster::tm_transaction::tx_status::killed;
    tx.last_update_ts = clock_type::now();
    co_return co_await update_tx(std::move(tx), expected_term);
}

ss::future<checked<tm_transaction, tm_stm::op_status>>
tm_stm::reset_transferring(model::term_id term, kafka::transactional_id tx_id) {
    vlog(
      _ctx_log.trace,
      "[tx_id={}] resetting transfer of transaction in term: {}",
      tx_id,
      term);
    auto ptx = co_await get_tx(tx_id);
    if (!ptx.has_value()) {
        co_return ptx;
    }
    auto tx = ptx.value();
    // Check if transferring.
    if (!tx.transferring) {
        co_return tm_stm::op_status::conflict;
    }
    vlog(
      _ctx_log.trace,
      "[tx_id={}] observed a transferring tx: {}, term: {}",
      tx_id,
      tx,
      term);
    if (tx.etag == term) {
        // case 1 - Unlikely, just reset the transferring flag.
        vlog(
          _ctx_log.warn,
          "[tx_id={}] transferring within same term: {}, resetting.",
          tx_id,
          tx.etag);
    }
    // case 2 - Valid, txn is getting transferred from previous term.
    tx.etag = term;
    tx.transferring = false;
    auto r = co_await update_tx(tx, tx.etag);
    if (!r.has_value()) {
        co_return r;
    }
    tx = r.value();
    _cache->set_mem(tx.etag, tx_id, tx);
    co_return tx;
}

ss::future<checked<tm_transaction, tm_stm::op_status>> tm_stm::mark_tx_ongoing(
  model::term_id expected_term, kafka::transactional_id tx_id) {
    vlog(
      _ctx_log.trace,
      "[tx_id={}] marking transaction as ongoing in term: {}",
      tx_id,
      expected_term);
    auto tx_opt = co_await get_tx(tx_id);
    if (!tx_opt.has_value()) {
        co_return tx_opt;
    }
    tm_transaction tx = tx_opt.value();
    if (tx.etag != expected_term) {
        vlog(
          _ctx_log.warn,
          "[tx_id={}] attempt to update state data pid:{} tx_seq:{} etag:{} "
          "assuming etag is {}",
          tx.id,
          tx.pid,
          tx.tx_seq,
          tx.etag,
          expected_term);
        co_return tm_stm::op_status::unknown;
    }
    tx.status = tm_transaction::tx_status::ongoing;
    tx.tx_seq += 1;
    tx.partitions.clear();
    tx.groups.clear();
    tx.last_update_ts = clock_type::now();
    _cache->set_mem(tx.etag, tx_id, tx);
    co_return tx;
}

ss::future<tm_stm::op_status> tm_stm::re_register_producer(
  model::term_id expected_term,
  kafka::transactional_id tx_id,
  std::chrono::milliseconds transaction_timeout_ms,
  model::producer_identity pid,
  model::producer_identity last_pid,
  model::producer_identity rolled_pid) {
    vlog(
      _ctx_log.trace,
      "[tx_id={}] Registering existing transaction with new pid: {}, previous "
      "pid: {}, rolled_pid: {}",
      tx_id,
      pid,
      last_pid,
      rolled_pid);

    auto tx_opt = co_await get_tx(tx_id);
    if (!tx_opt.has_value()) {
        co_return tx_opt.error();
    }
    tm_transaction tx = tx_opt.value();
    tx.status = tm_transaction::tx_status::ready;
    tx.pid = pid;
    tx.last_pid = last_pid;
    tx.tx_seq += 1;
    tx.etag = expected_term;
    tx.timeout_ms = transaction_timeout_ms;
    tx.partitions.clear();
    tx.groups.clear();
    tx.last_update_ts = clock_type::now();

    _pid_tx_id[pid] = tx_id;

    auto r = co_await update_tx(std::move(tx), expected_term);

    if (!r.has_value()) {
        co_return tm_stm::op_status::unknown;
    }
    _pid_tx_id.erase(rolled_pid);
    co_return tm_stm::op_status::success;
}

ss::future<tm_stm::op_status> tm_stm::register_new_producer(
  model::term_id expected_term,
  kafka::transactional_id tx_id,
  std::chrono::milliseconds transaction_timeout_ms,
  model::producer_identity pid) {
    return ss::with_gate(
      _gate, [this, expected_term, tx_id, transaction_timeout_ms, pid] {
          return do_register_new_producer(
            expected_term, tx_id, transaction_timeout_ms, pid);
      });
}

ss::future<tm_stm::op_status> tm_stm::do_register_new_producer(
  model::term_id expected_term,
  kafka::transactional_id tx_id,
  std::chrono::milliseconds transaction_timeout_ms,
  model::producer_identity pid) {
    vlog(
      _ctx_log.trace,
      "[tx_id={}] Registering new transaction pid: {}, term: {}",
      tx_id,
      pid,
      expected_term);

    auto tx_opt = co_await get_tx(tx_id);
    if (tx_opt.has_value()) {
        co_return tm_stm::op_status::conflict;
    }

    auto tx = tm_transaction{
      .id = tx_id,
      .pid = pid,
      .last_pid = model::no_pid,
      .tx_seq = model::tx_seq(0),
      .etag = expected_term,
      .status = tm_transaction::tx_status::ready,
      .timeout_ms = transaction_timeout_ms,
      .last_update_ts = clock_type::now()};
    auto batch = serialize_tx(tx);

    _pid_tx_id[pid] = tx_id;

    auto r = co_await replicate_quorum_ack(expected_term, std::move(batch));

    if (!r) {
        if (_raft->is_leader() && _raft->term() == expected_term) {
            co_await _raft->step_down(
              "txn coordinator register_new_producer replication error");
        }
        co_return tm_stm::op_status::unknown;
    }

    if (!co_await wait_no_throw(
          model::offset(r.value().last_offset()),
          model::timeout_clock::now() + _sync_timeout)) {
        co_return tm_stm::op_status::unknown;
    }
    if (_raft->term() != expected_term) {
        // we lost leadership during waiting
        co_return tm_stm::op_status::unknown;
    }

    _cache->set_mem(tx.etag, tx_id, tx);

    co_return tm_stm::op_status::success;
}

ss::future<tm_stm::op_status> tm_stm::add_partitions(
  model::term_id expected_term,
  kafka::transactional_id tx_id,
  std::vector<tm_transaction::tx_partition> partitions) {
    auto tx_opt = find_tx(tx_id);
    if (!tx_opt) {
        vlog(
          _ctx_log.warn,
          "[tx_id={}] unable to find ongoing transaction",
          tx_id);

        co_return tm_stm::op_status::unknown;
    }
    auto tx = tx_opt.value();
    if (tx.status != tm_transaction::tx_status::ongoing) {
        vlog(
          _ctx_log.warn,
          "[tx_id={}] expected ongoing transaction, found: {} ",
          tx_id,
          tx);

        co_return tm_stm::op_status::unknown;
    }
    if (tx.etag != expected_term) {
        vlog(
          _ctx_log.warn,
          "[tx_id={}] adding partition fenced transaction: {} expected term: "
          "{}",
          tx_id,
          tx,
          expected_term);

        co_return tm_stm::op_status::unknown;
    }

    if (!is_transaction_ga()) {
        bool just_started = tx.partitions.size() == 0 && tx.groups.size() == 0;

        if (just_started) {
            for (auto& partition : partitions) {
                tx.partitions.push_back(partition);
            }
            tx.last_update_ts = clock_type::now();
            auto r = co_await update_tx(tx, tx.etag);

            if (!r.has_value()) {
                co_return tm_stm::op_status::unknown;
            }
            _cache->set_mem(tx.etag, tx_id, tx);
            co_return tm_stm::op_status::success;
        }
    }

    for (auto& partition : partitions) {
        tx.partitions.push_back(partition);
    }
    tx.last_update_ts = clock_type::now();
    _cache->set_mem(tx.etag, tx_id, tx);
    vlog(
      _ctx_log.trace,
      "[tx_id={}] transaction: {} added with etag: {}",
      tx_id,
      tx,
      expected_term);

    co_return tm_stm::op_status::success;
}

ss::future<tm_stm::op_status> tm_stm::add_group(
  model::term_id expected_term,
  kafka::transactional_id tx_id,
  kafka::group_id group_id,
  model::term_id etag) {
    auto tx_opt = find_tx(tx_id);
    if (!tx_opt) {
        vlog(
          _ctx_log.trace,
          "[tx_id={}] unable to find ongoing transaction",
          tx_id);
        co_return tm_stm::op_status::unknown;
    }
    auto tx = tx_opt.value();
    if (tx.status != tm_transaction::tx_status::ongoing) {
        vlog(
          _ctx_log.warn,
          "[tx_id={}] expected ongoing transaction, found: {} ",
          tx_id,
          tx);
        co_return tm_stm::op_status::unknown;
    }
    if (tx.etag != expected_term) {
        vlog(
          _ctx_log.warn,
          "[tx_id={}] adding group fenced transaction: {} expected term: {}",
          tx_id,
          tx,
          expected_term);
        co_return tm_stm::op_status::unknown;
    }

    if (!is_transaction_ga()) {
        bool just_started = tx.partitions.size() == 0 && tx.groups.size() == 0;

        if (just_started) {
            tx.groups.push_back(
              tm_transaction::tx_group{.group_id = group_id, .etag = etag});
            tx.last_update_ts = clock_type::now();
            auto r = co_await update_tx(tx, tx.etag);

            if (!r.has_value()) {
                co_return tm_stm::op_status::unknown;
            }
            _cache->set_mem(tx.etag, tx_id, tx);
            co_return tm_stm::op_status::success;
        }
    }

    tx.groups.push_back(
      tm_transaction::tx_group{.group_id = group_id, .etag = etag});
    tx.last_update_ts = clock_type::now();
    _cache->set_mem(tx.etag, tx_id, tx);

    co_return tm_stm::op_status::success;
}

ss::future<>
tm_stm::apply_local_snapshot(raft::stm_snapshot_header hdr, iobuf&& tm_ss_buf) {
    vassert(
      hdr.version >= tm_snapshot_v0::version
        && hdr.version <= tm_snapshot::version,
      "unsupported seq_snapshot_header version {}",
      hdr.version);
    iobuf_parser data_parser(std::move(tm_ss_buf));
    if (hdr.version == tm_snapshot_v0::version) {
        auto data = reflection::adl<tm_snapshot_v0>{}.from(data_parser);

        _cache->clear_mem();
        _cache->clear_log();
        for (auto& entry : data.transactions) {
            _cache->set_log(entry);
            _pid_tx_id[entry.pid] = entry.id;
        }
    } else if (hdr.version == tm_snapshot::version) {
        auto data = reflection::adl<tm_snapshot>{}.from(data_parser);

        _cache->clear_mem();
        _cache->clear_log();
        for (auto& entry : data.transactions) {
            _cache->set_log(entry);
            _pid_tx_id[entry.pid] = entry.id;
        }

        vlog(_ctx_log.trace, "Applied snapshot at offset: {}", hdr.offset);
    }

    return ss::now();
}

ss::future<raft::stm_snapshot> tm_stm::take_local_snapshot() {
    // Update hash ranges to always have batch in log
    // So it cannot be deleted with cleanup policy
    if (_raft->is_leader()) {
        auto sync_res = co_await sync();
        if (sync_res.has_error()) {
            throw std::runtime_error(fmt::format(
              "Cannot sync before taking snapshot, err: {}", sync_res.error()));
        }
    }
    co_return co_await ss::with_gate(
      _gate, [this] { return do_take_snapshot(); });
}

ss::future<raft::stm_snapshot> tm_stm::do_take_snapshot() {
    auto snapshot_version = active_snapshot_version();
    if (snapshot_version == tm_snapshot_v0::version) {
        tm_snapshot_v0 tm_ss;
        tm_ss.offset = last_applied_offset();
        tm_ss.transactions = _cache->get_log_transactions();

        iobuf tm_ss_buf;
        reflection::adl<tm_snapshot_v0>{}.to(tm_ss_buf, std::move(tm_ss));

        co_return raft::stm_snapshot::create(
          tm_snapshot_v0::version, last_applied_offset(), std::move(tm_ss_buf));
    } else {
        tm_snapshot tm_ss;
        tm_ss.offset = last_applied_offset();
        tm_ss.transactions = _cache->get_log_transactions();

        iobuf tm_ss_buf;
        reflection::adl<tm_snapshot>{}.to(tm_ss_buf, std::move(tm_ss));

        co_return raft::stm_snapshot::create(
          tm_snapshot::version, last_applied_offset(), std::move(tm_ss_buf));
    }
}

ss::future<>
tm_stm::apply_tm_update(model::record_batch_header hdr, model::record_batch b) {
    vassert(
      b.record_count() == 1,
      "model::record_batch_type::tm_update batch must contain a single record");
    auto r = b.copy_records();
    auto& record = *r.begin();
    auto val_buf = record.release_value();

    iobuf_parser val_reader(std::move(val_buf));
    auto version = reflection::adl<int8_t>{}.from(val_reader);

    tm_transaction tx;
    switch (version) {
    case tm_transaction_v0::version: {
        auto tx0 = reflection::adl<tm_transaction_v0>{}.from(val_reader);
        tx = tx0.upcast();
        break;
    }
    case tm_transaction_v1::version: {
        auto tx1 = reflection::adl<tm_transaction_v1>{}.from(val_reader);
        tx = tx1.upcast();
        break;
    }
    default: {
        vassert(
          version == tm_transaction::version,
          "unknown group inflight tx record version: {} expected: {}",
          version,
          tm_transaction::version);
        tx = reflection::adl<tm_transaction>{}.from(val_reader);
        break;
    }
    }

    auto key_buf = record.release_key();
    iobuf_parser key_reader(std::move(key_buf));
    auto batch_type = reflection::adl<model::record_batch_type>{}.from(
      key_reader);
    vassert(
      hdr.type == batch_type,
      "broken model::record_batch_type::tm_update. expected batch type {} got: "
      "{}",
      hdr.type,
      batch_type);
    auto p_id = model::producer_id(reflection::adl<int64_t>{}.from(key_reader));
    vassert(
      p_id == tx.pid.id,
      "broken model::record_batch_type::tm_update. expected tx.pid {} got: {}",
      tx.pid.id,
      p_id);
    auto tx_id = kafka::transactional_id(
      reflection::adl<ss::sstring>{}.from(key_reader));
    vassert(
      tx_id == tx.id,
      "broken model::record_batch_type::tm_update. expected tx.id {} got: {}",
      tx.id,
      tx_id);
    vlog(
      _ctx_log.trace,
      "[tx_id={}] applying transaction: {} in term: {}",
      tx.id,
      tx,
      _insync_term);

    if (tx.status == tm_transaction::tx_status::tombstone) {
        _cache->erase_log(tx.id);
        vlog(
          _ctx_log.trace,
          "[tx_id={}] erasing (tombstone) transaction: {} in term: {} from "
          "memory",
          tx.id,
          tx,
          _insync_term);
        _cache->erase_mem(tx.id);
        _pid_tx_id.erase(tx.pid);
        return ss::now();
    }

    auto tx_opt = _cache->find_mem(tx.id);
    if (tx_opt) {
        auto old_tx = tx_opt.value();
        if (
          (old_tx.etag < tx.etag)
          || (old_tx.etag == tx.etag && old_tx.tx_seq <= tx.tx_seq)) {
            _cache->erase_mem(tx.id);
            vlog(
              _ctx_log.trace,
              "[tx_id={}] erasing (log overwrite) transaction: {} in term: {} "
              "from memory by new transaction: {}",
              tx.id,
              old_tx,
              _insync_term,
              tx);
        }
    }

    _cache->set_log(tx);
    _pid_tx_id.erase(tx.last_pid);
    _pid_tx_id[tx.pid] = tx.id;

    return ss::now();
}

ss::future<> tm_stm::apply(const model::record_batch& b) {
    const auto& hdr = b.header();

    if (hdr.type == model::record_batch_type::tm_update) {
        return apply_tm_update(hdr, b.copy());
    }

    return ss::now();
}

bool tm_stm::is_expired(const tm_transaction& tx) {
    auto now_ts = clock_type::now();
    return _transactional_id_expiration() < now_ts - tx.last_update_ts;
}

ss::lw_shared_ptr<mutex> tm_stm::get_tx_lock(kafka::transactional_id tid) {
    auto [lock_it, inserted] = _tx_locks.try_emplace(tid, nullptr);
    if (inserted) {
        lock_it->second = ss::make_lw_shared<mutex>("tm_stm::tx_lock");
    }
    return lock_it->second;
}

ss::future<txlock_unit>
tm_stm::lock_tx(kafka::transactional_id tx_id, std::string_view lock_name) {
    auto [lock_it, inserted] = _tx_locks.try_emplace(tx_id, nullptr);
    if (inserted) {
        lock_it->second = ss::make_lw_shared<mutex>("lock_tx");
    }
    auto units = co_await lock_it->second->get_units();
    co_return txlock_unit(this, std::move(units), tx_id, lock_name);
}

std::optional<txlock_unit>
tm_stm::try_lock_tx(kafka::transactional_id tx_id, std::string_view lock_name) {
    auto [lock_it, inserted] = _tx_locks.try_emplace(tx_id, nullptr);
    if (inserted) {
        lock_it->second = ss::make_lw_shared<mutex>("tm_stm::tx_lock");
    }
    auto units = lock_it->second->try_get_units();
    if (units) {
        return txlock_unit(this, std::move(units.value()), tx_id, lock_name);
    }
    return std::nullopt;
}

absl::btree_set<kafka::transactional_id> tm_stm::get_expired_txs() {
    auto now_ts = clock_type::now();
    auto ids = _cache->filter_all_txid_by_tx([this, now_ts](auto tx) {
        return _transactional_id_expiration() < now_ts - tx.last_update_ts;
    });
    return ids;
}

ss::future<tm_stm::get_txs_result> tm_stm::get_all_transactions() {
    if (!_raft->is_leader()) {
        co_return tm_stm::op_status::not_leader;
    }

    auto is_ready = co_await sync(_sync_timeout);
    if (!is_ready) {
        co_return tm_stm::op_status::unknown;
    }

    co_return _cache->get_all_transactions();
}

size_t tm_stm::tx_cache_size() const { return _cache->tx_cache_size(); }

std::optional<tm_transaction> tm_stm::oldest_tx() const {
    return _cache->oldest_tx();
}

ss::future<checked<tm_transaction, tm_stm::op_status>>
tm_stm::delete_partition_from_tx(
  model::term_id term,
  kafka::transactional_id tid,
  tm_transaction::tx_partition ntp) {
    if (!_raft->is_leader()) {
        co_return tm_stm::op_status::not_leader;
    }

    auto optional_tx = co_await get_tx(tid);
    if (!optional_tx.has_value()) {
        co_return optional_tx;
    }

    auto tx = optional_tx.value();

    auto res = tx.delete_partition(ntp);
    if (!res) {
        co_return tm_stm::op_status::partition_not_found;
    }

    if (tx.status == tm_transaction::tx_status::ongoing) {
        _cache->set_mem(term, tid, tx);
        co_return tx;
    } else {
        co_return co_await update_tx(std::move(tx), term);
    }
}

ss::future<tm_stm::op_status>
tm_stm::expire_tx(model::term_id term, kafka::transactional_id tx_id) {
    auto tx_opt = co_await get_tx(tx_id);
    if (!tx_opt.has_value()) {
        co_return tm_stm::op_status::unknown;
    }
    tm_transaction tx = tx_opt.value();
    tx.etag = term;
    tx.status = tm_transaction::tx_status::tombstone;
    tx.last_pid = model::no_pid;
    tx.partitions.clear();
    tx.groups.clear();
    tx.last_update_ts = clock_type::now();
    auto etag = tx.etag;
    auto r0 = co_await update_tx(std::move(tx), etag);
    if (r0.has_value()) {
        vlog(
          _ctx_log.error,
          "[tx_id={}] written tombstone should evict transaction from the "
          "cache",
          tx_id);
        co_return tm_stm::op_status::unknown;
    }
    if (r0.error() == tm_stm::op_status::conflict) {
        // update_tx returns conflict when it can't find
        // tx after the successful update; it may happen only
        // with the tombstone
        co_return tm_stm::op_status::success;
    }
    co_return r0.error();
}

ss::future<> tm_stm::apply_raft_snapshot(const iobuf&) {
    return _cache->write_lock().then(
      [this]([[maybe_unused]] ss::basic_rwlock<>::holder unit) {
          _cache->clear_log();
          _cache->clear_mem();
          _pid_tx_id.clear();
          return ss::now();
      });
}

tm_stm_factory::tm_stm_factory(
  ss::sharded<tm_stm_cache_manager>& tm_stm_cache_manager,
  ss::sharded<features::feature_table>& feature_table)
  : _tm_stm_cache_manager(tm_stm_cache_manager)
  , _feature_table(feature_table) {}

bool tm_stm_factory::is_applicable_for(const storage::ntp_config& cfg) const {
    auto const& ntp = cfg.ntp();
    return ntp.ns == model::kafka_internal_namespace
           && ntp.tp.topic == model::tx_manager_topic;
}

void tm_stm_factory::create(
  raft::state_machine_manager_builder& builder, raft::consensus* raft) {
    auto tm_stm = builder.create_stm<cluster::tm_stm>(
      clusterlog,
      raft,
      _feature_table,
      _tm_stm_cache_manager.local().get(raft->ntp().tp.partition));
    raft->log()->stm_manager()->add_stm(tm_stm);
}

} // namespace cluster

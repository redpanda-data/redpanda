// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cluster/tm_stm.h"

#include "cluster/tm_stm_types.h"
#include "cluster/types.h"
#include "container/fragmented_vector.h"
#include "kafka/protocol/types.h"
#include "model/record.h"
#include "raft/errc.h"
#include "storage/record_batch_builder.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/future.hh>
#include <seastar/util/bool_class.hh>

#include <absl/container/btree_set.h>

#include <cstdint>
#include <optional>
#include <ranges>
namespace cluster {

ss::future<result<raft::replicate_result>>
tm_stm::replicate_quorum_ack(model::term_id term, model::record_batch&& batch) {
    auto opts = raft::replicate_options{raft::consistency_level::quorum_ack};
    opts.set_force_flush();
    return _raft->replicate(
      term, model::make_memory_record_batch_reader(std::move(batch)), opts);
}

model::record_batch tm_stm::serialize_tx(tx_metadata tx) {
    iobuf key;
    reflection::serialize(key, model::record_batch_type::tm_update);
    auto pid_id = tx.pid.id;
    auto tx_id = tx.id;
    reflection::serialize(key, pid_id, tx_id);

    iobuf value;
    reflection::serialize(value, tx_metadata::version);
    reflection::serialize(value, std::move(tx));

    storage::record_batch_builder b(
      model::record_batch_type::tm_update, model::offset(0));
    b.add_raw_kv(std::move(key), std::move(value));
    return std::move(b).build();
}

tm_stm::tm_stm(
  ss::logger& logger,
  raft::consensus* c,
  ss::sharded<features::feature_table>& feature_table)
  : raft::persisted_stm<>(tm_stm_snapshot, logger, c)
  , _sync_timeout(config::shard_local_cfg().tm_sync_timeout_ms.value())
  , _transactional_id_expiration(
      config::shard_local_cfg().transactional_id_expiration_ms.bind())
  , _feature_table(feature_table)
  , _ctx_log(logger, ssx::sformat("[{}]", _raft->ntp())) {}

ss::future<> tm_stm::start() { co_await raft::persisted_stm<>::start(); }

uint8_t tm_stm::active_snapshot_version() { return tm_snapshot::version; }

std::optional<tx_metadata>
tm_stm::find_tx(const kafka::transactional_id& tx_id) {
    auto it = _transactions.find(tx_id);
    if (it == _transactions.end()) {
        return std::nullopt;
    }
    return it->second.tx;
}

ss::future<checked<tx_metadata, tm_stm::op_status>>
tm_stm::get_tx(kafka::transactional_id tx_id) {
    auto r = co_await sync(_sync_timeout);
    if (!r.has_value()) {
        vlog(
          txlog.info,
          "[tx_id={}] error syncing state machine for getting transaction - {}",
          tx_id,
          r.error());
        co_return r.error();
    }

    auto tx_opt = find_tx(tx_id);
    if (!tx_opt) {
        vlog(txlog.trace, "[tx_id={}] transaction state not found", tx_id);
        co_return tm_stm::op_status::not_found;
    }
    co_return tx_opt.value();
}

ss::future<checked<model::term_id, tm_stm::op_status>> tm_stm::barrier() {
    return ss::with_gate(_gate, [this] { return do_barrier(); });
}

ss::future<checked<model::term_id, tm_stm::op_status>> tm_stm::do_barrier() {
    auto sync_result = co_await sync();
    if (!sync_result.has_value()) {
        vlog(
          txlog.info,
          "error syncing state machine for barrier - {}",
          sync_result.error());
        co_return sync_result.error();
    }

    auto term = _insync_term;
    co_return co_await quorum_write_empty_batch(
      model::timeout_clock::now() + _sync_timeout)
      .then_wrapped(
        [this, term](ss::future<result<raft::replicate_result>> f)
          -> checked<model::term_id, tm_stm::op_status> {
            try {
                if (!f.get().has_value()) {
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
        _insync_term,
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

ss::future<ss::basic_rwlock<>::holder> tm_stm::prepare_transfer_leadership() {
    vlog(_ctx_log.trace, "Preparing for leadership transfer");
    auto units = co_await _state_lock.hold_write_lock();
    // This is a best effort basis, we checkpoint as many as we can
    // and stop at the first error.
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

    auto ready = co_await raft::persisted_stm<>::sync(timeout);
    if (!ready) {
        co_return tm_stm::op_status::unknown;
    }

    co_return _insync_term;
}

ss::future<checked<tx_metadata, tm_stm::op_status>>
tm_stm::update_tx(tx_metadata tx, model::term_id term) {
    return ss::with_gate(_gate, [this, tx = std::move(tx), term]() mutable {
        return do_update_tx(std::move(tx), term);
    });
}

ss::future<checked<tx_metadata, tm_stm::op_status>>
tm_stm::do_update_tx(tx_metadata tx, model::term_id term) {
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

    auto tx_opt = find_tx(tx.id);
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

ss::future<checked<tx_metadata, tm_stm::op_status>>
tm_stm::update_transaction_status(
  model::term_id expected_term,
  kafka::transactional_id tx_id,
  tx_status status) {
    vassert(
      status != tx_status::completed_commit
        && status != tx_status::completed_abort,
      "Status update to completed state must be done thorough "
      "finish_transaction method");

    vlog(
      _ctx_log.trace,
      "[tx_id={}] updating transaction status with: {} in term: {}",
      tx_id,
      status,
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

    auto err = tx.try_update_status(status);
    if (err) {
        vlog(
          _ctx_log.warn,
          "[tx_id={}] error updating transaction status - {}",
          tx_id,
          err.value());
        co_return tm_stm::op_status::conflict;
    }
    co_return co_await update_tx(std::move(tx), expected_term);
}

ss::future<checked<tx_metadata, tm_stm::op_status>> tm_stm::finish_transaction(
  model::term_id expected_term,
  kafka::transactional_id tx_id,
  tx_status completed_status,
  bool bump_producer_epoch) {
    vassert(
      completed_status == tx_status::completed_commit
        || completed_status == tx_status::completed_abort,
      "Can not complete transaction with status: {}",
      completed_status);
    vlog(
      _ctx_log.trace,
      "[tx_id={}] finishing transaction with status of: {}",
      tx_id,
      completed_status);
    auto tx_opt = co_await get_tx(tx_id);

    if (expected_term != _insync_term) {
        co_return op_status::not_leader;
    }

    if (!tx_opt.has_value()) {
        co_return tx_opt;
    }

    tx_metadata tx = std::move(tx_opt.value());
    auto err = tx.try_update_status(completed_status);
    if (err) {
        vlog(
          _ctx_log.warn,
          "[tx_id={}] error updating transaction status - {}",
          tx_id,
          err.value());
        co_return tm_stm::op_status::conflict;
    }
    if (bump_producer_epoch) {
        tx.pid.epoch++;
        tx.last_pid.epoch = model::no_producer_epoch;
        vlog(
          _ctx_log.debug,
          "[tx_id={}] bumping transaction producer id epoch. New pid: {}",
          tx_id,
          tx.pid);
    }

    co_return co_await update_tx(tx, expected_term);
}

ss::future<tm_stm::op_status> tm_stm::update_tx_producer(
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
    tx_metadata tx = tx_opt.value();
    tx.status = tx_status::empty;
    tx.pid = pid;
    tx.last_pid = last_pid;
    tx.tx_seq = model::tx_seq(0);
    tx.etag = expected_term;
    tx.timeout_ms = transaction_timeout_ms;
    tx.partitions.clear();
    tx.groups.clear();
    tx.last_update_ts = clock_type::now();

    auto r = co_await update_tx(std::move(tx), expected_term);

    if (!r.has_value()) {
        co_return tm_stm::op_status::unknown;
    }
    _pid_tx_id.erase(rolled_pid);
    co_return tm_stm::op_status::success;
}

checked<tx_metadata, tm_stm::op_status>
tm_stm::reset_transaction_state(tx_metadata& tx) {
    if (!tx.is_finished()) {
        vlog(
          _ctx_log.warn,
          "[tx_id={}] unable to reset transaction state that is not finished",
          tx.id);
        return tm_stm::op_status::conflict;
    }

    tx.groups.clear();
    tx.partitions.clear();
    tx.etag = _insync_term;
    tx.status = tx_status::empty;
    tx.tx_seq += 1;
    vlog(
      _ctx_log.trace, "[tx_id={}] reset transaction state to: {}", tx.id, tx);
    upsert_transaction(tx);
    return std::move(tx);
}

ss::future<tm_stm::op_status> tm_stm::register_new_producer(
  model::term_id expected_term,
  kafka::transactional_id tx_id,
  std::chrono::milliseconds transaction_timeout_ms,
  model::producer_identity pid) {
    return ss::with_gate(
      _gate,
      [this,
       expected_term,
       tx_id = std::move(tx_id),
       transaction_timeout_ms,
       pid] {
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
      "[tx_id={}] Registering new producer: {}, term: {}",
      tx_id,
      pid,
      expected_term);

    auto tx_opt = co_await get_tx(tx_id);
    if (tx_opt.has_value()) {
        co_return tm_stm::op_status::conflict;
    }

    auto tx = tx_metadata{
      .id = tx_id,
      .pid = pid,
      .last_pid = model::no_pid,
      .tx_seq = model::tx_seq(0),
      .etag = expected_term,
      .status = tx_status::empty,
      .timeout_ms = transaction_timeout_ms,
      .last_update_ts = clock_type::now()};
    auto batch = serialize_tx(tx);

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

    co_return tm_stm::op_status::success;
}

ss::future<tm_stm::op_status> tm_stm::add_partitions(
  model::term_id expected_term,
  kafka::transactional_id tx_id,
  model::tx_seq tx_seq,
  std::vector<tx_metadata::tx_partition> partitions) {
    auto tx_opt = find_tx(tx_id);
    if (!tx_opt) {
        vlog(
          _ctx_log.warn,
          "[tx_id={}] unable to find ongoing transaction",
          tx_id);

        co_return tm_stm::op_status::unknown;
    }
    auto tx = tx_opt.value();
    tx.tx_seq = tx_seq;
    auto err = tx.try_update_status(cluster::tx_status::ongoing);
    if (err) {
        vlog(
          clusterlog.info,
          "[tx_id={}] error adding partitions to transaction - {}",
          tx_id,
          err.value());
        co_return tm_stm::op_status::conflict;
    }

    for (auto& partition : partitions) {
        tx.partitions.push_back(partition);
    }

    auto result = co_await update_tx(std::move(tx), expected_term);
    if (result.has_error()) {
        co_return result.error();
    }
    vlog(
      _ctx_log.trace,
      "[tx_id={}] transaction: {} added with etag: {}",
      tx_id,
      result.value(),
      expected_term);

    co_return tm_stm::op_status::success;
}

ss::future<tm_stm::op_status> tm_stm::add_group(
  model::term_id expected_term,
  kafka::transactional_id tx_id,
  model::tx_seq tx_seq,
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
    auto err = tx.try_update_status(cluster::tx_status::ongoing);
    if (err) {
        vlog(
          clusterlog.info,
          "[tx_id={}] error adding groups to transaction - {}",
          tx_id,
          err.value());
        co_return tm_stm::op_status::conflict;
    }
    tx.tx_seq = tx_seq;
    tx.groups.push_back(
      tx_metadata::tx_group{.group_id = group_id, .etag = etag});
    auto result = co_await update_tx(std::move(tx), expected_term);
    if (result.has_error()) {
        co_return result.error();
    }

    vlog(
      _ctx_log.trace,
      "[tx_id={}] transaction: {} added with etag: {}",
      tx_id,
      result.value(),
      expected_term);

    co_return tm_stm::op_status::success;
}
void tm_stm::upsert_transaction(tx_metadata tx) {
    auto [tx_it, inserted] = _transactions.try_emplace(tx.id, tx);
    _pid_tx_id[tx.pid] = tx.id;
    _pid_tx_id.erase(tx.last_pid);
    if (!inserted) {
        tx_it->second.tx = std::move(tx);
    }
    tx_it->second._hook.unlink();
    _transactions_lru.push_back(tx_it->second);
}

fragmented_vector<tx_metadata> tm_stm::get_transactions_list() const {
    fragmented_vector<tx_metadata> ret;
    ret.reserve(_transactions.size());
    for (const auto& [_, wrapper] : _transactions) {
        ret.push_back(wrapper.tx);
    }
    return ret;
}

ss::future<>
tm_stm::apply_local_snapshot(raft::stm_snapshot_header hdr, iobuf&& tm_ss_buf) {
    vassert(
      hdr.version >= tm_snapshot_v0::version
        && hdr.version <= tm_snapshot::version,
      "unsupported seq_snapshot_header version {}",
      hdr.version);
    iobuf_parser data_parser(std::move(tm_ss_buf));
    _transactions.clear();
    _pid_tx_id.clear();
    if (hdr.version == tm_snapshot_v0::version) {
        auto data = reflection::adl<tm_snapshot_v0>{}.from(data_parser);
        for (auto& entry : data.transactions) {
            upsert_transaction(entry);
        }
    } else if (hdr.version == tm_snapshot::version) {
        auto data = reflection::adl<tm_snapshot>{}.from(data_parser);

        for (auto& entry : data.transactions) {
            upsert_transaction(entry);
        }

        vlog(_ctx_log.trace, "Applied snapshot at offset: {}", hdr.offset);
    }

    return ss::now();
}

ss::future<raft::stm_snapshot>
tm_stm::take_local_snapshot(ssx::semaphore_units apply_units) {
    auto snapshot_version = active_snapshot_version();
    auto snapshot_offset = last_applied_offset();
    if (snapshot_version == tm_snapshot_v0::version) {
        tm_snapshot_v0 tm_ss;
        tm_ss.offset = snapshot_offset;
        tm_ss.transactions = get_transactions_list();

        iobuf tm_ss_buf;
        reflection::adl<tm_snapshot_v0>{}.to(tm_ss_buf, std::move(tm_ss));

        apply_units.return_all();
        co_return raft::stm_snapshot::create(
          tm_snapshot_v0::version, snapshot_offset, std::move(tm_ss_buf));
    } else {
        tm_snapshot tm_ss;
        tm_ss.offset = snapshot_offset;
        tm_ss.transactions = get_transactions_list();

        iobuf tm_ss_buf;
        reflection::adl<tm_snapshot>{}.to(tm_ss_buf, std::move(tm_ss));

        apply_units.return_all();
        co_return raft::stm_snapshot::create(
          tm_snapshot::version, snapshot_offset, std::move(tm_ss_buf));
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

    tx_metadata tx;
    switch (version) {
    case transaction_metadata_v0::version: {
        auto tx0 = reflection::adl<transaction_metadata_v0>{}.from(val_reader);
        tx = tx0.upcast();
        break;
    }
    case transaction_metadata_v1::version: {
        auto tx1 = reflection::adl<transaction_metadata_v1>{}.from(val_reader);
        tx = tx1.upcast();
        break;
    }
    default: {
        vassert(
          version == tx_metadata::version,
          "unknown group inflight tx record version: {} expected: {}",
          version,
          tx_metadata::version);
        tx = reflection::adl<tx_metadata>{}.from(val_reader);
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
    vlog(_ctx_log.trace, "[tx_id={}] applying transaction: {}", tx.id, tx);

    if (tx.status == tx_status::tombstone) {
        _transactions.erase(tx.id);
        vlog(
          _ctx_log.trace,
          "[tx_id={}] erasing (tombstone) transaction: {} in term: {} from "
          "memory",
          tx.id,
          tx,
          _insync_term);
        _pid_tx_id.erase(tx.pid);
        return ss::now();
    }
    // NOTE: currently we do not validate the transaction state on apply, for
    // now this is fine as there was no validation on apply in the first place
    // before the refactoring happened. We will add validation after we will
    // make sure the transaction FSM transitions are all valid
    upsert_transaction(tx);

    return ss::now();
}

ss::future<> tm_stm::do_apply(const model::record_batch& b) {
    const auto& hdr = b.header();

    if (hdr.type == model::record_batch_type::tm_update) {
        return apply_tm_update(hdr, b.copy());
    }

    return ss::now();
}

bool tm_stm::is_expired(const tx_metadata& tx) {
    auto now_ts = clock_type::now();
    return _transactional_id_expiration() < now_ts - tx.last_update_ts;
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

std::optional<txlock_unit> tm_stm::try_lock_tx(
  const kafka::transactional_id& tx_id, std::string_view lock_name) {
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
    auto now = clock_type::now();
    auto is_expired = [this,
                       now](const decltype(_transactions)::value_type& p) {
        return p.second.tx.last_update_ts + _transactional_id_expiration()
               < now;
    };
    auto filtered = _transactions | std::views::filter(is_expired)
                    | std::views::keys;

    return {filtered.begin(), filtered.end()};
}

ss::future<tm_stm::get_txs_result> tm_stm::get_all_transactions() {
    if (!_raft->is_leader()) {
        co_return tm_stm::op_status::not_leader;
    }

    auto is_ready = co_await sync(_sync_timeout);
    if (!is_ready) {
        co_return tm_stm::op_status::unknown;
    }

    co_return get_transactions_list();
}

size_t tm_stm::tx_cache_size() const { return _transactions.size(); }

std::optional<tx_metadata> tm_stm::oldest_tx() const {
    if (_transactions_lru.empty()) {
        return std::nullopt;
    }

    return _transactions_lru.front().tx;
}

ss::future<checked<tx_metadata, tm_stm::op_status>>
tm_stm::delete_partition_from_tx(
  model::term_id term,
  kafka::transactional_id tid,
  tx_metadata::tx_partition ntp) {
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
    co_return co_await update_tx(std::move(tx), term);
}

ss::future<tm_stm::op_status>
tm_stm::expire_tx(model::term_id term, kafka::transactional_id tx_id) {
    auto tx_opt = co_await get_tx(tx_id);
    if (!tx_opt.has_value()) {
        co_return tm_stm::op_status::unknown;
    }
    tx_metadata tx = tx_opt.value();
    tx.etag = term;
    tx.status = tx_status::tombstone;
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
    return _state_lock.hold_write_lock().then(
      [this]([[maybe_unused]] ss::basic_rwlock<>::holder unit) {
          _transactions.clear();
          _pid_tx_id.clear();
      });
}

tm_stm_factory::tm_stm_factory(
  ss::sharded<features::feature_table>& feature_table)
  : _feature_table(feature_table) {}

bool tm_stm_factory::is_applicable_for(const storage::ntp_config& cfg) const {
    const auto& ntp = cfg.ntp();
    return ntp.ns == model::kafka_internal_namespace
           && ntp.tp.topic == model::tx_manager_topic;
}

void tm_stm_factory::create(
  raft::state_machine_manager_builder& builder, raft::consensus* raft) {
    auto tm_stm = builder.create_stm<cluster::tm_stm>(
      txlog, raft, _feature_table);
    raft->log()->stm_manager()->add_stm(tm_stm);
}

} // namespace cluster

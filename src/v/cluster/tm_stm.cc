// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cluster/tm_stm.h"

#include "cluster/logger.h"
#include "cluster/types.h"
#include "raft/errc.h"
#include "raft/types.h"
#include "storage/record_batch_builder.h"
#include "units.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/future.hh>

#include <filesystem>
#include <optional>

namespace cluster {

static model::record_batch serialize_tx(tm_transaction tx) {
    iobuf key;
    reflection::serialize(key, model::record_batch_type::tm_update);
    auto pid_id = tx.pid.id;
    auto tx_id = tx.id;
    reflection::serialize(key, pid_id, tx_id);

    iobuf value;
    reflection::serialize(value, tm_transaction::version);
    reflection::serialize(value, std::move(tx));

    storage::record_batch_builder b(
      model::record_batch_type::tm_update, model::offset(0));
    b.add_raw_kv(std::move(key), std::move(value));
    return std::move(b).build();
}

std::ostream& operator<<(std::ostream& o, const tm_transaction& tx) {
    return o << "{tm_transaction: id=" << tx.id << ", status=" << tx.status
             << ", pid=" << tx.pid << ", etag=" << tx.etag
             << ", size(partitions)=" << tx.partitions.size()
             << ", tx_seq=" << tx.tx_seq << "}";
}

tm_stm::tm_stm(ss::logger& logger, raft::consensus* c)
  : persisted_stm("tx.coordinator.snapshot", logger, c)
  , _sync_timeout(config::shard_local_cfg().tm_sync_timeout_ms.value())
  , _transactional_id_expiration(
      config::shard_local_cfg().transactional_id_expiration_ms.value())
  , _recovery_policy(
      config::shard_local_cfg().tm_violation_recovery_policy.value()) {}

std::optional<tm_transaction> tm_stm::get_tx(kafka::transactional_id tx_id) {
    auto tx = _mem_txes.find(tx_id);
    if (tx != _mem_txes.end()) {
        return tx->second;
    }
    tx = _log_txes.find(tx_id);
    if (tx != _log_txes.end()) {
        return tx->second;
    }
    return std::nullopt;
}

ss::future<checked<model::term_id, tm_stm::op_status>> tm_stm::barrier() {
    if (!_c->is_leader()) {
        return ss::make_ready_future<
          checked<model::term_id, tm_stm::op_status>>(
          tm_stm::op_status::not_leader);
    }
    if (_insync_term != _c->term()) {
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
                if (term != _c->term()) {
                    return tm_stm::op_status::unknown;
                }
                return term;
            } catch (...) {
                vlog(
                  clusterlog.error,
                  "Error during writing a barrier batch: {}",
                  std::current_exception());
                return tm_stm::op_status::unknown;
            }
        });
}

ss::future<checked<model::term_id, tm_stm::op_status>>
tm_stm::sync(model::timeout_clock::duration timeout) {
    if (!_c->is_leader()) {
        co_return tm_stm::op_status::not_leader;
    }

    auto old_term = _insync_term;
    auto ready = co_await persisted_stm::sync(timeout);
    if (!ready) {
        _mem_txes.clear();
        co_return tm_stm::op_status::unknown;
    }
    if (old_term != _insync_term) {
        _mem_txes.clear();
    }
    co_return _insync_term;
}

ss::future<checked<tm_transaction, tm_stm::op_status>>
tm_stm::update_tx(tm_transaction tx, model::term_id term) {
    auto batch = serialize_tx(tx);

    auto r = co_await replicate_quorum_ack(term, std::move(batch));
    if (!r) {
        co_return tm_stm::op_status::unknown;
    }

    if (!co_await wait_no_throw(
          model::offset(r.value().last_offset()), _sync_timeout)) {
        co_return tm_stm::op_status::unknown;
    }
    if (_c->term() != term) {
        // we lost leadership during waiting
        co_return tm_stm::op_status::unknown;
    }

    auto tx_opt = get_tx(tx.id);
    if (!tx_opt.has_value()) {
        co_return tm_stm::op_status::conflict;
    }
    co_return tx_opt.value();
}

ss::future<checked<tm_transaction, tm_stm::op_status>>
tm_stm::mark_tx_preparing(
  model::term_id expected_term, kafka::transactional_id tx_id) {
    auto ptx = _mem_txes.find(tx_id);
    if (ptx == _mem_txes.end()) {
        co_return tm_stm::op_status::not_found;
    }
    auto tx = ptx->second;
    if (tx.status != tm_transaction::tx_status::ongoing) {
        co_return tm_stm::op_status::conflict;
    }
    tx.status = cluster::tm_transaction::tx_status::preparing;
    tx.last_update_ts = clock_type::now();
    co_return co_await update_tx(std::move(tx), expected_term);
}

ss::future<checked<tm_transaction, tm_stm::op_status>> tm_stm::mark_tx_aborting(
  model::term_id expected_term, kafka::transactional_id tx_id) {
    auto ptx = _mem_txes.find(tx_id);
    if (ptx == _mem_txes.end()) {
        co_return tm_stm::op_status::not_found;
    }
    auto tx = ptx->second;
    if (tx.status != tm_transaction::tx_status::ongoing) {
        co_return tm_stm::op_status::conflict;
    }
    tx.status = cluster::tm_transaction::tx_status::aborting;
    tx.last_update_ts = clock_type::now();
    co_return co_await update_tx(std::move(tx), expected_term);
}

ss::future<checked<tm_transaction, tm_stm::op_status>> tm_stm::mark_tx_prepared(
  model::term_id expected_term, kafka::transactional_id tx_id) {
    auto tx_opt = get_tx(tx_id);
    if (!tx_opt.has_value()) {
        co_return tm_stm::op_status::not_found;
    }
    auto tx = tx_opt.value();
    if (tx.status != tm_transaction::tx_status::preparing) {
        co_return tm_stm::op_status::conflict;
    }
    tx.status = cluster::tm_transaction::tx_status::prepared;
    tx.last_update_ts = clock_type::now();
    co_return co_await update_tx(std::move(tx), expected_term);
}

ss::future<checked<tm_transaction, tm_stm::op_status>> tm_stm::mark_tx_killed(
  model::term_id expected_term, kafka::transactional_id tx_id) {
    auto tx_opt = get_tx(tx_id);
    if (!tx_opt.has_value()) {
        co_return tm_stm::op_status::not_found;
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

ss::future<checked<tm_transaction, tm_stm::op_status>> tm_stm::reset_tx_ready(
  model::term_id expected_term, kafka::transactional_id tx_id) {
    return reset_tx_ready(expected_term, tx_id, expected_term);
}

ss::future<checked<tm_transaction, tm_stm::op_status>> tm_stm::reset_tx_ready(
  model::term_id expected_term,
  kafka::transactional_id tx_id,
  model::term_id term) {
    auto tx_opt = get_tx(tx_id);
    if (!tx_opt.has_value()) {
        co_return tm_stm::op_status::not_found;
    }
    tm_transaction tx = tx_opt.value();
    tx.status = tm_transaction::tx_status::ready;
    tx.partitions.clear();
    tx.groups.clear();
    tx.etag = term;
    tx.last_update_ts = clock_type::now();
    co_return co_await update_tx(std::move(tx), expected_term);
}

checked<tm_transaction, tm_stm::op_status>
tm_stm::mark_tx_ongoing(kafka::transactional_id tx_id) {
    auto tx_opt = get_tx(tx_id);
    if (!tx_opt.has_value()) {
        return tm_stm::op_status::not_found;
    }
    tm_transaction tx = tx_opt.value();
    tx.status = tm_transaction::tx_status::ongoing;
    tx.tx_seq += 1;
    tx.partitions.clear();
    tx.groups.clear();
    tx.last_update_ts = clock_type::now();
    _mem_txes[tx_id] = tx;
    return tx;
}

checked<tm_transaction, tm_stm::op_status>
tm_stm::reset_tx_ongoing(kafka::transactional_id tx_id, model::term_id term) {
    auto tx_opt = get_tx(tx_id);
    if (!tx_opt.has_value()) {
        return tm_stm::op_status::not_found;
    }
    tm_transaction tx = tx_opt.value();
    tx.status = tm_transaction::tx_status::ongoing;
    tx.tx_seq += 1;
    tx.partitions.clear();
    tx.groups.clear();
    tx.last_update_ts = clock_type::now();
    tx.etag = term;
    _mem_txes[tx_id] = tx;
    return tx;
}

ss::future<tm_stm::op_status> tm_stm::re_register_producer(
  model::term_id expected_term,
  kafka::transactional_id tx_id,
  std::chrono::milliseconds transaction_timeout_ms,
  model::producer_identity pid) {
    vlog(
      clusterlog.trace, "Registering existing tx: id={}, pid={}", tx_id, pid);

    auto tx_opt = get_tx(tx_id);
    if (!tx_opt.has_value()) {
        co_return tm_stm::op_status::not_found;
    }
    tm_transaction tx = tx_opt.value();
    tx.status = tm_transaction::tx_status::ready;
    tx.pid = pid;
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
    co_return tm_stm::op_status::success;
}

ss::future<tm_stm::op_status> tm_stm::register_new_producer(
  model::term_id expected_term,
  kafka::transactional_id tx_id,
  std::chrono::milliseconds transaction_timeout_ms,
  model::producer_identity pid) {
    vlog(clusterlog.trace, "Registering new tx: id={}, pid={}", tx_id, pid);

    auto tx_opt = get_tx(tx_id);
    if (tx_opt.has_value()) {
        co_return tm_stm::op_status::conflict;
    }

    auto tx = tm_transaction{
      .id = tx_id,
      .pid = pid,
      .tx_seq = model::tx_seq(0),
      .etag = expected_term,
      .status = tm_transaction::tx_status::ready,
      .timeout_ms = transaction_timeout_ms,
      .last_update_ts = clock_type::now()};
    auto batch = serialize_tx(tx);

    _pid_tx_id[pid] = tx_id;

    auto r = co_await replicate_quorum_ack(expected_term, std::move(batch));

    if (!r) {
        co_return tm_stm::op_status::unknown;
    }

    if (!co_await wait_no_throw(
          model::offset(r.value().last_offset()), _sync_timeout)) {
        co_return tm_stm::op_status::unknown;
    }
    if (_c->term() != expected_term) {
        // we lost leadership during waiting
        co_return tm_stm::op_status::unknown;
    }

    co_return tm_stm::op_status::success;
}

bool tm_stm::add_partitions(
  kafka::transactional_id tx_id,
  std::vector<tm_transaction::tx_partition> partitions) {
    auto ptx = _mem_txes.find(tx_id);
    if (ptx == _mem_txes.end()) {
        return false;
    }
    if (ptx->second.status != tm_transaction::tx_status::ongoing) {
        return false;
    }
    for (auto& partition : partitions) {
        ptx->second.partitions.push_back(partition);
    }
    ptx->second.last_update_ts = clock_type::now();
    return true;
}

bool tm_stm::add_group(
  kafka::transactional_id tx_id,
  kafka::group_id group_id,
  model::term_id term) {
    auto ptx = _mem_txes.find(tx_id);
    if (ptx == _mem_txes.end()) {
        return false;
    }
    if (ptx->second.status != tm_transaction::tx_status::ongoing) {
        return false;
    }
    ptx->second.groups.push_back(
      tm_transaction::tx_group{.group_id = group_id, .etag = term});
    ptx->second.last_update_ts = clock_type::now();
    return true;
}

ss::future<>
tm_stm::apply_snapshot(stm_snapshot_header hdr, iobuf&& tm_ss_buf) {
    vassert(
      hdr.version == supported_version,
      "unsupported seq_snapshot_header version {}",
      hdr.version);
    iobuf_parser data_parser(std::move(tm_ss_buf));
    auto data = reflection::adl<tm_snapshot>{}.from(data_parser);

    _mem_txes.clear();
    _log_txes.clear();
    for (auto& entry : data.transactions) {
        _log_txes[entry.id] = entry;
        _pid_tx_id[entry.pid] = entry.id;
    }
    _last_snapshot_offset = data.offset;
    _insync_offset = data.offset;

    return ss::now();
}

ss::future<stm_snapshot> tm_stm::take_snapshot() {
    tm_snapshot tm_ss;
    tm_ss.offset = _insync_offset;
    for (auto& entry : _log_txes) {
        tm_ss.transactions.push_back(entry.second);
    }

    iobuf tm_ss_buf;
    reflection::adl<tm_snapshot>{}.to(tm_ss_buf, tm_ss);

    co_return stm_snapshot::create(
      supported_version, _insync_offset, std::move(tm_ss_buf));
}

ss::future<> tm_stm::apply(model::record_batch b) {
    const auto& hdr = b.header();
    _insync_offset = b.last_offset();

    if (hdr.type != model::record_batch_type::tm_update) {
        return ss::now();
    }

    vassert(
      b.record_count() == 1,
      "model::record_batch_type::tm_update batch must contain a single record");
    auto r = b.copy_records();
    auto& record = *r.begin();
    auto val_buf = record.release_value();

    iobuf_parser val_reader(std::move(val_buf));
    auto version = reflection::adl<int8_t>{}.from(val_reader);

    tm_transaction tx;
    if (version == tm_transaction_v0::version) {
        auto tx0 = reflection::adl<tm_transaction_v0>{}.from(val_reader);
        tx = tx0.upcast();
    } else {
        vassert(
          version == tm_transaction::version,
          "unknown group inflight tx record version: {} expected: {}",
          version,
          tm_transaction::version);
        tx = reflection::adl<tm_transaction>{}.from(val_reader);
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

    if (tx.status == tm_transaction::tx_status::tombstone) {
        _log_txes.erase(tx.id);
        _mem_txes.erase(tx.id);
        _tx_locks.erase(tx.id);
        _pid_tx_id.erase(tx.pid);
        return ss::now();
    }

    auto it = _mem_txes.find(tx.id);
    if (it != _mem_txes.end()) {
        if (it->second.etag < tx.etag) {
            _mem_txes.erase(tx.id);
        } else if (it->second.etag == tx.etag) {
            if (it->second.tx_seq <= tx.tx_seq) {
                _mem_txes.erase(tx.id);
            }
        }
    }

    _log_txes[tx.id] = tx;
    _pid_tx_id[tx.pid] = tx.id;

    return ss::now();
}

bool tm_stm::is_expired(const tm_transaction& tx) {
    auto now_ts = clock_type::now();
    return _transactional_id_expiration < now_ts - tx.last_update_ts;
}

absl::btree_set<kafka::transactional_id> tm_stm::get_expired_txs() {
    auto now_ts = clock_type::now();
    absl::btree_set<kafka::transactional_id> ids;
    for (auto& [id, tx] : _log_txes) {
        if (_transactional_id_expiration < now_ts - tx.last_update_ts) {
            ids.insert(id);
        }
    }
    for (auto& [id, tx] : _mem_txes) {
        if (_transactional_id_expiration < now_ts - tx.last_update_ts) {
            ids.insert(id);
        }
    }
    return ids;
}

ss::future<tm_stm::get_txs_result> tm_stm::get_all_transactions() {
    if (!_c->is_leader()) {
        co_return tm_stm::op_status::not_leader;
    }

    auto is_ready = co_await sync(_sync_timeout);
    if (!is_ready) {
        co_return tm_stm::op_status::unknown;
    }

    std::vector<tm_transaction> ans;
    for (const auto& [_, tx] : _mem_txes) {
        ans.push_back(tx);
    }

    for (const auto& [id, tx] : _log_txes) {
        auto it = _mem_txes.find(id);
        if (it == _mem_txes.end()) {
            ans.push_back(tx);
        }
    }

    co_return ans;
}

ss::future<checked<tm_transaction, tm_stm::op_status>>
tm_stm::delete_partition_from_tx(
  model::term_id term,
  kafka::transactional_id tid,
  tm_transaction::tx_partition ntp) {
    if (!_c->is_leader()) {
        co_return tm_stm::op_status::not_leader;
    }

    auto optional_tx = get_tx(tid);
    if (!optional_tx.has_value()) {
        co_return tm_stm::op_status::not_found;
    }

    auto tx = optional_tx.value();

    auto res = tx.delete_partition(ntp);
    if (!res) {
        co_return tm_stm::op_status::partition_not_found;
    }

    if (tx.status == tm_transaction::tx_status::ongoing) {
        _mem_txes.insert_or_assign(tid, tx);
        co_return tx;
    } else {
        co_return co_await update_tx(std::move(tx), term);
    }
}

ss::future<> tm_stm::expire_tx(kafka::transactional_id tx_id) {
    auto tx_opt = get_tx(tx_id);
    if (!tx_opt.has_value()) {
        co_return;
    }
    tm_transaction tx = tx_opt.value();
    tx.etag = _insync_term;
    tx.status = tm_transaction::tx_status::tombstone;
    tx.partitions.clear();
    tx.groups.clear();
    tx.last_update_ts = clock_type::now();
    auto etag = tx.etag;
    co_await update_tx(std::move(tx), etag).discard_result();
}

ss::future<> tm_stm::handle_eviction() {
    return _state_lock.hold_write_lock().then(
      [this]([[maybe_unused]] ss::basic_rwlock<>::holder unit) {
          _log_txes.clear();
          _mem_txes.clear();
          _pid_tx_id.clear();
          set_next(_c->start_offset());
          return ss::now();
      });
}

} // namespace cluster

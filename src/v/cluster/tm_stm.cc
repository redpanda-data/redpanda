// Copyright 2020 Vectorized, Inc.
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

template<typename T>
static model::record_batch serialize_cmd(T t, model::record_batch_type type) {
    storage::record_batch_builder b(type, model::offset(0));
    iobuf key_buf;
    reflection::adl<uint8_t>{}.to(key_buf, T::record_key);
    iobuf v_buf;
    reflection::adl<T>{}.to(v_buf, std::move(t));
    b.add_raw_kv(std::move(key_buf), std::move(v_buf));
    return std::move(b).build();
}

std::ostream& operator<<(std::ostream& o, const tm_etag& etag) {
    return o << "{tm_etag: log_etag=" << etag.log_etag
             << ", mem_etag=" << etag.mem_etag << "}";
}

std::ostream& operator<<(std::ostream& o, const tm_transaction& tx) {
    return o << "{tm_transaction: id=" << tx.id << ", status=" << tx.status
             << ", pid=" << tx.pid
             << ", size(partitions)=" << tx.partitions.size()
             << ", etag=" << tx.etag << ", tx_seq=" << tx.tx_seq << "}";
}

tm_stm::tm_stm(ss::logger& logger, raft::consensus* c)
  : persisted_stm("tm", logger, c)
  , _sync_timeout(config::shard_local_cfg().tm_sync_timeout_ms.value())
  , _recovery_policy(
      config::shard_local_cfg().tm_violation_recovery_policy.value()) {}

std::optional<tm_transaction> tm_stm::get_tx(kafka::transactional_id tx_id) {
    auto tx = _tx_table.find(tx_id);
    if (tx != _tx_table.end()) {
        return tx->second;
    }
    return std::nullopt;
}

ss::future<checked<tm_transaction, tm_stm::op_status>>
tm_stm::save_tx(model::term_id term, tm_transaction tx) {
    auto ptx = _tx_table.find(tx.id);
    if (ptx == _tx_table.end()) {
        co_return tm_stm::op_status::not_found;
    }
    if (ptx->second.etag != tx.etag) {
        co_return tm_stm::op_status::conflict;
    }

    auto etag = tx.etag;
    tx.etag = etag.inc_log();
    tx_updated_cmd cmd{.tx = tx, .prev_etag = etag};
    auto batch = serialize_cmd(cmd, tm_update_batch_type);

    auto r = co_await replicate_quorum_ack(term, std::move(batch));
    if (!r) {
        co_return tm_stm::op_status::unknown;
    }

    if (!co_await wait_no_throw(
          model::offset(r.value().last_offset()), _sync_timeout)) {
        co_return tm_stm::op_status::unknown;
    }

    ptx = _tx_table.find(tx.id);
    if (ptx == _tx_table.end()) {
        co_return tm_stm::op_status::conflict;
    }
    if (ptx->second.etag != tx.etag) {
        co_return tm_stm::op_status::conflict;
    }
    co_return ptx->second;
}

ss::future<checked<tm_transaction, tm_stm::op_status>>
tm_stm::try_change_status(
  kafka::transactional_id tx_id,
  tm_etag etag,
  tm_transaction::tx_status status) {
    auto is_ready = co_await sync(_sync_timeout);
    if (!is_ready) {
        co_return tm_stm::op_status::unknown;
    }

    auto term = _insync_term;
    auto ptx = _tx_table.find(tx_id);
    if (ptx == _tx_table.end()) {
        co_return tm_stm::op_status::not_found;
    }
    if (ptx->second.etag != etag) {
        co_return tm_stm::op_status::conflict;
    }
    auto tx = ptx->second;
    tx.status = status;
    auto lock_it = _tx_locks.find(tx_id);
    vassert(
      lock_it != _tx_locks.end(),
      "Existence of _tx_table[{}] must emply existence of _tx_locks[{}]",
      tx_id,
      tx_id);

    // hack: https://gcc.gnu.org/bugzilla/show_bug.cgi?id=95599
    auto func = [this, term, tx]() { return save_tx(term, tx); };
    co_return co_await lock_it->second->with(func);
}

checked<tm_transaction, tm_stm::op_status>
tm_stm::mark_tx_finished(kafka::transactional_id tx_id, tm_etag etag) {
    auto ptx = _tx_table.find(tx_id);
    if (ptx == _tx_table.end()) {
        return tm_stm::op_status::not_found;
    }
    if (ptx->second.etag != etag) {
        return tm_stm::op_status::conflict;
    }
    ptx->second.status = tm_transaction::tx_status::finished;
    ptx->second.etag = etag.inc_mem();
    ptx->second.partitions.clear();
    return ptx->second;
}

checked<tm_transaction, tm_stm::op_status>
tm_stm::mark_tx_ongoing(kafka::transactional_id tx_id, tm_etag etag) {
    auto ptx = _tx_table.find(tx_id);
    if (ptx == _tx_table.end()) {
        return tm_stm::op_status::not_found;
    }
    if (ptx->second.etag != etag) {
        return tm_stm::op_status::conflict;
    }
    ptx->second.status = tm_transaction::tx_status::ongoing;
    ptx->second.etag = etag.inc_mem();
    ptx->second.tx_seq += 1;
    ptx->second.partitions.clear();
    return ptx->second;
}

ss::future<tm_stm::op_status> tm_stm::re_register_producer(
  kafka::transactional_id tx_id, tm_etag etag, model::producer_identity pid) {
    auto is_ready = co_await sync(_sync_timeout);
    if (!is_ready) {
        co_return tm_stm::op_status::unknown;
    }

    vlog(
      clusterlog.trace,
      "Registering existing tx: id={}, pid={}, etag={}",
      tx_id,
      pid,
      etag);

    auto term = _insync_term;
    auto ptx = _tx_table.find(tx_id);
    if (ptx == _tx_table.end()) {
        co_return tm_stm::op_status::not_found;
    }
    if (ptx->second.etag != etag) {
        co_return tm_stm::op_status::conflict;
    }
    tm_transaction tx = ptx->second;
    tx.status = tm_transaction::tx_status::ongoing;
    tx.pid = pid;
    tx.tx_seq += 1;
    tx.partitions.clear();

    // hack: https://gcc.gnu.org/bugzilla/show_bug.cgi?id=95599
    auto func = [this, term, tx]() { return save_tx(term, tx); };
    auto r = co_await _tx_locks.find(tx_id)->second->with(func);

    if (!r.has_value()) {
        co_return tm_stm::op_status::unknown;
    }
    co_return tm_stm::op_status::success;
}

ss::future<tm_stm::op_status> tm_stm::register_new_producer(
  kafka::transactional_id tx_id, model::producer_identity pid) {
    auto is_ready = co_await sync(_sync_timeout);
    if (!is_ready) {
        co_return tm_stm::op_status::unknown;
    }

    vlog(clusterlog.trace, "Registering new tx: id={}, pid={}", tx_id, pid);

    auto term = _insync_term;
    auto ptx = _tx_table.find(tx_id);
    if (ptx != _tx_table.end()) {
        co_return tm_stm::op_status::conflict;
    }
    _end_locks.try_emplace(tx_id, ss::make_lw_shared<mutex>());
    auto [lock_it, _] = _tx_locks.try_emplace(
      tx_id, ss::make_lw_shared<mutex>());

    // hack: https://gcc.gnu.org/bugzilla/show_bug.cgi?id=95599
    auto func = [this, term, tx_id, pid]() {
        return register_new_producer(term, tx_id, pid);
    };
    co_return co_await lock_it->second->with(func);
}

ss::future<tm_stm::op_status> tm_stm::register_new_producer(
  model::term_id term,
  kafka::transactional_id tx_id,
  model::producer_identity pid) {
    auto ptx = _tx_table.find(tx_id);
    if (ptx != _tx_table.end()) {
        co_return tm_stm::op_status::conflict;
    }

    auto tx = tm_transaction{
      .id = tx_id,
      .pid = pid,
      .tx_seq = model::tx_seq(0),
      .status = tm_transaction::tx_status::ongoing,
    };
    tx_updated_cmd cmd{.tx = tx, .prev_etag = tx.etag};
    auto batch = serialize_cmd(cmd, tm_update_batch_type);

    auto r = co_await replicate_quorum_ack(term, std::move(batch));

    if (!r) {
        co_return tm_stm::op_status::unknown;
    }

    if (!co_await wait_no_throw(
          model::offset(r.value().last_offset()), _sync_timeout)) {
        co_return tm_stm::op_status::unknown;
    }

    ptx = _tx_table.find(tx_id);
    if (ptx == _tx_table.end()) {
        co_return tm_stm::op_status::unknown;
    }
    if (ptx->second.etag != tx.etag) {
        co_return tm_stm::op_status::conflict;
    }
    co_return tm_stm::op_status::success;
}

bool tm_stm::add_partitions(
  kafka::transactional_id tx_id,
  tm_etag etag,
  std::vector<tm_transaction::tx_partition> partitions) {
    auto ptx = _tx_table.find(tx_id);
    if (ptx == _tx_table.end()) {
        return false;
    }
    if (ptx->second.etag != etag) {
        return false;
    }
    ptx->second.etag = etag.inc_mem();
    for (auto& partition : partitions) {
        ptx->second.partitions.push_back(partition);
    }
    return true;
}

void tm_stm::expire_old_txs() {
    // TODO: expiration of old transactions
}

void tm_stm::load_snapshot(stm_snapshot_header hdr, iobuf&& tm_ss_buf) {
    vassert(
      hdr.version == supported_version,
      "unsupported seq_snapshot_header version {}",
      hdr.version);
    iobuf_parser data_parser(std::move(tm_ss_buf));
    auto data = reflection::adl<tm_snapshot>{}.from(data_parser);

    for (auto& entry : data.transactions) {
        _tx_table.try_emplace(entry.id, entry);
        _tx_locks.try_emplace(entry.id, ss::make_lw_shared<mutex>());
        _end_locks.try_emplace(entry.id, ss::make_lw_shared<mutex>());
    }
    _last_snapshot_offset = data.offset;
    _insync_offset = data.offset;
}

stm_snapshot tm_stm::take_snapshot() {
    tm_snapshot tm_ss;
    tm_ss.offset = _insync_offset;
    for (auto& entry : _tx_table) {
        tm_ss.transactions.push_back(entry.second);
    }

    iobuf tm_ss_buf;
    reflection::adl<tm_snapshot>{}.to(tm_ss_buf, tm_ss);

    stm_snapshot_header header;
    header.version = supported_version;
    header.snapshot_size = tm_ss_buf.size_bytes();

    stm_snapshot stm_ss;
    stm_ss.header = header;
    stm_ss.offset = _insync_offset;
    stm_ss.data = std::move(tm_ss_buf);
    return stm_ss;
}

ss::future<> tm_stm::apply(model::record_batch b) {
    const auto& hdr = b.header();
    _insync_offset = b.last_offset();

    if (hdr.type != tm_update_batch_type) {
        return ss::now();
    }

    vassert(
      b.record_count() == 1,
      "We expect single command in a batch of tm_update_batch_type");
    auto r = b.copy_records();
    auto& record = *r.begin();
    auto rk = reflection::adl<uint8_t>{}.from(record.release_key());

    vassert(
      rk == tx_updated_cmd::record_key, "Unknown tm update command: {}", rk);
    tx_updated_cmd cmd = reflection::adl<tx_updated_cmd>{}.from(
      record.release_value());

    auto ptx = _tx_table.find(cmd.tx.id);
    if (ptx == _tx_table.end()) {
        if (cmd.prev_etag != cmd.tx.etag) {
            vlog(
              clusterlog.error,
              "Inconsistent tm log. First command should depend on itself but "
              "tx.id={}, etag={} it reffers to {}",
              cmd.tx.id,
              cmd.tx.etag,
              cmd.prev_etag);
            if (_recovery_policy == model::violation_recovery_policy::crash) {
                vassert(
                  false, "Crushing to prevent potential consistency violation");
            } else if (
              _recovery_policy
              == model::violation_recovery_policy::best_effort) {
                vlog(
                  clusterlog.error,
                  "Recovering by blindly applying tx: {}",
                  cmd.tx);
            } else {
                vassert(false, "Unknown recovery policy {}", _recovery_policy);
            }
        }
        _tx_table.try_emplace(cmd.tx.id, cmd.tx);
        _end_locks.try_emplace(cmd.tx.id, ss::make_lw_shared<mutex>());
        _tx_locks.try_emplace(cmd.tx.id, ss::make_lw_shared<mutex>());
    } else {
        if (ptx->second.etag.log_etag == cmd.prev_etag.log_etag) {
            ptx->second = cmd.tx;
        }
    }

    expire_old_txs();

    return ss::now();
}

} // namespace cluster

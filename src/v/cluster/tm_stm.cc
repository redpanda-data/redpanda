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
             << ", pid=" << tx.pid
             << ", size(partitions)=" << tx.partitions.size()
             << ", tx_seq=" << tx.tx_seq << "}";
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

ss::future<bool> tm_stm::barrier() {
    if (_insync_term != _c->term()) {
        return ss::make_ready_future<bool>(false);
    }
    auto term = _c->term();
    return quorum_write_empty_batch(model::timeout_clock::now() + _sync_timeout)
      .then_wrapped([this, term](ss::future<result<raft::replicate_result>> f) {
          try {
              if (!f.get0().has_value()) {
                  return false;
              }
              if (term != _c->term()) {
                  return false;
              }
              return true;
          } catch (...) {
              vlog(
                clusterlog.error,
                "Error during writing a barrier batch: {}",
                std::current_exception());
              return false;
          }
      });
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

    auto ptx = _tx_table.find(tx.id);
    if (ptx == _tx_table.end()) {
        co_return tm_stm::op_status::conflict;
    }
    co_return ptx->second;
}

ss::future<checked<tm_transaction, tm_stm::op_status>>
tm_stm::try_change_status(
  kafka::transactional_id tx_id, tm_transaction::tx_status status) {
    auto is_ready = co_await sync(_sync_timeout);
    if (!is_ready) {
        co_return tm_stm::op_status::unknown;
    }

    auto ptx = _tx_table.find(tx_id);
    if (ptx == _tx_table.end()) {
        co_return tm_stm::op_status::not_found;
    }
    tm_transaction tx = ptx->second;
    tx.etag = _insync_term;
    tx.status = status;
    co_return co_await update_tx(tx, tx.etag);
}

ss::future<std::optional<tm_transaction>>
tm_stm::get_actual_tx(kafka::transactional_id tx_id) {
    auto is_ready = co_await sync(_sync_timeout);
    if (!is_ready) {
        co_return std::nullopt;
    }

    auto tx = _tx_table.find(tx_id);
    if (tx != _tx_table.end()) {
        co_return tx->second;
    }

    co_return std::nullopt;
}

ss::future<checked<tm_transaction, tm_stm::op_status>>
tm_stm::mark_tx_ready(kafka::transactional_id tx_id) {
    return mark_tx_ready(tx_id, _insync_term);
}

ss::future<checked<tm_transaction, tm_stm::op_status>>
tm_stm::mark_tx_ready(kafka::transactional_id tx_id, model::term_id term) {
    auto ptx = _tx_table.find(tx_id);
    if (ptx == _tx_table.end()) {
        co_return tm_stm::op_status::not_found;
    }
    tm_transaction tx = ptx->second;
    tx.status = tm_transaction::tx_status::ready;
    tx.partitions.clear();
    tx.groups.clear();
    tx.etag = term;
    co_return co_await update_tx(tx, _insync_term);
}

checked<tm_transaction, tm_stm::op_status>
tm_stm::mark_tx_ongoing(kafka::transactional_id tx_id) {
    auto ptx = _tx_table.find(tx_id);
    if (ptx == _tx_table.end()) {
        return tm_stm::op_status::not_found;
    }
    ptx->second.status = tm_transaction::tx_status::ongoing;
    ptx->second.tx_seq += 1;
    ptx->second.partitions.clear();
    ptx->second.groups.clear();
    return ptx->second;
}

ss::future<tm_stm::op_status> tm_stm::re_register_producer(
  kafka::transactional_id tx_id,
  std::chrono::milliseconds transaction_timeout_ms,
  model::producer_identity pid) {
    vlog(
      clusterlog.trace, "Registering existing tx: id={}, pid={}", tx_id, pid);

    auto ptx = _tx_table.find(tx_id);
    if (ptx == _tx_table.end()) {
        co_return tm_stm::op_status::not_found;
    }
    tm_transaction tx = ptx->second;
    tx.status = tm_transaction::tx_status::ready;
    tx.pid = pid;
    tx.tx_seq += 1;
    tx.etag = _insync_term;
    tx.timeout_ms = transaction_timeout_ms;
    tx.partitions.clear();
    tx.groups.clear();

    _pid_tx_id[pid] = tx_id;

    auto r = co_await update_tx(tx, tx.etag);

    if (!r.has_value()) {
        co_return tm_stm::op_status::unknown;
    }
    co_return tm_stm::op_status::success;
}

ss::future<tm_stm::op_status> tm_stm::register_new_producer(
  kafka::transactional_id tx_id,
  std::chrono::milliseconds transaction_timeout_ms,
  model::producer_identity pid) {
    auto is_ready = co_await sync(_sync_timeout);
    if (!is_ready) {
        co_return tm_stm::op_status::unknown;
    }

    vlog(clusterlog.trace, "Registering new tx: id={}, pid={}", tx_id, pid);

    if (_tx_table.contains(tx_id)) {
        co_return tm_stm::op_status::conflict;
    }

    auto tx = tm_transaction{
      .id = tx_id,
      .pid = pid,
      .tx_seq = model::tx_seq(0),
      .etag = _insync_term,
      .status = tm_transaction::tx_status::ready,
      .timeout_ms = transaction_timeout_ms};
    auto batch = serialize_tx(tx);

    _pid_tx_id[pid] = tx_id;

    auto r = co_await replicate_quorum_ack(tx.etag, std::move(batch));

    if (!r) {
        co_return tm_stm::op_status::unknown;
    }

    if (!co_await wait_no_throw(
          model::offset(r.value().last_offset()), _sync_timeout)) {
        co_return tm_stm::op_status::unknown;
    }

    co_return tm_stm::op_status::success;
}

bool tm_stm::add_partitions(
  kafka::transactional_id tx_id,
  std::vector<tm_transaction::tx_partition> partitions) {
    auto ptx = _tx_table.find(tx_id);
    if (ptx == _tx_table.end()) {
        return false;
    }
    for (auto& partition : partitions) {
        ptx->second.partitions.push_back(partition);
    }
    return true;
}

bool tm_stm::add_group(
  kafka::transactional_id tx_id,
  kafka::group_id group_id,
  model::term_id term) {
    auto ptx = _tx_table.find(tx_id);
    if (ptx == _tx_table.end()) {
        return false;
    }
    ptx->second.groups.push_back(
      tm_transaction::tx_group{.group_id = group_id, .etag = term});
    return true;
}

void tm_stm::expire_old_txs() {
    // TODO: expiration of old transactions
}

ss::future<> tm_stm::load_snapshot(stm_snapshot_header hdr, iobuf&& tm_ss_buf) {
    vassert(
      hdr.version == supported_version,
      "unsupported seq_snapshot_header version {}",
      hdr.version);
    iobuf_parser data_parser(std::move(tm_ss_buf));
    auto data = reflection::adl<tm_snapshot>{}.from(data_parser);

    for (auto& entry : data.transactions) {
        _tx_table.try_emplace(entry.id, entry);
        _pid_tx_id[entry.pid] = entry.id;
    }
    _last_snapshot_offset = data.offset;
    _insync_offset = data.offset;

    return ss::now();
}

ss::future<stm_snapshot> tm_stm::take_snapshot() {
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
    co_return stm_ss;
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
    vassert(
      version == tm_transaction::version,
      "unknown group inflight tx record version: {} expected: {}",
      version,
      tm_transaction::version);
    auto tx = reflection::adl<tm_transaction>{}.from(val_reader);

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

    auto [tx_it, inserted] = _tx_table.try_emplace(tx.id, tx);
    if (!inserted) {
        tx_it->second = tx;
    }

    _pid_tx_id[tx.pid] = tx.id;

    expire_old_txs();

    return ss::now();
}

} // namespace cluster

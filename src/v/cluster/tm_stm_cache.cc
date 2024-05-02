// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cluster/tm_stm_cache.h"

#include "cluster/logger.h"
#include "model/record.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/future.hh>
#include <seastar/util/bool_class.hh>

#include <fmt/ranges.h>

#include <optional>

namespace cluster {

std::ostream& operator<<(std::ostream& o, const tm_transaction& tx) {
    return o << "{tm_transaction: id=" << tx.id << ", status=" << tx.status
             << ", pid=" << tx.pid << ", last_pid=" << tx.last_pid
             << ", etag=" << tx.etag
             << ", size(partitions)=" << tx.partitions.size()
             << ", tx_seq=" << tx.tx_seq << "}";
}

std::optional<tm_transaction>
tm_stm_cache::find(model::term_id term, kafka::transactional_id tx_id) {
    vlog(txlog.trace, "[tx_id={}] looking for tx with term: {}", tx_id, term);

    auto log_it = _log_txes.find(tx_id);
    if (log_it == _log_txes.end()) {
        vlog(
          txlog.trace,
          "[tx_id={}] looking for tx with etag: {}, can't find tx_id in log",
          tx_id,
          term);
        return std::nullopt;
    }

    if (log_it->second.tx.etag != term) {
        vlog(
          txlog.trace,
          "[tx_id={}] looking for tx with etag: {}: found a tx with etag: {} "
          "pid: {} tx_seq: {} (wrong etag)",
          tx_id,
          term,
          log_it->second.tx.etag,
          log_it->second.tx.pid,
          log_it->second.tx.tx_seq);
        return std::nullopt;
    }
    vlog(
      txlog.trace,
      "[tx_id={}] found tx with etag: {} - {}",
      tx_id,
      term,
      log_it->second.tx);
    return log_it->second.tx;
}

std::optional<tm_transaction>
tm_stm_cache::find_log(kafka::transactional_id tx_id) {
    auto tx_it = _log_txes.find(tx_id);
    if (tx_it == _log_txes.end()) {
        return std::nullopt;
    }
    return tx_it->second.tx;
}

void tm_stm_cache::set_log(tm_transaction tx) {
    vlog(
      txlog.trace,
      "[tx_id={}] saving tx with etag: {} pid: {} tx_seq: {} to log",
      tx.id,
      tx.etag,
      tx.pid,
      tx.tx_seq);

    auto [tx_it, inserted] = _log_txes.try_emplace(tx.id, tx);
    if (!inserted) {
        tx_it->second.tx = tx;
    }

    if (tx_it->second._hook.is_linked()) {
        tx_it->second._hook.unlink();
    }
    lru_txes.push_back(tx_it->second);
}

void tm_stm_cache::erase_log(kafka::transactional_id tx_id) {
    auto tx_it = _log_txes.find(tx_id);
    if (tx_it == _log_txes.end()) {
        return;
    }
    auto& tx = tx_it->second.tx;
    vlog(
      txlog.trace,
      "[tx_id= {}] erasing tx with etag: {} pid: {} tx_seq: {} from log",
      tx.id,
      tx.etag,
      tx.pid,
      tx.tx_seq);
    _log_txes.erase(tx_it);
}

fragmented_vector<tm_transaction> tm_stm_cache::get_log_transactions() {
    fragmented_vector<tm_transaction> txes;
    for (auto& entry : _log_txes) {
        txes.push_back(entry.second.tx);
    }
    return txes;
}

void tm_stm_cache::clear_log() {
    vlog(txlog.trace, "clearing log");
    _log_txes.clear();
}

fragmented_vector<tm_transaction> tm_stm_cache::get_all_transactions() {
    fragmented_vector<tm_transaction> ans;

    return get_log_transactions();
}

std::deque<tm_transaction> tm_stm_cache::checkpoint() {
    std::deque<tm_transaction> txes_to_checkpoint;

    return txes_to_checkpoint;
}

std::optional<tm_transaction> tm_stm_cache::oldest_tx() const {
    if (lru_txes.empty()) {
        return std::nullopt;
    }

    return lru_txes.front().tx;
}

size_t tm_stm_cache::tx_cache_size() const { return lru_txes.size(); }

std::ostream& operator<<(std::ostream& o, tm_transaction::tx_status status) {
    switch (status) {
    case tm_transaction::tx_status::ongoing:
        return o << "ongoing";
    case tm_transaction::tx_status::preparing:
        return o << "preparing";
    case tm_transaction::tx_status::prepared:
        return o << "prepared";
    case tm_transaction::tx_status::aborting:
        return o << "aborting";
    case tm_transaction::tx_status::killed:
        return o << "killed";
    case tm_transaction::tx_status::ready:
        return o << "ready";
    case tm_transaction::tx_status::tombstone:
        return o << "tombstone";
    }
}

} // namespace cluster

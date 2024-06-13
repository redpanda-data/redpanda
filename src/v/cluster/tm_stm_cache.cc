// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cluster/tm_stm_cache.h"

#include "base/units.h"
#include "cluster/logger.h"
#include "cluster/types.h"
#include "model/record.h"
#include "raft/errc.h"
#include "raft/fundamental.h"
#include "storage/record_batch_builder.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/future.hh>
#include <seastar/util/bool_class.hh>

#include <fmt/ranges.h>

#include <filesystem>
#include <optional>

namespace cluster {

std::ostream&
operator<<(std::ostream& o, const tm_transaction::tx_partition& tp) {
    fmt::print(
      o,
      "{{ntp: {}, etag: {}, revision: {}}}",
      tp.ntp,
      tp.etag,
      tp.topic_revision);
    return o;
}
std::ostream& operator<<(std::ostream& o, const tm_transaction& tx) {
    fmt::print(
      o,
      "{{id: {}, state: {}, pid: {}, last_pid: {}, etag: {}, seq: {}, "
      "partitions: {}}}",
      tx.id,
      tx.status,
      tx.pid,
      tx.last_pid,
      tx.etag,
      tx.tx_seq,
      fmt::join(tx.partitions, ", "));
    return o;
}

std::optional<tm_transaction>
tm_stm_cache::find(model::term_id term, kafka::transactional_id tx_id) {
    vlog(txlog.trace, "[tx_id={}] looking for tx with term: {}", tx_id, term);
    if (_mem_term && _mem_term.value() == term) {
        // when a node fetches a tx withing a term it means that it was
        // elected as a leader with a higher term and the request should
        // prevent old leader from changing in-memory state (log changes
        // are protected by raft)
        clear_mem();
    }
    auto entry_it = _state.find(term);
    if (entry_it == _state.end()) {
        vlog(
          txlog.trace,
          "[tx_id={}] looking for tx with etag: {}, term not found",
          tx_id,
          term);
        // tm_stm_cache_entry isn't found it means a node memory was wiped
        // and we can't guess it's last state
        return std::nullopt;
    }

    // existence of tm_stm_cache_entry implies that this node
    // was hosting a leader in the given term and if tx_id is missing
    // it means that it was erased and NOT lost because of the
    // reboot

    auto& entry = entry_it->second;
    auto tx_it = entry.txes.find(tx_id);
    if (tx_it != entry.txes.end()) {
        return tx_it->second;
    }

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
tm_stm_cache::find_mem(kafka::transactional_id tx_id) {
    if (_mem_term == std::nullopt) {
        return std::nullopt;
    }
    auto term = _mem_term.value();
    auto entry_it = _state.find(term);
    if (entry_it == _state.end()) {
        return std::nullopt;
    }
    auto& entry = entry_it->second;
    auto tx_it = entry.txes.find(tx_id);
    if (tx_it == entry.txes.end()) {
        return std::nullopt;
    }
    return tx_it->second;
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

    for (auto& [term, entry] : _state) {
        if (term < tx.etag) {
            entry.txes.erase(tx.id);
        }
    }
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

void tm_stm_cache::set_mem(
  model::term_id term, kafka::transactional_id tx_id, tm_transaction tx) {
    vlog(
      txlog.trace,
      "[tx_id={}] setting tx with etag: {} to {}",
      tx_id,
      term,
      tx);
    auto [entry_it, _] = _state.try_emplace(
      term, tm_stm_cache_entry{.term = term});

    entry_it->second.txes[tx_id] = tx;

    if (!_mem_term) {
        if (!_sealed_term || _sealed_term.value() < term) {
            _mem_term = term;
        }
    } else if (_mem_term.value() < term) {
        _mem_term = term;
    }
}

void tm_stm_cache::clear_mem() {
    if (_mem_term) {
        _sealed_term = _mem_term.value();
    }
    _mem_term = std::nullopt;
}

void tm_stm_cache::clear_log() {
    vlog(txlog.trace, "clearing log");
    _log_txes.clear();
}

void tm_stm_cache::erase_mem(kafka::transactional_id tx_id) {
    if (!_mem_term) {
        return;
    }
    auto entry_it = _state.find(_mem_term.value());
    if (entry_it == _state.end()) {
        return;
    }
    auto tx_it = entry_it->second.txes.find(tx_id);
    if (tx_it == entry_it->second.txes.end()) {
        return;
    }
    auto tx = tx_it->second;
    vlog(
      txlog.trace,
      "[tx_id={}] erasing tx with etag: {} pid: {} tx_seq: {} from mem",
      tx.id,
      tx.etag,
      tx.pid,
      tx.tx_seq);
    entry_it->second.txes.erase(tx_id);
}

fragmented_vector<tm_transaction> tm_stm_cache::get_all_transactions() {
    fragmented_vector<tm_transaction> ans;
    if (_mem_term) {
        auto entry_it = _state.find(_mem_term.value());
        if (entry_it != _state.end()) {
            for (const auto& [_, tx] : entry_it->second.txes) {
                ans.push_back(tx);
            }
            for (const auto& [id, tx] : _log_txes) {
                auto tx_it = entry_it->second.txes.find(id);
                if (tx_it == entry_it->second.txes.end()) {
                    ans.push_back(tx.tx);
                }
            }
            return ans;
        }
    }
    return get_log_transactions();
}

std::deque<tm_transaction> tm_stm_cache::checkpoint() {
    std::deque<tm_transaction> txes_to_checkpoint;

    if (_mem_term == std::nullopt) {
        return txes_to_checkpoint;
    }
    auto term = _mem_term.value();

    auto entry_it = _state.find(term);
    if (entry_it == _state.end()) {
        return txes_to_checkpoint;
    }
    auto& entry = entry_it->second;

    auto can_transfer = [](const tm_transaction& tx) {
        return !tx.transferring
               && (tx.status == tm_transaction::ready || tx.status == tm_transaction::ongoing);
    };
    // Loop through all ongoing/pending txns in memory and checkpoint.

    for (auto& [_, tx] : entry.txes) {
        if (can_transfer(tx)) {
            txes_to_checkpoint.push_back(tx);
        }
    }

    return txes_to_checkpoint;
}

std::optional<tm_transaction> tm_stm_cache::oldest_tx() const {
    if (lru_txes.empty()) {
        return std::nullopt;
    }

    return lru_txes.front().tx;
}

size_t tm_stm_cache::tx_cache_size() const { return lru_txes.size(); }

} // namespace cluster

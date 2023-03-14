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
#include "cluster/types.h"
#include "model/record.h"
#include "raft/errc.h"
#include "raft/types.h"
#include "storage/record_batch_builder.h"
#include "units.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/future.hh>
#include <seastar/util/bool_class.hh>

#include <filesystem>
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
    vlog(txlog.trace, "looking for tx:{} etag:{}", tx_id, term);
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
          "looking for tx:{} etag:{}: can't find term:{} in _state",
          tx_id,
          term,
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

    tx_it = _log_txes.find(tx_id);
    if (tx_it == _log_txes.end()) {
        vlog(
          txlog.trace,
          "looking for tx:{} etag:{}: can't find tx:{} in log",
          tx_id,
          term,
          tx_id);
        return std::nullopt;
    }

    if (tx_it->second.etag != term) {
        vlog(
          txlog.trace,
          "looking for tx:{} etag:{}: found a tx with etag:{} pid:{} tx_seq:{} "
          "(wrong etag)",
          tx_id,
          term,
          tx_it->second.etag,
          tx_it->second.pid,
          tx_it->second.tx_seq);
        return std::nullopt;
    }

    return tx_it->second;
}

void tm_stm_cache::set_log(tm_transaction tx) {
    vlog(
      txlog.trace,
      "saving tx:{} etag:{} pid:{} tx_seq:{} to log",
      tx.id,
      tx.etag,
      tx.pid,
      tx.tx_seq);
    _log_txes[tx.id] = tx;
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
    auto tx = tx_it->second;
    vlog(
      txlog.trace,
      "erasing tx:{} etag:{} pid:{} tx_seq:{} from log",
      tx.id,
      tx.etag,
      tx.pid,
      tx.tx_seq);
    _log_txes.erase(tx_id);
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
      "erasing tx:{} etag:{} pid:{} tx_seq:{} from mem",
      tx.id,
      tx.etag,
      tx.pid,
      tx.tx_seq);
    entry_it->second.txes.erase(tx_id);
}

} // namespace cluster

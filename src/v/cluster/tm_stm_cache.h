/*
 * Copyright 2020 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "cluster/persisted_stm.h"
#include "config/configuration.h"
#include "features/feature_table.h"
#include "kafka/protocol/errors.h"
#include "kafka/types.h"
#include "model/fundamental.h"
#include "model/record.h"
#include "model/timestamp.h"
#include "raft/consensus.h"
#include "raft/errc.h"
#include "raft/logger.h"
#include "raft/state_machine.h"
#include "raft/types.h"
#include "storage/snapshot.h"
#include "utils/expiring_promise.h"
#include "utils/mutex.h"

#include <absl/container/btree_set.h>
#include <absl/container/flat_hash_map.h>

#include <compare>

namespace cluster {

// Update this if your patch bumps the version.
// Current version of transaction record (v2).
// Includes all changes for transactions GA.
// + last_pid field - KIP-360 support
// + transferring - tm_stm graceful failover support
// + partition::topic_revision - revision_id of the topic
struct tm_transaction {
    static constexpr uint8_t version = 2;

    enum tx_status : int32_t {
        ongoing,
        preparing,
        prepared,
        aborting, // abort is initiated by a client
        killed,   // abort is initiated by a timeout
        ready,
        tombstone,
    };

    struct tx_partition {
        model::ntp ntp;
        model::term_id etag;
        // Revision id associated with the topic when this partition
        // was added to the transaction. This helps identify if a topic
        // is deleted [+ recreated] in the middle of a transaction.
        // Revision id of a topic is monotonically increasing and
        // corresponds to the create command offset in the controller
        // log.
        model::revision_id topic_revision;

        bool operator==(const tx_partition& other) const = default;
    };

    struct tx_group {
        kafka::group_id group_id;
        model::term_id etag;
    };

    // id of an application executing a transaction. in the early
    // drafts of Kafka protocol transactional_id used to be named
    // application_id
    kafka::transactional_id id;
    // another misnomer in fact producer_identity identifies a
    // session of transactional_id'ed application as any given
    // moment there maybe only one session per tx.id
    model::producer_identity pid;
    // Inforamtion about last producer_identity who worked with this
    // transaction. It is needed for restore producer after redpanda failures
    model::producer_identity last_pid;
    // tx_seq identifues a transactions within a session so a
    // triple (transactional_id, producer_identity, tx_seq) uniquely
    // identidies a transaction
    model::tx_seq tx_seq;
    // term of a transaction coordinated started a transaction.
    // transactions can't span cross term to prevent loss of information stored
    // only in memory (partitions and groups).
    model::term_id etag;
    tx_status status;
    std::chrono::milliseconds timeout_ms;
    ss::lowres_system_clock::time_point last_update_ts;
    std::vector<tx_partition> partitions;
    std::vector<tx_group> groups;

    // Set when transferring a tx from one leader to another. Typically only
    // applies to ready/ongoing txns that can have dirty uncommitted state
    // whereas any other status beyond that is checkpointed to the log.

    // Any `transferring` transactions are first checkpointed as `not
    // transferring` on the new leader before it makes any changes to the state.
    // This happens lazily on access.
    bool transferring = false;

    friend std::ostream& operator<<(std::ostream&, const tm_transaction&);

    std::string_view get_status() const {
        switch (status) {
        case tx_status::ongoing:
            return "ongoing";
        case tx_status::preparing:
            return "preparing";
        case tx_status::prepared:
            return "prepared";
        case tx_status::aborting:
            return "aborting";
        case tx_status::killed:
            return "killed";
        case tx_status::ready:
            return "ready";
        case tx_status::tombstone:
            return "tombstone";
        }
    }

    std::string_view get_kafka_status() const {
        switch (status) {
        case tx_status::ongoing: {
            if (groups.empty() && partitions.empty()) {
                return "Empty";
            }
            return "Ongoing";
        }
        case tx_status::preparing:
            return "Ongoing";
        case tx_status::prepared:
            return "PrepareCommit";
        case tx_status::aborting:
            return "PrepareAbort";
        case tx_status::killed:
            // https://issues.apache.org/jira/browse/KAFKA-6119
            // https://github.com/apache/kafka/commit/501a5e262702bcc043724cb9e1f536e16a66399e
            return "PrepareEpochFence";
        case tx_status::ready:
            return "Empty";
        case tx_status::tombstone:
            return "Dead";
        }
    }

    std::chrono::milliseconds get_staleness() const {
        auto now = ss::lowres_system_clock::now();
        return std::chrono::duration_cast<std::chrono::milliseconds>(
          now - last_update_ts);
    }

    std::chrono::milliseconds get_timeout() const { return timeout_ms; }

    bool delete_partition(const tx_partition& part) {
        return std::erase_if(
                 partitions,
                 [part](const auto& partition) {
                     return partition.ntp == part.ntp
                            && partition.etag == part.etag;
                 })
               > 0;
    }
};

class tm_stm_cache_entry {
public:
    model::term_id term;
    absl::node_hash_map<kafka::transactional_id, tm_transaction> txes;
};

class tm_stm_cache {
public:
    ss::future<ss::basic_rwlock<>::holder> read_lock() {
        return _state_lock.hold_read_lock();
    }

    ss::future<ss::basic_rwlock<>::holder> write_lock() {
        return _state_lock.hold_write_lock();
    }

    void clear_mem() {
        if (_mem_term) {
            _sealed_term = _mem_term.value();
        }
        _mem_term = std::nullopt;
    }

    void clear_log();

    std::optional<tm_transaction> find(model::term_id, kafka::transactional_id);

    std::optional<tm_transaction> find_mem(kafka::transactional_id tx_id) {
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

    std::optional<tm_transaction> find_log(kafka::transactional_id tx_id) {
        auto tx_it = _log_txes.find(tx_id);
        if (tx_it == _log_txes.end()) {
            return std::nullopt;
        }
        return tx_it->second;
    }

    void set_log(tm_transaction);

    void erase_log(kafka::transactional_id);

    fragmented_vector<tm_transaction> get_log_transactions() {
        fragmented_vector<tm_transaction> txes;
        for (auto& entry : _log_txes) {
            txes.push_back(entry.second);
        }
        return txes;
    }

    void set_mem(
      model::term_id term, kafka::transactional_id tx_id, tm_transaction tx) {
        auto entry_it = _state.find(term);
        if (entry_it == _state.end()) {
            _state[term] = tm_stm_cache_entry{.term = term};
            entry_it = _state.find(term);
        }
        entry_it->second.txes[tx_id] = tx;

        if (!_mem_term) {
            if (!_sealed_term || _sealed_term.value() < term) {
                _mem_term = term;
            }
        } else if (_mem_term.value() < term) {
            _mem_term = term;
        }
    }

    void erase_mem(kafka::transactional_id);

    template<typename Func>
    absl::btree_set<kafka::transactional_id>
    filter_all_txid_by_tx(Func&& func) {
        absl::btree_set<kafka::transactional_id> ids;
        for (auto& [id, tx] : _log_txes) {
            if (func(tx)) {
                ids.insert(id);
            }
        }
        if (!_mem_term) {
            return ids;
        }
        auto entry_it = _state.find(_mem_term.value());
        if (entry_it == _state.end()) {
            return ids;
        }
        for (auto& [id, tx] : entry_it->second.txes) {
            if (func(tx)) {
                ids.insert(id);
            }
        }
        return ids;
    }

    fragmented_vector<tm_transaction> get_all_transactions() {
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
                        ans.push_back(tx);
                    }
                }
                return ans;
            }
        }
        return get_log_transactions();
    }

    std::deque<tm_transaction> checkpoint() {
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

private:
    ss::basic_rwlock<> _state_lock;
    // the cache stores all last known txes written by this nodes
    // when it was a leader. a node could be a leader multiple times
    // so the cache groups the txes by the leader's term to preserve
    // last tx per each term
    absl::node_hash_map<model::term_id, tm_stm_cache_entry> _state;
    absl::node_hash_map<kafka::transactional_id, tm_transaction> _log_txes;
    // when a node is a leader _mem_term contains its term to let find_mem
    // fetch txes without specifying it
    std::optional<model::term_id> _mem_term;
    // we can't clear the state when a node loses leadership because
    // a new leader may use fetch_tx rpc to fetch old state; so instead of
    // clearing the state we set _sealed_term to make sure we won't acci-
    // dentally update records in the past after the re-election
    std::optional<model::term_id> _sealed_term;
};

// Updates in v1.
//  + last_update_ts - tracks last updated ts for transactions expiration
//  + tx_status::tombstone - Removes all txn related state upon apply.
struct tm_transaction_v1 {
    static constexpr uint8_t version = 1;

    enum tx_status : int32_t {
        ongoing,
        preparing,
        prepared,
        aborting, // abort is initiated by a client
        killed,   // abort is initiated by a timeout
        ready,
        tombstone,
    };

    struct tx_partition {
        model::ntp ntp;
        model::term_id etag;

        bool operator==(const tx_partition& other) const = default;
    };

    struct tx_group {
        kafka::group_id group_id;
        model::term_id etag;
    };

    // id of an application executing a transaction. in the early
    // drafts of Kafka protocol transactional_id used to be named
    // application_id
    kafka::transactional_id id;
    // another misnomer in fact producer_identity identifies a
    // session of transactional_id'ed application as any given
    // moment there maybe only one session per tx.id
    model::producer_identity pid;
    // tx_seq identifues a transactions within a session so a
    // triple (transactional_id, producer_identity, tx_seq) uniquely
    // identidies a transaction
    model::tx_seq tx_seq;
    // term of a transaction coordinated started a transaction.
    // transactions can't span cross term to prevent loss of information stored
    // only in memory (partitions and groups).
    model::term_id etag;
    tx_status status;
    std::chrono::milliseconds timeout_ms;
    ss::lowres_system_clock::time_point last_update_ts;
    std::vector<tx_partition> partitions;
    std::vector<tx_group> groups;

    tm_transaction::tx_status upcast(tx_status status) {
        switch (status) {
        case tx_status::ongoing:
            return tm_transaction::tx_status::ongoing;
        case tx_status::preparing:
            return tm_transaction::tx_status::preparing;
        case tx_status::prepared:
            return tm_transaction::tx_status::prepared;
        case tx_status::aborting:
            return tm_transaction::tx_status::aborting;
        case tx_status::killed:
            return tm_transaction::tx_status::killed;
        case tx_status::ready:
            return tm_transaction::tx_status::ready;
        case tx_status::tombstone:
            return tm_transaction::tx_status::tombstone;
        }
        vassert(false, "unknown status: {}", status);
    };

    tm_transaction upcast() {
        tm_transaction result;
        result.id = id;
        result.pid = pid;
        result.last_pid = model::unknown_pid;
        result.tx_seq = tx_seq;
        result.etag = etag;
        result.status = upcast(status);
        result.timeout_ms = timeout_ms;
        result.last_update_ts = last_update_ts;
        result.transferring = false;
        for (auto& partition : partitions) {
            result.partitions.push_back(tm_transaction::tx_partition{
              .ntp = partition.ntp, .etag = partition.etag});
        }
        for (auto& group : groups) {
            result.groups.push_back(tm_transaction::tx_group{
              .group_id = group.group_id, .etag = group.etag});
        }
        return result;
    };
};

struct tm_transaction_v0 {
    static constexpr uint8_t version = 0;

    enum tx_status : int32_t {
        ongoing,
        preparing,
        prepared,
        aborting,
        killed,
        ready,
    };

    struct tx_partition {
        model::ntp ntp;
        model::term_id etag;
    };

    struct tx_group {
        kafka::group_id group_id;
        model::term_id etag;
    };

    kafka::transactional_id id;
    model::producer_identity pid;
    model::tx_seq tx_seq;
    model::term_id etag;
    tx_status status;
    std::chrono::milliseconds timeout_ms;
    std::vector<tx_partition> partitions;
    std::vector<tx_group> groups;

    tm_transaction::tx_status upcast(tx_status status) {
        switch (status) {
        case tx_status::ongoing:
            return tm_transaction::tx_status::ongoing;
        case tx_status::preparing:
            return tm_transaction::tx_status::preparing;
        case tx_status::prepared:
            return tm_transaction::tx_status::prepared;
        case tx_status::aborting:
            return tm_transaction::tx_status::aborting;
        case tx_status::killed:
            return tm_transaction::tx_status::killed;
        case tx_status::ready:
            return tm_transaction::tx_status::ready;
        }
        vassert(false, "unknown status: {}", status);
    };

    tm_transaction upcast() {
        tm_transaction result;
        result.id = id;
        result.pid = pid;
        result.last_pid = model::unknown_pid;
        result.tx_seq = tx_seq;
        result.etag = etag;
        result.status = upcast(status);
        result.timeout_ms = timeout_ms;
        result.last_update_ts = ss::lowres_system_clock::now();
        result.transferring = false;
        for (auto& partition : partitions) {
            result.partitions.push_back(tm_transaction::tx_partition{
              .ntp = partition.ntp, .etag = partition.etag});
        }
        for (auto& group : groups) {
            result.groups.push_back(tm_transaction::tx_group{
              .group_id = group.group_id, .etag = group.etag});
        }
        return result;
    };
};

} // namespace cluster

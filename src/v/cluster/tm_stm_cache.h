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

#include "cluster/tm_stm_types.h"
#include "container/fragmented_vector.h"
#include "container/intrusive_list_helpers.h"
#include "kafka/protocol/types.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "model/record.h"

#include <seastar/core/rwlock.hh>

#include <absl/container/btree_set.h>
#include <absl/container/flat_hash_map.h>
#include <absl/container/node_hash_map.h>

namespace cluster {

class tm_stm_cache_entry {
public:
    model::term_id term;
    absl::node_hash_map<kafka::transactional_id, tx_metadata> txes;
};

class tm_stm_cache {
public:
    ss::future<ss::basic_rwlock<>::holder> read_lock() {
        return _state_lock.hold_read_lock();
    }

    ss::future<ss::basic_rwlock<>::holder> write_lock() {
        return _state_lock.hold_write_lock();
    }

    void clear_mem();

    void clear_log();

    std::optional<tx_metadata>
    find(model::term_id, const kafka::transactional_id&);

    std::optional<tx_metadata> find_mem(const kafka::transactional_id& tx_id);

    std::optional<tx_metadata> find_log(const kafka::transactional_id& tx_id);

    void set_log(tx_metadata);
    // It is important that we unlink entries from _log_txes before
    // destroying the entries themselves so that the safe link does not
    // assert.
    void erase_log(const kafka::transactional_id&);

    fragmented_vector<tx_metadata> get_log_transactions();

    void set_mem(model::term_id, kafka::transactional_id, tx_metadata);

    void erase_mem(const kafka::transactional_id&);

    template<typename Func>
    absl::btree_set<kafka::transactional_id>
    filter_all_txid_by_tx(Func&& func) {
        absl::btree_set<kafka::transactional_id> ids;
        for (auto& [id, entry] : _log_txes) {
            if (func(entry.tx)) {
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

    fragmented_vector<tx_metadata> get_all_transactions();

    std::deque<tx_metadata> checkpoint();

    std::optional<tx_metadata> oldest_tx() const;

    size_t tx_cache_size() const;

private:
    struct tx_wrapper {
        tx_wrapper() = default;

        tx_wrapper(const tx_metadata& tx)
          : tx(tx) {}

        tx_metadata tx;
        intrusive_list_hook _hook;
    };

    // Tracks the LRU order of tx sessions. When the count exceeds
    // max_transactions_per_coordinator, we abort tx sessions in the
    // LRU order.
    intrusive_list<tx_wrapper, &tx_wrapper::_hook> lru_txes;

    ss::basic_rwlock<> _state_lock;
    // the cache stores all last known txes written by this nodes
    // when it was a leader. a node could be a leader multiple times
    // so the cache groups the txes by the leader's term to preserve
    // last tx per each term
    absl::node_hash_map<model::term_id, tm_stm_cache_entry> _state;
    absl::node_hash_map<kafka::transactional_id, tx_wrapper> _log_txes;
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
struct transaction_metadata_v1 {
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

    cluster::tx_status upcast(tx_status status) {
        switch (status) {
        case tx_status::ongoing:
            return cluster::tx_status::ongoing;
        case tx_status::preparing:
            return cluster::tx_status::preparing;
        case tx_status::prepared:
            return cluster::tx_status::prepared;
        case tx_status::aborting:
            return cluster::tx_status::aborting;
        case tx_status::killed:
            return cluster::tx_status::killed;
        case tx_status::ready:
            return cluster::tx_status::ready;
        case tx_status::tombstone:
            return cluster::tx_status::tombstone;
        }
        vassert(false, "unknown status: {}", status);
    };

    tx_metadata upcast() {
        tx_metadata result;
        result.id = id;
        result.pid = pid;
        result.last_pid = model::no_pid;
        result.tx_seq = tx_seq;
        result.etag = etag;
        result.status = upcast(status);
        result.timeout_ms = timeout_ms;
        result.last_update_ts = last_update_ts;
        for (auto& partition : partitions) {
            result.partitions.push_back(tx_metadata::tx_partition{
              .ntp = partition.ntp, .etag = partition.etag});
        }
        for (auto& group : groups) {
            result.groups.push_back(tx_metadata::tx_group{
              .group_id = group.group_id, .etag = group.etag});
        }
        return result;
    };
};

struct transaction_metadata_v0 {
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

    cluster::tx_status upcast(tx_status status) {
        switch (status) {
        case tx_status::ongoing:
            return cluster::tx_status::ongoing;
        case tx_status::preparing:
            return cluster::tx_status::preparing;
        case tx_status::prepared:
            return cluster::tx_status::prepared;
        case tx_status::aborting:
            return cluster::tx_status::aborting;
        case tx_status::killed:
            return cluster::tx_status::killed;
        case tx_status::ready:
            return cluster::tx_status::ready;
        }
        vassert(false, "unknown status: {}", status);
    };

    tx_metadata upcast() {
        tx_metadata result;
        result.id = id;
        result.pid = pid;
        result.last_pid = model::no_pid;
        result.tx_seq = tx_seq;
        result.etag = etag;
        result.status = upcast(status);
        result.timeout_ms = timeout_ms;
        result.last_update_ts = ss::lowres_system_clock::now();
        for (auto& partition : partitions) {
            result.partitions.push_back(tx_metadata::tx_partition{
              .ntp = partition.ntp, .etag = partition.etag});
        }
        for (auto& group : groups) {
            result.groups.push_back(tx_metadata::tx_group{
              .group_id = group.group_id, .etag = group.etag});
        }
        return result;
    };
};

} // namespace cluster

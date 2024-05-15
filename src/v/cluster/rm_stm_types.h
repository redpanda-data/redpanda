// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#pragma once

#include "cluster/types.h"
#include "kafka/protocol/wire.h"
#include "reflection/async_adl.h"

namespace cluster::tx {

static constexpr const int8_t abort_snapshot_version = 0;
static constexpr int8_t prepare_control_record_version{0};
static constexpr int8_t fence_control_record_v0_version{0};
static constexpr int8_t fence_control_record_v1_version{1};
static constexpr int8_t fence_control_record_version{2};

using tx_range = model::tx_range;
using clock_type = ss::lowres_clock;
using time_point_type = clock_type::time_point;
using duration_type = clock_type::duration;

struct fence_batch_data {
    model::batch_identity bid;
    std::optional<model::tx_seq> tx_seq;
    std::optional<std::chrono::milliseconds> transaction_timeout_ms;
    model::partition_id tm;
};

struct expiration_info {
    duration_type timeout;
    time_point_type last_update;
    bool is_expiration_requested;

    time_point_type deadline() const { return last_update + timeout; }

    bool is_expired(time_point_type now) const {
        return is_expiration_requested || deadline() <= now;
    }
};

// Status of a single transaction within a data partition.
// Not to be confused with user visible transaction status
// that can potentially span multiple data partitions.
enum class partition_transaction_status : int8_t {
    // Data has been appended as a part of the transaction.
    ongoing = 0,
    //
    // note: couple of unused status types [1, 2] were removed.
    //
    // Partition is aware of the transaction but the client has
    // not produced data as a part of this transaction.
    initialized = 3
};

std::ostream& operator<<(std::ostream&, const partition_transaction_status&);

struct transaction_info {
    partition_transaction_status status;
    model::offset lso_bound;
    std::optional<expiration_info> info;
    std::optional<int32_t> seq;

    ss::sstring get_status() const;
    bool is_expired() const;
    std::optional<duration_type> get_staleness() const;
    std::optional<duration_type> get_timeout() const;
};

using transaction_set
  = absl::btree_map<model::producer_identity, transaction_info>;

struct tx_data {
    model::tx_seq tx_seq;
    model::partition_id tm_partition;
};

model::record_batch make_fence_batch(
  model::producer_identity,
  model::tx_seq,
  std::chrono::milliseconds,
  model::partition_id);

fence_batch_data read_fence_batch(model::record_batch&& b);

model::control_record_type parse_control_batch(const model::record_batch&);

model::record_batch
make_tx_control_batch(model::producer_identity pid, model::control_record_type);

// snapshot related types

struct abort_index {
    model::offset first;
    model::offset last;

    bool operator==(const abort_index&) const = default;
};

struct prepare_marker {
    // partition of the transaction manager
    // reposible for curent transaction
    model::partition_id tm_partition;
    // tx_seq identifies a transaction within a session
    model::tx_seq tx_seq;
    model::producer_identity pid;

    bool operator==(const prepare_marker&) const = default;
};

struct abort_snapshot {
    model::offset first;
    model::offset last;
    fragmented_vector<tx_range> aborted;

    bool match(abort_index idx) {
        return idx.first == first && idx.last == last;
    }
    friend std::ostream& operator<<(std::ostream&, const abort_snapshot&);

    bool operator==(const abort_snapshot&) const = default;
};

struct producer_state_snapshot {
    struct finished_request {
        int32_t _first_sequence;
        int32_t _last_sequence;
        kafka::offset _last_offset;
    };

    model::producer_identity _id;
    raft::group_id _group;
    std::vector<finished_request> _finished_requests;
    std::chrono::milliseconds _ms_since_last_update;
};

struct tx_data_snapshot {
    model::producer_identity pid;
    model::tx_seq tx_seq;
    model::partition_id tm;

    bool operator==(const tx_data_snapshot&) const = default;
};

// note: support for tx_snapshot::version[0-3] was dropped
// in v24.1.x

struct expiration_snapshot {
    model::producer_identity pid;
    duration_type timeout;

    bool operator==(const expiration_snapshot&) const = default;
};

// only retained for snapshot backward compatibility purposes
// during rollback of an upgrade.
struct deprecated_seq_entry {
    static const int seq_cache_size = 5;

    struct deprecated_seq_cache_entry {
        int32_t seq{-1};
        kafka::offset offset;

        bool operator==(const deprecated_seq_cache_entry&) const = default;
    };

    model::producer_identity pid;
    int32_t seq{-1};
    kafka::offset last_offset{-1};
    ss::circular_buffer<deprecated_seq_cache_entry> seq_cache;
    model::timestamp::type last_write_timestamp;

    bool operator==(const deprecated_seq_entry& other) const;

    static deprecated_seq_entry
    from_producer_state_snapshot(producer_state_snapshot&);
};

struct tx_snapshot_v4 {
    static constexpr uint8_t version = 4;

    fragmented_vector<model::producer_identity> fenced;
    fragmented_vector<tx_range> ongoing;
    fragmented_vector<prepare_marker> prepared;
    fragmented_vector<tx_range> aborted;
    fragmented_vector<abort_index> abort_indexes;
    model::offset offset;
    fragmented_vector<deprecated_seq_entry> seqs;
    fragmented_vector<tx_data_snapshot> tx_data;
    fragmented_vector<expiration_snapshot> expiration;

    bool operator==(const tx_snapshot_v4&) const = default;
};

struct tx_snapshot {
    static constexpr uint8_t version = 5;

    tx_snapshot() = default;
    explicit tx_snapshot(tx_snapshot_v4, raft::group_id);

    model::offset offset;
    // NOTE:
    // Currently producer_state only encapsulates idempotency
    // related state, hence the snapshot contains separate data
    // members for transactional state. Once transactional state
    // is ported into producer_state, these data members can
    // be removed.
    fragmented_vector<producer_state_snapshot> producers;

    // transactional state
    fragmented_vector<model::producer_identity> fenced;
    fragmented_vector<tx_range> ongoing;
    fragmented_vector<prepare_marker> prepared;
    fragmented_vector<tx_range> aborted;
    fragmented_vector<abort_index> abort_indexes;

    fragmented_vector<tx_data_snapshot> tx_data;
    fragmented_vector<expiration_snapshot> expiration;
    model::producer_id highest_producer_id{};

    bool operator==(const tx_snapshot&) const = default;
};

}; // namespace cluster::tx

namespace reflection {

using tx_snapshot_v4 = cluster::tx::tx_snapshot_v4;
template<>
struct async_adl<tx_snapshot_v4> {
    ss::future<> to(iobuf&, tx_snapshot_v4);
    ss::future<tx_snapshot_v4> from(iobuf_parser&);
};

using tx_snapshot = cluster::tx::tx_snapshot;
template<>
struct async_adl<tx_snapshot> {
    ss::future<> to(iobuf&, tx_snapshot);
    ss::future<tx_snapshot> from(iobuf_parser&);
};
}; // namespace reflection

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

inline constexpr const int8_t abort_snapshot_version = 0;
inline constexpr int8_t prepare_control_record_version{0};
inline constexpr int8_t fence_control_record_v0_version{0};
inline constexpr int8_t fence_control_record_v1_version{1};
inline constexpr int8_t fence_control_record_version{2};

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
    // note: couple of unused status types [1, 2] were removed.
    // enumerators are defined in the order in which a transaction
    // state progresses but the enum values are all over the place
    // to maintain backward compatibility with original definitions
    // that definied ongoing = 0
    //
    // Partition is aware of the transaction but the client has
    // not produced data as a part of this transaction.
    initialized = 3,
    // Data has been appended as a part of the transaction.
    ongoing = 0,
    committed = 4,
    aborted = 5
};

std::ostream& operator<<(std::ostream&, const partition_transaction_status&);

// Captures the information about the transaction within a single data
// partition. A user initiated transaction can span multiple data partitions but
// this struct only captures it's state within a single partition. This is
// always used in the context of a given partition.
struct partition_transaction_info {
    partition_transaction_status status;
    model::offset lso_bound;
    std::optional<expiration_info> info;
    std::optional<int32_t> seq;

    ss::sstring get_status() const;
    bool is_expired() const;
    std::optional<duration_type> get_staleness() const;
    std::optional<duration_type> get_timeout() const;
};

using partition_transactions
  = absl::btree_map<model::producer_identity, partition_transaction_info>;

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

struct abort_index
  : serde::envelope<abort_index, serde::version<0>, serde::compat_version<0>> {
    abort_index() = default;
    abort_index(model::offset first, model::offset last)
      : first(first)
      , last(last) {}
    model::offset first;
    model::offset last;

    auto serde_fields() { return std::tie(first, last); }

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

// All transaction related state of an open transaction
// on a single partition.
struct producer_partition_transaction_state
  : serde::envelope<
      producer_partition_transaction_state,
      serde::version<0>,
      serde::compat_version<0>> {
    // begin offset of the open transaction.
    model::offset first;
    // current last offset of the open transaction. This
    // changes as more data batches are appended as a part
    // of the transaction.
    model::offset last;
    // sequence is bumped for every completed (committed/aborted) transaction
    // using the same producer id. This helps disambiguate requests meant for
    // different transactions with the same producer id.
    model::tx_seq sequence;
    // An optional user set timeout for the transaction.
    std::optional<std::chrono::milliseconds> timeout;
    // Transaction coordinator partition (tx/n) coordinating this transaction.
    model::partition_id coordinator_partition{
      model::legacy_tm_ntp.tp.partition};
    // Current status of the transaction.
    partition_transaction_status status;

    std::chrono::milliseconds timeout_ms() const {
        return timeout.value_or(std::chrono::milliseconds::max());
    }

    bool operator==(const producer_partition_transaction_state&) const
      = default;

    bool is_in_progress() const;

    auto serde_fields() {
        return std::tie(
          first, last, sequence, timeout, coordinator_partition, status);
    }
};

std::ostream&
operator<<(std::ostream& o, const producer_partition_transaction_state&);

struct producer_state_snapshot
  : serde::envelope<
      producer_state_snapshot,
      serde::version<0>,
      serde::compat_version<0>> {
    struct finished_request
      : serde::envelope<
          finished_request,
          serde::version<0>,
          serde::compat_version<0>> {
        finished_request() = default;
        finished_request(int32_t first, int32_t last, kafka::offset last_offset)
          : first_sequence(first)
          , last_sequence(last)
          , last_offset(last_offset) {}

        int32_t first_sequence;
        int32_t last_sequence;
        kafka::offset last_offset;

        auto serde_fields() {
            return std::tie(first_sequence, last_sequence, last_offset);
        }
    };

    model::producer_identity id;
    raft::group_id group;
    std::vector<finished_request> finished_requests;
    std::chrono::milliseconds ms_since_last_update;
    std::optional<producer_partition_transaction_state> transaction_state;

    bool operator==(const producer_state_snapshot&) const = default;

    auto serde_fields() {
        return std::tie(
          id,
          group,
          finished_requests,
          ms_since_last_update,
          transaction_state);
    }
};

// Used in the older version of snapshots when the snapshot payload was
// serialized using reflection.
struct producer_state_snapshot_deprecated {
    struct finished_request {
        int32_t first_sequence;
        int32_t last_sequence;
        kafka::offset last_offset;
    };
    model::producer_identity id;
    raft::group_id group;
    std::vector<finished_request> finished_requests;
    std::chrono::milliseconds ms_since_last_update;
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

struct tx_snapshot_v5 {
    static constexpr uint8_t version = 5;

    tx_snapshot_v5() = default;
    explicit tx_snapshot_v5(tx_snapshot_v4, raft::group_id);

    model::offset offset;
    // NOTE:
    // Currently producer_state only encapsulates idempotency
    // related state, hence the snapshot contains separate data
    // members for transactional state. Once transactional state
    // is ported into producer_state, these data members can
    // be removed.
    fragmented_vector<producer_state_snapshot_deprecated> producers;

    // transactional state
    fragmented_vector<model::producer_identity> fenced;
    fragmented_vector<tx_range> ongoing;
    fragmented_vector<prepare_marker> prepared;
    fragmented_vector<tx_range> aborted;
    fragmented_vector<abort_index> abort_indexes;

    fragmented_vector<tx_data_snapshot> tx_data;
    fragmented_vector<expiration_snapshot> expiration;
    model::producer_id highest_producer_id{};

    bool operator==(const tx_snapshot_v5&) const = default;
};

struct tx_snapshot_v6
  : serde::
      envelope<tx_snapshot_v6, serde::version<0>, serde::compat_version<0>> {
    static constexpr uint8_t version = 6;

    tx_snapshot_v6() = default;
    explicit tx_snapshot_v6(tx_snapshot_v5, raft::group_id);

    fragmented_vector<producer_state_snapshot> producers;
    fragmented_vector<tx_range> aborted;
    fragmented_vector<abort_index> abort_indexes;
    model::producer_id highest_producer_id;

    tx_snapshot_v5 downgrade_to_v5() &&;

    friend std::ostream& operator<<(std::ostream&, const tx_snapshot_v6&);

    bool operator==(const tx_snapshot_v6&) const = default;

    auto serde_fields() {
        return std::tie(producers, aborted, abort_indexes, highest_producer_id);
    }
};

using tx_snapshot = tx_snapshot_v6;

}; // namespace cluster::tx

namespace reflection {

using tx_snapshot_v4 = cluster::tx::tx_snapshot_v4;
template<>
struct async_adl<tx_snapshot_v4> {
    ss::future<> to(iobuf&, tx_snapshot_v4);
    ss::future<tx_snapshot_v4> from(iobuf_parser&);
};

using tx_snapshot_v5 = cluster::tx::tx_snapshot_v5;
template<>
struct async_adl<tx_snapshot_v5> {
    ss::future<> to(iobuf&, tx_snapshot_v5);
    ss::future<tx_snapshot_v5> from(iobuf_parser&);
};

template<>
struct async_adl<cluster::tx::abort_index> {
    ss::future<> to(iobuf& out, cluster::tx::abort_index t);
    ss::future<cluster::tx::abort_index> from(iobuf_parser& in);
};

template<>
struct adl<model::tx_range> {
    void to(iobuf& out, model::tx_range t);
    model::tx_range from(iobuf_parser& in);
};

template<>
struct async_adl<model::tx_range> {
    ss::future<> to(iobuf& out, model::tx_range t);
    ss::future<model::tx_range> from(iobuf_parser& in);
};

}; // namespace reflection

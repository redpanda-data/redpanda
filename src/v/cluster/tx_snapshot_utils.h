// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#pragma once

#include "cluster/rm_stm.h"
#include "producer_state.h"
#include "reflection/async_adl.h"

using tx_range = cluster::rm_stm::tx_range;
using prepare_marker = cluster::rm_stm::prepare_marker;
using abort_index = cluster::rm_stm::abort_index;
using duration_type = cluster::rm_stm::duration_type;

namespace cluster {

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

struct tx_data_snapshot {
    model::producer_identity pid;
    model::tx_seq tx_seq;
    model::partition_id tm;

    bool operator==(const tx_data_snapshot&) const = default;
};

// note: support for tx_snapshot::version[0-3] was dropped
// in v23.3.x

struct expiration_snapshot {
    model::producer_identity pid;
    duration_type timeout;

    bool operator==(const expiration_snapshot&) const = default;
};

struct tx_snapshot_v3 {
    static constexpr uint8_t version = 3;

    fragmented_vector<model::producer_identity> fenced;
    fragmented_vector<rm_stm::tx_range> ongoing;
    fragmented_vector<rm_stm::prepare_marker> prepared;
    fragmented_vector<rm_stm::tx_range> aborted;
    fragmented_vector<rm_stm::abort_index> abort_indexes;
    model::offset offset;
    fragmented_vector<deprecated_seq_entry> seqs;

    struct tx_seqs_snapshot {
        model::producer_identity pid;
        model::tx_seq tx_seq;
        bool operator==(const tx_seqs_snapshot&) const = default;
    };

    fragmented_vector<tx_seqs_snapshot> tx_seqs;
    fragmented_vector<expiration_snapshot> expiration;

    bool operator==(const tx_snapshot_v3&) const = default;
};

struct tx_snapshot_v4 {
    static constexpr uint8_t version = 4;

    fragmented_vector<model::producer_identity> fenced;
    fragmented_vector<tx_range> ongoing;
    fragmented_vector<prepare_marker> prepared;
    fragmented_vector<tx_range> aborted;
    fragmented_vector<abort_index> abort_indexes;
    model::offset offset;
    fragmented_vector<cluster::deprecated_seq_entry> seqs;
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
    fragmented_vector<cluster::producer_state_snapshot> producers;

    // transactional state
    fragmented_vector<model::producer_identity> fenced;
    fragmented_vector<tx_range> ongoing;
    fragmented_vector<prepare_marker> prepared;
    fragmented_vector<tx_range> aborted;
    fragmented_vector<abort_index> abort_indexes;

    fragmented_vector<tx_data_snapshot> tx_data;
    fragmented_vector<expiration_snapshot> expiration;

    bool operator==(const tx_snapshot&) const = default;
};

}; // namespace cluster

namespace reflection {

// note: tx_snapshot[v0-v2] cleaned up in 23.3.x
using tx_snapshot_v3 = cluster::tx_snapshot_v3;
template<>
struct async_adl<tx_snapshot_v3> {
    ss::future<> to(iobuf&, tx_snapshot_v3);
    ss::future<tx_snapshot_v3> from(iobuf_parser&);
};

using tx_snapshot_v4 = cluster::tx_snapshot_v4;
template<>
struct async_adl<tx_snapshot_v4> {
    ss::future<> to(iobuf&, tx_snapshot_v4);
    ss::future<tx_snapshot_v4> from(iobuf_parser&);
};

using tx_snapshot = cluster::tx_snapshot;
template<>
struct async_adl<tx_snapshot> {
    ss::future<> to(iobuf&, tx_snapshot);
    ss::future<tx_snapshot> from(iobuf_parser&);
};
}; // namespace reflection

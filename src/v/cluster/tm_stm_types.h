/**
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */
#pragma once
#include "kafka/protocol/types.h"
#include "model/fundamental.h"
#include "model/record.h"
namespace cluster {

enum tx_status : int32_t {
    ongoing,
    preparing,
    prepared,
    aborting, // abort is initiated by a client
    killed,   // abort is initiated by a timeout
    ready,
    tombstone,
};

std::ostream& operator<<(std::ostream&, tx_status);

struct tx_metadata {
    static constexpr uint8_t version = 2;

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
        friend std::ostream& operator<<(std::ostream&, const tx_partition&);
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
    // Identity of a producer that previously updated the transaction state
    model::producer_identity last_pid;
    // tx_seq identifies a transactions within a session
    //
    // triple (transactional_id, producer_identity, tx_seq) uniquely
    // identifies a transaction
    model::tx_seq tx_seq;
    // Term of a transaction coordinator that started the transaction.
    //
    // Currently, transactions can't span cross term to prevent loss of
    // information stored only in memory (partitions and groups).
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

    std::string_view get_status() const;
    std::string_view get_kafka_status() const;
    std::chrono::milliseconds get_staleness() const;
    std::chrono::milliseconds get_timeout() const { return timeout_ms; }

    bool delete_partition(const tx_partition& part);

    friend std::ostream& operator<<(std::ostream&, const tx_metadata&);
};
} // namespace cluster

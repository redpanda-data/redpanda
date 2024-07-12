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

/*
 *                    +---------------+
 *                    |               |
 *     +--------------+     empty     +---+
 *     |              |               |   |
 *     |              +-------+-------+   |
 *     |                      |           |
 *     |                      |           |
 *     |                      |         abort
 *  commit                   add          |
 *     |              partition/offset    |
 *     |                      |           |
 *     |                      |           |
 *     |              +-------v-------+   |
 *     |              |               |   |
 *     |    +---------+    ongoing    +---+------------+
 *     |    |         |               |   |            |
 *     |    |         +-------+-------+   |            |
 *     |    |                 |           |         producer
 *     |  commit            abort   +-----+          fenced
 *     |    |                 |     |                  |
 *     |    |                 |     |                  |
 *  +--v----v-------+ +-------v-----v-+ +--------------v---------------+
 *  |               | |               | |                              |
 *  |prepare_commit | | prepare_abort | |    prepare_internal_abort    |
 *  |               | |               | |                              |
 *  +-------+-------+ +------+--------+ +------------+-----------------+
 *          |                |                       |
 *    done committing   done aborting            done aborting
 *          |                |                       |
 * +--------v--------+       |  +----------------+   |
 * |                 |       |  |                |   |
 * | complete_commit |       +->| complete_abort |<--+
 * |                 |          |                |
 * +--------+--------+          +--------+-------+
 *          |                            |
 *      expired                       expired
 *          |                            |
 *          |      +---------------+     |
 *          |      |               |     |
 *          +----->|   tombstone   |<----+
 *                 |               |
 *                 +---------------+
 */
enum tx_status : int32_t {
    /**
     * When transactional id to producer id mapping is added to coordinator it
     * starts in this state
     */
    empty = 5,
    /**
     * Transaction is ongoing as soon as partition or offset is added to it
     */
    ongoing = 0,
    /**
     * Transaction is marked as preparing_commit when it is requested to be
     * committed by the client but before committing transaction on data and
     * offset partitions (mapped from earlier prepared state)
     */
    preparing_commit = 2,
    /**
     * Transaction is completed commit when its participants committed the
     * transaction.
     */
    completed_commit = 7,
    /**
     * Transaction is marked as preparing_abort when it is requested to be
     * aborted by the client but before aborting transaction on data and
     * offset partitions
     */
    preparing_abort = 3,
    /**
     * The same as the preparing_abort but when transaction is aborted by
     * internals of Redpanda f.e. timeout or transactional id limit overflow
     */
    preparing_internal_abort = 4,
    /**
     * Transaction is completed abort when its participants aborted the
     * transaction. This state is not replicated as aborting data on
     * participants is idempotent and can be retried by the new leader.
     */
    completed_abort = 8,
    /**
     * Transaction is about to be removed. The tombstone is used when
     * transaction and all related state is forgotten. The tombstones are
     * replicated when transaction state is expired.
     */
    tombstone = 6,
};

std::ostream& operator<<(std::ostream&, tx_status);
/**
 * Simple tuple representing state transition error.
 */
struct state_transition_error {
    state_transition_error(tx_status, tx_status);

    tx_status from;
    tx_status to;

    friend std::ostream&
    operator<<(std::ostream&, const state_transition_error&);
};

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
    /**
     * Validates and updates the transaction metadata status if transition is
     * allowed by transaction FSM.
     */
    std::optional<state_transition_error> try_update_status(tx_status);
    /**
     * Returns true if transaction is already finished. When transaction is
     * finished it is either commit or abort completed and has no participants
     */
    bool is_finished() const;

    friend std::ostream& operator<<(std::ostream&, const tx_metadata&);
};

bool is_state_transition_valid(const tx_metadata&, tx_status);

/**
 * Deprecated metadata formats
 */

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
            return cluster::tx_status::preparing_commit;
        case tx_status::prepared:
            return cluster::tx_status::completed_commit;
        case tx_status::aborting:
            return cluster::tx_status::preparing_abort;
        case tx_status::killed:
            return cluster::tx_status::preparing_internal_abort;
        case tx_status::ready:
            return cluster::tx_status::empty;
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
            return cluster::tx_status::preparing_commit;
        case tx_status::prepared:
            return cluster::tx_status::completed_commit;
        case tx_status::aborting:
            return cluster::tx_status::preparing_abort;
        case tx_status::killed:
            return cluster::tx_status::preparing_internal_abort;
        case tx_status::ready:
            return cluster::tx_status::empty;
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

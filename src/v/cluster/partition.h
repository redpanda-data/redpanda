/*
 * Copyright 2020 Vectorized, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "cluster/partition_probe.h"
#include "cluster/seq_stm.h"
#include "cluster/types.h"
#include "model/metadata.h"
#include "model/record_batch_reader.h"
#include "raft/consensus.h"
#include "raft/group_configuration.h"
#include "raft/id_allocator_stm.h"
#include "raft/log_eviction_stm.h"
#include "raft/types.h"
#include "storage/types.h"

namespace cluster {
class partition_manager;

/// holds cluster logic that is not raft related
/// all raft logic is proxied transparently
class partition {
public:
    explicit partition(consensus_ptr r);

    raft::group_id group() const { return _raft->group(); }
    ss::future<> start();
    ss::future<> stop();

    ss::future<result<raft::replicate_result>>
    replicate(model::record_batch_reader&&, raft::replicate_options);

    ss::future<checked<raft::replicate_result, kafka::error_code>> replicate(
      model::batch_identity,
      model::record_batch_reader&&,
      raft::replicate_options);

    /**
     * The reader is modified such that the max offset is configured to be
     * the minimum of the max offset requested and the committed index of the
     * underlying raft group.
     */
    ss::future<model::record_batch_reader> make_reader(
      storage::log_reader_config config,
      std::optional<model::timeout_clock::time_point> deadline = std::nullopt) {
        return _raft->make_reader(std::move(config), deadline);
    }

    model::offset start_offset() const { return _raft->start_offset(); }

    /**
     * The returned value of last committed offset should not be used to
     * do things like initialize a reader (use partition::make_reader). Instead
     * it can be used to report upper offset bounds to clients.
     */
    model::offset committed_offset() const { return _raft->committed_offset(); }

    /**
     * <kafka>The last stable offset (LSO) is defined as the first offset such
     * that all lower offsets have been "decided." Non-transactional messages
     * are considered decided immediately, but transactional messages are only
     * decided when the corresponding COMMIT or ABORT marker is written. This
     * implies that the last stable offset will be equal to the high watermark
     * if there are no transactional messages in the log. Note also that the LSO
     * cannot advance beyond the high watermark.  </kafka>
     *
     * There are two important pieces in this comment:
     *
     *   1) "non-transaction message are considered decided immediately". Since
     *   redpanda doesn't have transactional messages, that's what we're
     *   interested in.
     *
     *   2) "first offset such that all lower offsets have been decided". this
     *   is describing a strictly greater than relationship.
     *
     * Since we currently use the commited_offset to report the end of log to
     * kafka clients, simply report the next offset.
     */
    model::offset last_stable_offset() const {
        return _raft->last_stable_offset() + model::offset(1);
    }

    /**
     * Greatest offset visible to consumers. Named high_watermark to be
     * consistent with Kafka nomenclature.
     */
    model::offset high_watermark() const { return _raft->last_visible_index(); }

    const model::ntp& ntp() const { return _raft->ntp(); }

    ss::future<std::optional<storage::timequery_result>>
      timequery(model::timestamp, ss::io_priority_class);

    bool is_leader() const { return _raft->is_leader(); }

    ss::future<std::error_code>
    transfer_leadership(std::optional<model::node_id> target) {
        return _raft->transfer_leadership(target);
    }

    ss::future<std::error_code> update_replica_set(
      std::vector<model::broker> brokers, model::revision_id new_revision_id) {
        return _raft->replace_configuration(
          std::move(brokers), new_revision_id);
    }

    raft::group_configuration group_configuration() const {
        return _raft->config();
    }

    partition_probe& probe() { return _probe; }

    model::revision_id get_revision_id() const {
        return _raft->config().revision_id();
    }

    model::offset get_latest_configuration_offset() const {
        return _raft->get_latest_configuration_offset();
    }

    ss::lw_shared_ptr<raft::id_allocator_stm>& id_allocator_stm() {
        return _id_allocator_stm;
    }

private:
    friend partition_manager;

    consensus_ptr raft() { return _raft; }

private:
    consensus_ptr _raft;
    ss::lw_shared_ptr<raft::log_eviction_stm> _nop_stm;
    ss::lw_shared_ptr<raft::id_allocator_stm> _id_allocator_stm;
    ss::shared_ptr<seq_stm> _seq_stm;
    ss::abort_source _as;
    partition_probe _probe;

    friend std::ostream& operator<<(std::ostream& o, const partition& x);
};
} // namespace cluster
namespace std {
template<>
struct hash<cluster::partition> {
    size_t operator()(const cluster::partition& x) const {
        return std::hash<model::ntp>()(x.ntp());
    }
};
} // namespace std

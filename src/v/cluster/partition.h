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

#include "cloud_storage/manifest.h"
#include "cloud_storage/remote_partition.h"
#include "cluster/id_allocator_stm.h"
#include "cluster/partition_probe.h"
#include "cluster/rm_stm.h"
#include "cluster/tm_stm.h"
#include "cluster/types.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "model/record_batch_reader.h"
#include "raft/consensus.h"
#include "raft/consensus_utils.h"
#include "raft/group_configuration.h"
#include "raft/log_eviction_stm.h"
#include "raft/types.h"
#include "storage/types.h"

namespace cluster {
class partition_manager;

/// holds cluster logic that is not raft related
/// all raft logic is proxied transparently
class partition {
public:
    partition(consensus_ptr r, ss::sharded<cluster::tx_gateway_frontend>&);

    raft::group_id group() const { return _raft->group(); }
    ss::future<> start();
    ss::future<> stop();

    ss::future<result<raft::replicate_result>>
    replicate(model::record_batch_reader&&, raft::replicate_options);

    raft::replicate_stages
    replicate_in_stages(model::record_batch_reader&&, raft::replicate_options);

    ss::future<result<raft::replicate_result>> replicate(
      model::term_id, model::record_batch_reader&&, raft::replicate_options);

    ss::future<result<raft::replicate_result>> replicate(
      model::batch_identity,
      model::record_batch_reader&&,
      raft::replicate_options);

    raft::replicate_stages replicate_in_stages(
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
        if (
          cloud_data_available()
          && config.start_offset < _raft->start_offset()) {
            auto begin = _cloud_storage_partition->first_uploaded_offset();
            auto end = _cloud_storage_partition->last_uploaded_offset();
            if (!(config.max_offset < begin) && !(end < config.start_offset)) {
                // Special case for shadow indexing.
                // We're creating shadow indexing record_batch_reader which will
                // read only remote data.
                return _cloud_storage_partition->make_reader(config, deadline);
            }
        }
        return _raft->make_reader(config, deadline);
    }

    model::offset start_offset() const {
        if (cloud_data_available()) {
            // TODO: reconsider
            return std::min(_raft->start_offset(), start_offset_cloud());
        }
        return _raft->start_offset();
    }

    // TODO: this code should go away when archival snapshot gets integrated
    model::offset start_offset_cloud() const {
        vassert(
          cloud_data_available(),
          "Method can only be called if cloud data is available");
        return _cloud_storage_partition->first_uploaded_offset();
    }
    model::offset max_offset_cloud() const {
        vassert(
          cloud_data_available(),
          "Method can only be called if cloud data is available");
        return _cloud_storage_partition->last_uploaded_offset();
    }
    bool cloud_data_available() const {
        return static_cast<bool>(_cloud_storage_partition);
    }
    void connect_with_cloud_storage(
      ss::weak_ptr<cloud_storage::remote_partition> part) {
        _cloud_storage_partition = std::move(part);
    }
    // TODO: end section

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
        if (_rm_stm) {
            return _rm_stm->last_stable_offset();
        }

        return high_watermark();
    }

    /**
     * All batches with offets smaller than high watermark are visible to
     * consumers. Named high_watermark to be consistent with Kafka nomenclature.
     */
    model::offset high_watermark() const {
        return raft::details::next_offset(_raft->last_visible_index());
    }

    model::term_id term() { return _raft->term(); }

    model::offset dirty_offset() const {
        return _raft->log().offsets().dirty_offset;
    }

    const model::ntp& ntp() const { return _raft->ntp(); }

    ss::future<std::optional<storage::timequery_result>>
      timequery(model::timestamp, ss::io_priority_class);

    bool is_leader() const { return _raft->is_leader(); }

    ss::future<std::error_code>
    transfer_leadership(std::optional<model::node_id> target) {
        return _raft->do_transfer_leadership(target);
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

    ss::lw_shared_ptr<cluster::id_allocator_stm>& id_allocator_stm() {
        return _id_allocator_stm;
    }

    const raft::configuration_manager& get_cfg_manager() const {
        return _raft->get_configuration_manager();
    }

    ss::shared_ptr<cluster::rm_stm> rm_stm();

    size_t size_bytes() const { return _raft->log().size_bytes(); }
    ss::future<> update_configuration(topic_properties);

    const storage::ntp_config& get_ntp_config() const {
        return _raft->log().config();
    }

    ss::shared_ptr<cluster::tm_stm> tm_stm() { return _tm_stm; }

    ss::future<std::vector<rm_stm::tx_range>>
    aborted_transactions(model::offset from, model::offset to) {
        if (!_rm_stm) {
            return ss::make_ready_future<std::vector<rm_stm::tx_range>>(
              std::vector<rm_stm::tx_range>());
        }
        return _rm_stm->aborted_transactions(from, to);
    }

private:
    friend partition_manager;
    friend replicated_partition_probe;

    consensus_ptr raft() { return _raft; }

private:
    consensus_ptr _raft;
    ss::lw_shared_ptr<raft::log_eviction_stm> _nop_stm;
    ss::lw_shared_ptr<cluster::id_allocator_stm> _id_allocator_stm;
    ss::shared_ptr<cluster::rm_stm> _rm_stm;
    ss::shared_ptr<cluster::tm_stm> _tm_stm;
    ss::abort_source _as;
    partition_probe _probe;
    ss::sharded<cluster::tx_gateway_frontend>& _tx_gateway_frontend;
    bool _is_tx_enabled{false};
    bool _is_idempotence_enabled{false};
    // TODO: next two fields should go away when archival snapshot will be
    // integrated
    ss::weak_ptr<cloud_storage::remote_partition> _cloud_storage_partition;
    // end TODO

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

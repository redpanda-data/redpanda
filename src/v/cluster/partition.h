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

#include "archival/fwd.h"
#include "cloud_storage/fwd.h"
#include "cluster/archival_metadata_stm.h"
#include "cluster/distributed_kv_stm.h"
#include "cluster/fwd.h"
#include "cluster/id_allocator_stm.h"
#include "cluster/log_eviction_stm.h"
#include "cluster/partition_probe.h"
#include "cluster/rm_stm.h"
#include "cluster/tm_stm.h"
#include "cluster/types.h"
#include "config/configuration.h"
#include "config/property.h"
#include "container/fragmented_vector.h"
#include "features/feature_table.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "model/record_batch_reader.h"
#include "model/timeout_clock.h"
#include "raft/consensus.h"
#include "raft/consensus_utils.h"
#include "raft/group_configuration.h"
#include "raft/types.h"
#include "storage/translating_reader.h"
#include "storage/types.h"

#include <seastar/core/shared_ptr.hh>

namespace cluster {
class partition_manager;

/// holds cluster logic that is not raft related
/// all raft logic is proxied transparently
class partition {
public:
    partition(
      consensus_ptr r,
      ss::sharded<cloud_storage::remote>&,
      ss::sharded<cloud_storage::cache>&,
      ss::lw_shared_ptr<const archival::configuration>,
      ss::sharded<features::feature_table>&,
      ss::sharded<archival::upload_housekeeping_service>&,
      std::optional<cloud_storage_clients::bucket_name> read_replica_bucket
      = std::nullopt);

    ~partition();

    raft::group_id group() const { return _raft->group(); }
    ss::future<> start(state_machine_registry&);
    ss::future<> stop();

    bool should_construct_archiver();
    /// Part of constructor that we may sometimes need to do again
    /// after a configuration change.
    void maybe_construct_archiver();

    ss::future<result<kafka_result>>
    replicate(model::record_batch_reader&&, raft::replicate_options);

    /// Truncate the beginning of the log up until a given offset
    /// Can only be performed on logs that are deletable and non internal
    ss::future<std::error_code> prefix_truncate(
      model::offset o, kafka::offset ko, ss::lowres_clock::time_point deadline);

    kafka_stages replicate_in_stages(
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
      std::optional<model::timeout_clock::time_point> debounce_deadline
      = std::nullopt) {
        return _raft->make_reader(std::move(config), debounce_deadline);
    }

    ss::future<result<model::offset, std::error_code>>
    sync_kafka_start_offset_override(model::timeout_clock::duration timeout) {
        if (_log_eviction_stm && !is_read_replica_mode_enabled()) {
            auto offset_res
              = co_await _log_eviction_stm->sync_start_offset_override(timeout);
            if (offset_res.has_failure()) {
                co_return offset_res.as_failure();
            }
            // The eviction STM only keeps track of DeleteRecords truncations
            // as Raft offsets. Translate if possible.
            auto offset_translator_state = get_offset_translator_state();
            if (
              offset_res.value() != model::offset{}
              && _raft->start_offset() < offset_res.value()) {
                auto start_kafka_offset
                  = offset_translator_state->from_log_offset(
                    offset_res.value());
                co_return start_kafka_offset;
            }
            // If a start override is no longer in the offset translator state,
            // it may have been uploaded and persisted in the manifest.
        }
        if (_archival_meta_stm) {
            auto term = _raft->term();
            if (!co_await _archival_meta_stm->sync(timeout)) {
                if (term != _raft->term()) {
                    co_return errc::not_leader;
                } else {
                    co_return errc::timeout;
                }
            }
            auto start_kafka_offset = _archival_meta_stm->manifest()
                                        .get_start_kafka_offset_override();
            if (start_kafka_offset != kafka::offset{}) {
                co_return kafka::offset_cast(start_kafka_offset);
            }
        }
        co_return model::offset{};
    }

    model::offset raft_start_offset() const { return _raft->start_offset(); }

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
     *   1) "non-transaction message are considered decided immediately".
     *   Since we currently use the commited_offset to report the end of log to
     *   kafka clients, simply report the next offset.
     *
     *   2) "first offset such that all lower offsets have been decided". this
     *   is describing a strictly greater than relationship.
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
        return model::next_offset(_raft->last_visible_index());
    }

    model::offset leader_high_watermark() const {
        return model::next_offset(_raft->last_leader_visible_index());
    }

    model::term_id term() { return _raft->term(); }

    model::offset dirty_offset() const {
        return _raft->log()->offsets().dirty_offset;
    }

    /// Return the offset up to which the storage layer would like to
    /// prefix truncate the log, if any.  This may be consumed as an indicator
    /// that any truncation-delaying activitiy (like uploading to tiered
    /// storage) could be expedited to enable local disk space to be reclaimed.
    std::optional<model::offset> eviction_requested_offset() {
        if (_log_eviction_stm) {
            return _log_eviction_stm->eviction_requested_offset();
        } else {
            return std::nullopt;
        }
    }

    const model::ntp& ntp() const { return _raft->ntp(); }

    ss::shared_ptr<storage::log> log() const { return _raft->log(); }

    ss::shared_ptr<const cloud_storage::remote_partition>
    remote_partition() const {
        return _cloud_storage_partition;
    }

    ss::future<std::optional<storage::timequery_result>>
      timequery(storage::timequery_config);

    bool is_elected_leader() const { return _raft->is_elected_leader(); }
    bool is_leader() const { return _raft->is_leader(); }
    bool has_followers() const { return _raft->has_followers(); }

    void block_new_leadership() const { _raft->block_new_leadership(); }
    void unblock_new_leadership() const { _raft->unblock_new_leadership(); }

    ss::future<result<model::offset>> linearizable_barrier() {
        return _raft->linearizable_barrier();
    }

    ss::future<std::error_code>
      transfer_leadership(transfer_leadership_request);

    ss::future<std::error_code> update_replica_set(
      std::vector<raft::broker_revision> brokers,
      model::revision_id new_revision_id) {
        return _raft->replace_configuration(
          std::move(brokers), new_revision_id);
    }
    ss::future<std::error_code> update_replica_set(
      std::vector<raft::vnode> nodes,
      model::revision_id new_revision_id,
      std::optional<model::offset> learner_start_offset) {
        return _raft->replace_configuration(
          std::move(nodes), new_revision_id, learner_start_offset);
    }

    ss::future<std::error_code> force_update_replica_set(
      std::vector<raft::vnode> voters,
      std::vector<raft::vnode> learners,
      model::revision_id new_revision_id) {
        return _raft->force_replace_configuration_locally(
          std::move(voters), std::move(learners), new_revision_id);
    }

    raft::group_configuration group_configuration() const {
        return _raft->config();
    }

    partition_probe& probe() { return _probe; }

    model::revision_id get_revision_id() const {
        return _raft->config().revision_id();
    }

    model::revision_id get_log_revision_id() const {
        return _raft->log_config().get_revision();
    }

    std::optional<model::node_id> get_leader_id() const {
        return _raft->get_leader_id();
    }

    std::optional<uint8_t> get_under_replicated() const {
        return _raft->get_under_replicated();
    }

    model::offset get_latest_configuration_offset() const {
        return _raft->get_latest_configuration_offset();
    }

    ss::shared_ptr<cluster::id_allocator_stm> id_allocator_stm() const {
        return _raft->stm_manager()->get<cluster::id_allocator_stm>();
    }

    ss::lw_shared_ptr<const storage::offset_translator_state>
    get_offset_translator_state() const {
        return _raft->get_offset_translator_state();
    }

    ss::shared_ptr<cluster::rm_stm> rm_stm();

    size_t size_bytes() const { return _raft->log()->size_bytes(); }

    size_t reclaimable_size_bytes() const {
        return _raft->log()->reclaimable_size_bytes();
    }

    size_t reclaimable_local_size_bytes() const {
        return _raft->log()->reclaimable_local_size_bytes();
    }

    uint64_t non_log_disk_size_bytes() const;

    ss::future<> update_configuration(topic_properties);

    const storage::ntp_config& get_ntp_config() const {
        return _raft->log()->config();
    }

    ss::shared_ptr<cluster::tm_stm> tm_stm() {
        return _raft->stm_manager()->get<cluster::tm_stm>();
    }

    ss::future<fragmented_vector<rm_stm::tx_range>>
    aborted_transactions(model::offset from, model::offset to) {
        if (!_rm_stm) {
            return ss::make_ready_future<fragmented_vector<rm_stm::tx_range>>(
              fragmented_vector<rm_stm::tx_range>());
        }
        return _rm_stm->aborted_transactions(from, to);
    }

    ss::future<std::vector<rm_stm::tx_range>>
    aborted_transactions_cloud(const cloud_storage::offset_range& offsets);

    model::producer_id highest_producer_id() {
        auto pid = _rm_stm ? _rm_stm->highest_producer_id()
                           : model::producer_id{};
        if (_archival_meta_stm) {
            // It's possible this partition is a recovery partition, in which
            // case the archival metadata may be higher than what's in the
            // rm_stm.
            return std::max(
              pid, _archival_meta_stm->manifest().highest_producer_id());
        }
        return pid;
    }

    const ss::shared_ptr<cluster::archival_metadata_stm>&
    archival_meta_stm() const {
        return _archival_meta_stm;
    }

    bool is_read_replica_mode_enabled() const {
        const auto& cfg = _raft->log_config();
        return cfg.is_read_replica_mode_enabled();
    }

    cloud_storage_clients::bucket_name get_read_replica_bucket() const {
        return _read_replica_bucket.value();
    }

    cloud_storage_mode get_cloud_storage_mode() const;

    partition_cloud_storage_status get_cloud_storage_status() const;

    std::optional<cloud_storage::anomalies> get_cloud_storage_anomalies() const;

    /// Return true if shadow indexing is enabled for the partition
    bool is_remote_fetch_enabled() const;

    /// Check if cloud storage is connected to cluster partition
    ///
    /// The remaining 'cloud' methods can only be called if this
    /// method returned 'true'.
    bool cloud_data_available() const;

    std::optional<uint64_t> cloud_log_size() const;

    /// Starting offset in the object store
    model::offset start_cloud_offset() const;

    /// Kafka offset one past the end of the last offset (i.e. the high
    /// watermark as reported by object storage).
    model::offset next_cloud_offset() const;

    /// Create a reader that will fetch data from remote storage
    ss::future<storage::translating_reader> make_cloud_reader(
      storage::log_reader_config config,
      std::optional<model::timeout_clock::time_point> deadline = std::nullopt);

    std::optional<model::offset> kafka_start_offset_override() const {
        if (_log_eviction_stm && !is_read_replica_mode_enabled()) {
            auto o = _log_eviction_stm->start_offset_override();
            if (o != model::offset{} && _raft->start_offset() < o) {
                auto offset_translator_state = get_offset_translator_state();
                auto start_kafka_offset
                  = offset_translator_state->from_log_offset(o);
                return start_kafka_offset;
            }
            // If a start override is no longer in the offset translator state,
            // it may have been uploaded and persisted in the manifest.
        }
        if (_archival_meta_stm) {
            auto o = _archival_meta_stm->manifest()
                       .get_start_kafka_offset_override();
            if (o != kafka::offset{}) {
                return kafka::offset_cast(o);
            }
        }
        return std::nullopt;
    }

    ss::future<> remove_persistent_state();
    ss::future<> finalize_remote_partition(ss::abort_source& as);

    std::optional<model::offset> get_term_last_offset(model::term_id) const;

    model::term_id get_term(model::offset o) const {
        return _raft->get_term(o);
    }

    ss::future<std::optional<model::offset>>
    get_cloud_term_last_offset(model::term_id term) const;

    ss::future<std::error_code>
    cancel_replica_set_update(model::revision_id rev) {
        return _raft->cancel_configuration_change(rev);
    }

    ss::future<std::error_code>
    force_abort_replica_set_update(model::revision_id rev) {
        return _raft->abort_configuration_change(rev);
    }

    consensus_ptr raft() const { return _raft; }

    std::optional<std::reference_wrapper<archival::ntp_archiver>> archiver() {
        if (_archiver) {
            return *_archiver;
        } else {
            return std::nullopt;
        }
    }

    uint64_t upload_backlog_size() const;

    /**
     * Partition 0 carries a copy of the topic configuration, updated by
     * the controller, so that its archiver can make updates to the topic
     * manifest in cloud storage
     */
    void set_topic_config(std::unique_ptr<cluster::topic_configuration> cfg);

    // If the partition is enabled for cloud storage, serialize the manifest to
    // an ss::output_stream in JSON format. Otherwise, throw an
    // std::runtime_error.
    //
    // If the serialization does not complete within
    // manifest_serialization_timeout, a ss::timed_out_error is thrown.
    //
    //
    // Note that the caller must keep the stream alive until the future
    // completes.
    static constexpr std::chrono::seconds manifest_serialization_timeout{3s};
    ss::future<>
    serialize_json_manifest_to_output_stream(ss::output_stream<char>& output);

    std::optional<std::reference_wrapper<cluster::topic_configuration>>
    get_topic_config() {
        if (_topic_cfg) {
            return std::ref(*_topic_cfg);
        } else {
            return std::nullopt;
        }
    }

    ss::sharded<features::feature_table>& feature_table() const {
        return _feature_table;
    }

    result<std::vector<raft::follower_metrics>> get_follower_metrics() const;

    // Attempt to reset the partition manifest of a cloud storage partition
    // from an iobuf containing the JSON representation of the manifest.
    //
    // Warning: in order to call this safely, one must stop the archiver
    // manually whilst ensuring that the max collectible offset reported
    // by the archival metadata STM remains stable. Prefer its sibling
    // which resets from the cloud state.
    //
    // Returns a failed future if unsuccessful.
    ss::future<>
    unsafe_reset_remote_partition_manifest_from_json(iobuf json_buf);

    // Attempt to reset the partition manifest of a cloud storage partition
    // to the one last uploaded to cloud storage.
    //
    // If `force` is true, the safety checks will be disregarded, which
    // may lead to data loss.
    //
    // Returns a failed future if unsuccessful.
    ss::future<> unsafe_reset_remote_partition_manifest_from_cloud(bool force);

private:
    ss::future<>
    replicate_unsafe_reset(cloud_storage::partition_manifest manifest);

    ss::future<>
    do_unsafe_reset_remote_partition_manifest_from_cloud(bool force);

    ss::future<std::optional<storage::timequery_result>>
      cloud_storage_timequery(storage::timequery_config);

    bool may_read_from_cloud() const;

    ss::future<std::optional<storage::timequery_result>>
      local_timequery(storage::timequery_config);

    consensus_ptr _raft;
    ss::shared_ptr<cluster::log_eviction_stm> _log_eviction_stm;
    ss::shared_ptr<cluster::rm_stm> _rm_stm;
    ss::shared_ptr<archival_metadata_stm> _archival_meta_stm;
    ss::abort_source _as;
    partition_probe _probe;
    ss::sharded<features::feature_table>& _feature_table;
    ss::lw_shared_ptr<const archival::configuration> _archival_conf;
    ss::sharded<cloud_storage::remote>& _cloud_storage_api;
    ss::sharded<cloud_storage::cache>& _cloud_storage_cache;
    ss::shared_ptr<cloud_storage::partition_probe> _cloud_storage_probe;
    ss::shared_ptr<cloud_storage::async_manifest_view>
      _cloud_storage_manifest_view;
    ss::shared_ptr<cloud_storage::remote_partition> _cloud_storage_partition;

    static constexpr auto archiver_reset_mutex_timeout = 10s;
    ssx::semaphore _archiver_reset_mutex{1, "archiver_reset"};
    std::unique_ptr<archival::ntp_archiver> _archiver;

    std::optional<cloud_storage_clients::bucket_name> _read_replica_bucket{
      std::nullopt};
    bool _remote_delete_enabled{storage::ntp_config::default_remote_delete};

    // Populated for partition 0 only, used by cloud storage uploads
    // to generate topic manifests.
    std::unique_ptr<cluster::topic_configuration> _topic_cfg;

    ss::sharded<archival::upload_housekeeping_service>& _upload_housekeeping;

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

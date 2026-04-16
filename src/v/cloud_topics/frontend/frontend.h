/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#pragma once

#include "base/format_to.h"
#include "base/outcome.h"
#include "cloud_topics/frontend/errc.h"
#include "cloud_topics/level_one/metastore/metastore.h"
#include "cloud_topics/level_zero/stm/ctp_stm_api.h"
#include "cloud_topics/log_reader_config.h"
#include "cloud_topics/types.h"
#include "model/fundamental.h"
#include "model/timeout_clock.h"
#include "raft/types.h"
#include "storage/translating_reader.h"
#include "storage/types.h"
#include "utils/retry_chain_node.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/core/sharded.hh>

#include <expected>
#include <optional>
#include <system_error>

namespace cluster {
class partition;
}

namespace kafka {
class write_at_offset_stm;
} // namespace kafka

namespace cloud_topics {
class data_plane_api;
class ctp_stm_api;

struct replica_info {
    model::node_id id;
    kafka::offset high_watermark;
    kafka::offset log_end_offset;
    bool is_alive;
};

struct partition_info {
    std::vector<replica_info> replicas;
    std::optional<model::node_id> leader;
};

/// CloudTopics entry point for the read and write paths.
/// The frontend handles requests for one particular NTP. There could be
/// multiple frontends created per NTP. E.g. one per request. This
/// implementation follows kafka::data::partition_proxy interface with some
/// minor differences but it doesn't depend on kafka layer. It's supposed to be
/// used to implement partition_proxy in the kafka layer.
///
/// This class serves as the entry point into the cloud-topics (CT) subsystem,
/// which comprises two main components: the data plane and the metadata layer.
///
/// Data Plane:
/// - Accessible via the 'cloud_topics::app' instance passed through the
///   constructor.
/// - Contains 'l0::read_pipeline' and 'l0::write_pipeline'.
///
/// Metadata layer:
/// - Composed of 'cluster::partition' and 'metastore' components
///
/// Write Request Path:
/// - Batch is pushed to the data plane (app::write_and_debounce method).
/// - Data plane returns a placeholder for the record batch, containing
///   metadata to locate data in cloud storage.
/// - 'cloud_topic_partition' pushes the placeholder to the metadata layer by
///   replicating 'ctp_placeholder' batch.
///
/// Read Request Path:
/// - 'ctp_placeholder' batches are queried from the metadata layer, fetched
///   from 'cluster::partition'.
/// - Includes information about aborted transactions.
/// - 'ctp_placeholder' batches are 'materialized' using the data plane.
///
/// Currently, the data plane is explicitly a sharded service. The control
/// plane includes 'cluster::partition' and 'ctp_stm', with no explicit API
/// boundary. However, component use is limited to allow future
/// introduction of such an API.
///
class frontend final {
public:
    explicit frontend(
      ss::lw_shared_ptr<cluster::partition> p, data_plane_api* ct) noexcept;

    /// Get current NTP
    const model::ntp& ntp() const;

    ss::future<std::expected<kafka::offset, frontend_errc>>
    sync_effective_start(
      model::timeout_clock::duration timeout, ss::abort_source& as);
    ss::future<std::expected<kafka::offset, frontend_errc>>
    sync_effective_start(
      model::timeout_clock::time_point deadline, ss::abort_source& as);

    /// This method defines starting offset for translation in data-lake
    /// subsystem
    kafka::offset local_start_offset() const;

    /// Logical start offset
    kafka::offset start_offset() const;

    /// HWM (from underlying partition)
    kafka::offset high_watermark() const;

    /// Current LSO value (underlying partition)
    std::expected<kafka::offset, frontend_errc> last_stable_offset() const;

    /// Returns true if underlying partition is a leader
    bool is_leader() const;

    ss::future<std::expected<void, frontend_errc>>
      prefix_truncate(kafka::offset, ss::lowres_clock::time_point);

    ss::future<std::optional<storage::timequery_result>>
    timequery(storage::timequery_config cfg);

    ss::future<std::expected<kafka::offset, std::error_code>>
      replicate(chunked_vector<model::record_batch>, raft::replicate_options);

    raft::replicate_stages replicate(
      model::batch_identity, model::record_batch, raft::replicate_options);

    /// Upload batches to object storage, generate placeholders, then
    /// replicate the placeholders through the provided write_at_offset_stm.
    /// Used by cluster linking's exact_offset_replicator implementation.
    ss::future<result<raft::replicate_result>> replicate_at_offset(
      chunked_vector<model::record_batch>,
      chunked_vector<kafka::offset> expected_base_offsets,
      std::optional<kafka::offset> prev_log_offset,
      model::timeout_clock::duration timeout,
      std::optional<std::reference_wrapper<ss::abort_source>> as,
      ss::shared_ptr<kafka::write_at_offset_stm> stm);

    ss::future<storage::translating_reader>
    make_reader(cloud_topic_log_reader_config cfg);

    ss::future<std::vector<model::tx_range>> aborted_transactions(
      kafka::offset base,
      kafka::offset last,
      ss::lw_shared_ptr<const storage::offset_translator_state>);

    ss::future<std::optional<kafka::offset>>
      get_leader_epoch_last_offset(model::term_id) const;

    model::term_id leader_epoch() const;

    ss::future<std::expected<std::monostate, frontend_errc>>
    validate_fetch_offset(
      kafka::offset, bool, model::timeout_clock::time_point);

    std::expected<partition_info, frontend_errc> get_partition_info() const;

    size_t estimate_size_between(kafka::offset, kafka::offset) const;

    ss::future<std::error_code> linearizable_barrier();

    ss::future<std::optional<l1::metastore::size_response>> l1_size();
    ss::future<size_t> size_bytes();

    /// Get the current cluster epoch
    ss::future<std::expected<cloud_topics::cluster_epoch, frontend_errc>>
    get_current_epoch(ss::abort_source& as) noexcept;

    /// Epoch state snapshot returned by advance_epoch.
    struct epoch_info {
        cluster_epoch estimated_inactive_epoch;
        cluster_epoch max_applied_epoch;
        model::offset last_reconciled_log_offset;
        model::offset current_epoch_window_offset;
        friend auto operator<=>(const epoch_info&, const epoch_info&) = default;
    };

    /// Return current epoch state.
    epoch_info get_epoch_info() const;

    /// Advance the partition to the current cluster epoch and return epoch
    /// state.
    ss::future<std::expected<epoch_info, frontend_errc>> advance_epoch(
      cloud_topics::cluster_epoch, model::timeout_clock::time_point);

    /// Return the estimated active L0 bytes for this partition.
    uint64_t get_l0_size_estimate() const;

private:
    // All timequeries work by first getting a coarse grained timequery result
    // from metadata indexes, then getting an exact answer using the datapath.
    struct coarse_grained_timequery_result {
        model::timestamp time;
        kafka::offset start_offset;
        kafka::offset last_offset;

        fmt::iterator format_to(fmt::iterator) const;
    };

    // Determine the offset using the course grained index in local
    // storage to find out which L0 offset we should start reading from.
    ss::future<std::optional<coarse_grained_timequery_result>>
    l0_timequery(storage::timequery_config cfg);

    // Determine the offset using the course gained index in the metastore to
    // find out which L1 batch we should start reading from.
    ss::future<std::optional<coarse_grained_timequery_result>>
    l1_timequery(storage::timequery_config cfg);

    // Create a reader to find the exact offset for a timequery.
    ss::future<std::optional<storage::timequery_result>>
      refine_timequery_result(
        coarse_grained_timequery_result, model::opt_abort_source_t);

    raft::replicate_stages upload_and_replicate(
      model::batch_identity batch_id,
      model::record_batch,
      raft::replicate_options);

    kafka::offset get_log_end_offset() const;

    bool cache_enabled() const;

    /// Returns nullopt when the topic config has been removed from the
    /// topic_table (e.g. during deletion) but the partition has not yet
    /// been shut down.
    std::optional<model::topic_id_partition> topic_id_partition() const;

    std::unique_ptr<model::record_batch_reader::impl>
    make_l0_reader(const cloud_topic_log_reader_config& cfg) const;

    std::unique_ptr<model::record_batch_reader::impl> make_l1_reader(
      const cloud_topic_log_reader_config& cfg,
      model::topic_id_partition tidp) const;

    ss::lw_shared_ptr<cluster::partition> _partition;
    data_plane_api* _data_plane;
    ss::lw_shared_ptr<cloud_topics::ctp_stm_api> _ctp_stm_api;
};

} // namespace cloud_topics

/*
 * Copyright 2021 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */
#pragma once

#include "base/outcome.h"
#include "cluster/fwd.h"
#include "cluster/types.h"
#include "kafka/data/exact_offset_replicator.h"
#include "kafka/data/log_reader_config.h"
#include "kafka/protocol/errors.h"
#include "kafka/protocol/types.h"
#include "model/fundamental.h"
#include "model/ktp.h"
#include "raft/replicate.h"
#include "storage/translating_reader.h"
#include "storage/types.h"

#include <optional>
#include <system_error>

namespace kafka {

/**
 * Describes single partition replica. Used by replica selector
 */
struct replica_info {
    model::node_id id;
    model::offset high_watermark;
    model::offset log_end_offset;
    bool is_alive;
};

struct partition_info {
    std::vector<replica_info> replicas;
    std::optional<model::node_id> leader;
};

/**
 * Kafka layer wrapper for partition, this wrapper performs offsets translation
 * to eliminate offset skew caused by existantece of Raft configuration batches.
 */
class partition_proxy {
public:
    struct impl {
        impl() = default;
        impl(const impl&) = default;
        impl& operator=(const impl&) = default;
        impl(impl&&) = default;
        impl& operator=(impl&&) = default;
        virtual ~impl() noexcept = default;

        virtual const model::ntp& ntp() const = 0;
        virtual ss::future<result<model::offset, error_code>>
          sync_effective_start(model::timeout_clock::duration) = 0;
        virtual model::offset local_start_offset() const = 0;
        virtual model::offset start_offset() const = 0;
        virtual model::offset high_watermark() const = 0;
        virtual checked<model::offset, error_code>
        last_stable_offset() const = 0;
        virtual kafka::leader_epoch leader_epoch() const = 0;
        virtual ss::future<std::optional<model::offset>>
          get_leader_epoch_last_offset(kafka::leader_epoch) const = 0;

        virtual bool is_leader() const = 0;
        virtual ss::future<std::error_code> linearizable_barrier() = 0;
        virtual ss::future<error_code>
          prefix_truncate(model::offset, ss::lowres_clock::time_point) = 0;
        virtual ss::future<storage::translating_reader>
          make_reader(kafka::log_reader_config) = 0;
        virtual ss::future<std::optional<storage::timequery_result>>
          timequery(storage::timequery_config) = 0;
        virtual ss::future<std::vector<model::tx_range>> aborted_transactions(
          model::offset,
          model::offset,
          ss::lw_shared_ptr<const storage::offset_translator_state>) = 0;
        virtual ss::future<error_code> validate_fetch_offset(
          model::offset, bool, model::timeout_clock::time_point) = 0;

        virtual ss::future<result<model::offset>> replicate(
          chunked_vector<model::record_batch>, raft::replicate_options) = 0;
        virtual raft::replicate_stages replicate(
          model::batch_identity,
          model::record_batch,
          raft::replicate_options) = 0;

        /// Returns a replicator for writing batches at exact offsets,
        /// or nullptr if the partition does not support this operation.
        virtual std::unique_ptr<exact_offset_replicator>
        make_exact_offset_replicator() && = 0;

        virtual result<partition_info> get_partition_info() const = 0;
        virtual size_t
          estimate_size_between(kafka::offset, kafka::offset) const = 0;
        virtual cluster::partition_probe& probe() = 0;

        virtual size_t local_size_bytes() const = 0;
        virtual ss::future<std::optional<size_t>> cloud_size_bytes() const = 0;
        virtual model::offset offset_lag() const = 0;
        virtual ss::future<cluster::partition_cloud_storage_status>
        get_cloud_storage_status() const = 0;
    };

    explicit partition_proxy(std::unique_ptr<impl> impl) noexcept
      : _impl(std::move(impl)) {}

    ss::future<result<model::offset, error_code>> sync_effective_start(
      model::timeout_clock::duration timeout = std::chrono::seconds(5)) {
        return _impl->sync_effective_start(timeout);
    }

    model::offset local_start_offset() const {
        return _impl->local_start_offset();
    }

    model::offset start_offset() const { return _impl->start_offset(); }

    model::offset high_watermark() const { return _impl->high_watermark(); }

    checked<model::offset, error_code> last_stable_offset() const {
        return _impl->last_stable_offset();
    }

    ss::future<std::error_code> linearizable_barrier() {
        return _impl->linearizable_barrier();
    }

    ss::future<error_code>
    prefix_truncate(model::offset o, ss::lowres_clock::time_point deadline) {
        return _impl->prefix_truncate(o, deadline);
    }

    bool is_leader() const { return _impl->is_leader(); }

    const model::ntp& ntp() const { return _impl->ntp(); }

    ss::future<std::vector<model::tx_range>> aborted_transactions(
      model::offset base,
      model::offset last,
      ss::lw_shared_ptr<const storage::offset_translator_state> ot_state) {
        return _impl->aborted_transactions(base, last, std::move(ot_state));
    }

    ss::future<storage::translating_reader>
    make_reader(kafka::log_reader_config cfg) {
        return _impl->make_reader(cfg);
    }

    ss::future<std::optional<storage::timequery_result>>
    timequery(storage::timequery_config cfg) {
        return _impl->timequery(cfg);
    }

    cluster::partition_probe& probe() { return _impl->probe(); }

    kafka::leader_epoch leader_epoch() const { return _impl->leader_epoch(); }

    ss::future<std::optional<model::offset>>
    get_leader_epoch_last_offset(kafka::leader_epoch epoch) const {
        return _impl->get_leader_epoch_last_offset(epoch);
    }

    ss::future<error_code> validate_fetch_offset(
      model::offset o,
      bool is_follower,
      model::timeout_clock::time_point deadline) {
        return _impl->validate_fetch_offset(o, is_follower, deadline);
    }

    result<partition_info> get_partition_info() const {
        return _impl->get_partition_info();
    }

    size_t estimate_size_between(kafka::offset begin, kafka::offset end) const {
        return _impl->estimate_size_between(begin, end);
    }

    ss::future<result<model::offset>> replicate(
      chunked_vector<model::record_batch> batches,
      raft::replicate_options opts) const {
        return _impl->replicate(std::move(batches), opts);
    }

    raft::replicate_stages replicate(
      model::batch_identity bi,
      model::record_batch batch,
      raft::replicate_options opts) {
        return _impl->replicate(bi, std::move(batch), opts);
    }

    std::unique_ptr<exact_offset_replicator> make_exact_offset_replicator() && {
        return std::move(*_impl).make_exact_offset_replicator();
    }

    /*
     * Returns the local on-disk size of the partition.
     */
    size_t local_size_bytes() const { return _impl->local_size_bytes(); }

    /*
     * Returns the size of the partition in cloud storage. For example if this
     * partition is a tiered storage partition the manifest will be used to
     * compute the size of all segments. This method is used to drive
     * dashboards, so it should reflect the addressable size of a partition, and
     * generally should not include data that is unreadable (e.g. data that has
     * been logically deleted by retention but not yet garbage collected).
     */
    ss::future<std::optional<size_t>> cloud_size_bytes() const {
        return _impl->cloud_size_bytes();
    }

    /*
     * This returns the distance between the largest offset fully replicated and
     * the end of the log. With acks=all the expectation is that this should be
     * zero. It's calculated as the highwater mark minus the dirty offset.
     */
    model::offset offset_lag() const { return _impl->offset_lag(); }

    ss::future<cluster::partition_cloud_storage_status>
    get_cloud_storage_status() const {
        return _impl->get_cloud_storage_status();
    }

private:
    std::unique_ptr<impl> _impl;
};

partition_proxy
make_partition_proxy(const ss::lw_shared_ptr<cluster::partition>&);

std::optional<partition_proxy>
make_partition_proxy(const model::ktp&, cluster::partition_manager&);

std::optional<partition_proxy>
make_partition_proxy(const model::ntp&, cluster::partition_manager&);

} // namespace kafka

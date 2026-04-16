/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#pragma once

#include "cloud_topics/level_one/metastore/metastore.h"
#include "cloud_topics/read_replica/partition_metadata.h"
#include "kafka/data/partition_proxy.h"
#include "kafka/protocol/errors.h"
#include "model/fundamental.h"

#include <seastar/core/future.hh>
#include <seastar/core/shared_ptr.hh>

#include <expected>

namespace cluster {
class partition;
class partition_probe;
} // namespace cluster

namespace cloud_topics {
class state_accessors;
class l1_reader_cache;
class level_one_reader_probe;
namespace l1 {
class io;
} // namespace l1
} // namespace cloud_topics

namespace cloud_topics::read_replica {

class metadata_provider;
class snapshot_provider;
class snapshot_metastore;
class stm;

// Implements the kafka::partition_proxy interface for read replica cloud topic
// partitions. This provides read-only access to data stored in the source
// cluster's cloud database.
//
// Synchronous state (offsets, leader epoch) comes from the STM, which
// replicates metadata discovered from the cloud database. Asynchronous
// operations (make_reader, timequery) use the metadata_manager and
// snapshot_manager to access cloud database snapshots.
class partition_proxy final : public kafka::partition_proxy::impl {
public:
    partition_proxy(
      ss::lw_shared_ptr<cluster::partition> partition,
      ss::shared_ptr<stm> stm,
      cloud_topics::state_accessors* state);

    // Returns the NTP for this partition.
    const model::ntp& ntp() const final;

    // Syncs the STM and returns the effective start offset.
    ss::future<result<model::offset, kafka::error_code>>
    sync_effective_start(model::timeout_clock::duration timeout) final;

    // Not supported for read replicas.
    model::offset local_start_offset() const final;

    // Returns the start offset from the STM state.
    model::offset start_offset() const final;

    // Returns the high watermark from the STM state.
    model::offset high_watermark() const final;

    // Returns the last stable offset (same as high_watermark for read replicas
    // since transactions are not supported).
    checked<model::offset, kafka::error_code> last_stable_offset() const final;

    // Returns the leader epoch from the source partition's term.
    kafka::leader_epoch leader_epoch() const final;

    // Queries the cloud database for the last offset in the given epoch.
    ss::future<std::optional<model::offset>>
    get_leader_epoch_last_offset(kafka::leader_epoch epoch) const final;

    // Returns whether this replica is the leader.
    bool is_leader() const final;

    // Delegates to the underlying partition's linearizable barrier.
    ss::future<std::error_code> linearizable_barrier() final;

    // Not supported for read replicas (read-only).
    ss::future<kafka::error_code>
      prefix_truncate(model::offset, ss::lowres_clock::time_point) final;

    // Creates a reader from the cloud database snapshot.
    ss::future<storage::translating_reader>
    make_reader(kafka::log_reader_config cfg) final;

    // Queries the cloud database for the offset at the given timestamp.
    ss::future<std::optional<storage::timequery_result>>
    timequery(storage::timequery_config cfg) final;

    // Returns empty - cloud topics don't support transactions.
    ss::future<std::vector<model::tx_range>> aborted_transactions(
      model::offset base,
      model::offset last,
      ss::lw_shared_ptr<const storage::offset_translator_state>) final;

    // Validates that the fetch offset is within bounds.
    ss::future<kafka::error_code> validate_fetch_offset(
      model::offset, bool, model::timeout_clock::time_point) final;

    // Not supported for read replicas (read-only).
    ss::future<result<model::offset>> replicate(
      chunked_vector<model::record_batch>, raft::replicate_options) final;

    // Not supported for read replicas (read-only).
    raft::replicate_stages replicate(
      model::batch_identity,
      model::record_batch,
      raft::replicate_options) final;

    std::unique_ptr<kafka::exact_offset_replicator>
      make_exact_offset_replicator() && final;

    // Returns partition info built from raft state.
    result<kafka::partition_info> get_partition_info() const final;

    // Not supported for read replicas.
    size_t estimate_size_between(kafka::offset, kafka::offset) const final;

    // Returns the partition probe.
    cluster::partition_probe& probe() final;

    size_t local_size_bytes() const final;
    ss::future<std::optional<size_t>> cloud_size_bytes() const final;
    model::offset offset_lag() const final;
    ss::future<cluster::partition_cloud_storage_status>
    get_cloud_storage_status() const final;

private:
    ss::future<std::optional<cloud_topics::l1::metastore::size_response>>
    get_metastore_size() const;

    struct snapshot {
        partition_metadata metadata;
        std::unique_ptr<snapshot_metastore> metastore;
        l1::io* io;
    };
    ss::future<std::expected<snapshot, ss::sstring>> get_snapshot() const;

    ss::future<storage::translating_reader>
    make_reader(snapshot, kafka::log_reader_config cfg);

    ss::lw_shared_ptr<cluster::partition> partition_;
    ss::shared_ptr<stm> stm_;
    metadata_provider* metadata_provider_;
    snapshot_provider* snapshot_provider_;
    cloud_topics::level_one_reader_probe* l1_reader_probe_;
    cloud_topics::l1_reader_cache* l1_reader_cache_;
};

} // namespace cloud_topics::read_replica

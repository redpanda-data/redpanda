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

#include "cloud_topics/level_one/metastore/metastore.h"
#include "cloud_topics/types.h"
#include "config/property.h"
#include "model/fundamental.h"

#include <seastar/core/gate.hh>

#include <chrono>

namespace cloud_topics {

// Housekeeper is a component responsible for managing retention in cloud
// topics, primarily for L1 data. There is one housekeeper per CTP and it's
// managed by the cloud_topics_manager.
//
// L1 retention: L1 retention is computed by periodically querying the
// metastore, then propagating the updated start offset to L0 metadata storage
// (i.e. l0::ctp_stm). We propagate to L0 so that the fetch path doesn't need to
// make a RPC in order to service list offset requests. There is an additional
// Delete Records Kafka RPC that can also advance the start offset for the
// partition. For this reason, the start offset is also back-propagated to L1 at
// the end of housekeeping. By centralizing the start offset, we can give
// read-your-own-write consistency for delete records, and also have a single
// source of truth for pushing the new start offset to the L1 metastore.
//
// L0 retention: This is managed entirely by l0::ctp_stm based on the process
// the reconciler has made.
class housekeeper {
public:
    // An abstraction for partition metadata storage in L0, this is the source
    // of truth for the start offset in the partition.
    class l0_metadata_storage {
    public:
        l0_metadata_storage() = default;
        virtual ~l0_metadata_storage() = default;

        // Get the current start offset for the partition.
        virtual kafka::offset
        get_start_offset(const model::topic_id_partition&) = 0;

        // Get the current last reconciled offset for the partition.
        virtual kafka::offset
        get_last_reconciled_offset(const model::topic_id_partition&) = 0;

        // Update the start offset to the partition, this must be an
        // idempotent operation.
        virtual ss::future<> set_start_offset(
          const model::topic_id_partition&,
          kafka::offset,
          ss::abort_source*) = 0;

        // Get the highest start offset we're allowed to prefix truncate to.
        virtual kafka::offset
        get_max_allowed_start_offset(const model::topic_id_partition&) = 0;

        virtual std::optional<cloud_topics::cluster_epoch>
        estimate_inactive_epoch(const model::topic_id_partition&) noexcept = 0;

        virtual ss::future<std::optional<cloud_topics::cluster_epoch>>
        get_current_cluster_epoch(
          const model::topic_id_partition&, ss::abort_source*) noexcept = 0;

        virtual ss::future<> advance_epoch(
          const model::topic_id_partition& tidp,
          cloud_topics::cluster_epoch,
          ss::abort_source*) noexcept = 0;

        virtual ss::future<> sync_to_next_placeholder(
          const model::topic_id_partition& tidp,
          ss::abort_source*) noexcept = 0;
    };

    // A wrapper around a source of configuration for a give topic id +
    // partition.
    class retention_configuration {
    public:
        virtual ~retention_configuration() = default;

        // The amount of bytes to retain in the partition.
        virtual std::optional<size_t>
        retention_bytes(const model::topic_id_partition&) = 0;

        // How old of data to retain in the partition.
        virtual std::optional<std::chrono::milliseconds>
        retention_duration(const model::topic_id_partition&) = 0;
    };

    housekeeper(
      model::topic_id_partition,
      l0_metadata_storage*,
      l1::metastore*,
      retention_configuration*,
      config::binding<std::chrono::milliseconds> loop_interval);
    housekeeper(const housekeeper&) = delete;
    housekeeper(housekeeper&&) = delete;
    housekeeper& operator=(const housekeeper&) = delete;
    housekeeper& operator=(housekeeper&&) = delete;
    ~housekeeper() = default;

    // Start the housekeeping loop.
    ss::future<> start();

    // Stop the housekeeping loop. Must be called before destructing if start
    // was called.
    ss::future<> stop();

    // Run a single iteration of the housekeeping loop.
    //
    // Public for testing.
    ss::future<> do_housekeeping();

    ss::future<> do_bump_epoch();

private:
    ss::future<> do_loop();
    ss::future<kafka::offset> do_bytes_retention(size_t size);
    ss::future<kafka::offset> do_time_retention(std::chrono::milliseconds);
    // Syncs the start offset from L0 metadata storage to L1 metastore.
    // This ensures that L1 has the most up-to-date start offset, including
    // any updates from Delete Records requests.
    ss::future<> sync_start_offset();

    model::topic_id_partition _tidp;
    l0_metadata_storage* _l0_metastore;
    l1::metastore* _l1_metastore;
    retention_configuration* _config;
    config::binding<std::chrono::milliseconds> _loop_interval;
    ss::gate _gate;
    ss::abort_source _as;

    std::optional<cloud_topics::cluster_epoch> _last_epoch;
    std::optional<kafka::offset> _last_synced_start_offset;
};

} // namespace cloud_topics

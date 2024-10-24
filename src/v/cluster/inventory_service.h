/*
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

#include "cloud_storage/inventory/inv_ops.h"
#include "cloud_storage/remote.h"
#include "cluster/partition_leaders_table.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/lowres_clock.hh>
#include <seastar/core/sharded.hh>

#include <chrono>

namespace cluster {

// The remote provider and leader provider structs are wrappers used to
// facilitate testing. In real code the default versions below will be used. In
// test code mocks and static values are used.
struct remote_provider {
    virtual cloud_storage::cloud_storage_api& ref() = 0;
    virtual ~remote_provider() = default;
};

struct default_remote_provider final : remote_provider {
    explicit default_remote_provider(
      ss::sharded<cloud_storage::remote>& remote);

    cloud_storage::cloud_storage_api& ref() override;

private:
    ss::sharded<cloud_storage::remote>& _remote;
};

struct leaders_provider {
    virtual ss::future<absl::node_hash_set<model::ntp>>
    ntps(ss::abort_source&) = 0;
    virtual ~leaders_provider() = default;
};

struct default_leaders_provider final : leaders_provider {
    explicit default_leaders_provider(
      ss::sharded<partition_leaders_table>& leaders_table);
    ss::future<absl::node_hash_set<model::ntp>>
    ntps(ss::abort_source&) override;

private:
    ss::sharded<partition_leaders_table>& _leaders_table;
};

/// The inventory service periodically downloads inventory reports from the
/// storage bucket and processes them using the report consumer, writing hash
/// files to disk for all NTPs which have leaders on this node. The service runs
/// on shard 0. At startup the service also checks if the inventory
/// configuration exists, and if not creates it.
class inventory_service : public ss::sharded<inventory_service> {
public:
    static constexpr unsigned shard_id{0};
    inventory_service(
      std::filesystem::path hash_store_path,
      std::shared_ptr<leaders_provider> leaders,
      std::shared_ptr<remote_provider> remote,
      cloud_storage::inventory::inv_ops inv_ops,
      ss::lowres_clock::duration inventory_report_check_interval
      = std::chrono::hours{6},
      bool should_create_report_config = true);

    ss::future<> start();
    ss::future<> stop();

    /// A query method that signals that data written by this service to disk is
    /// consistent and ready to use. Scrubbers must query this service first
    /// before reading data from disk. Before processing a report we set this
    /// flag to false, and once the report is successfully parsed and data
    /// written to disk, this flag is set to true.
    bool can_use_inventory_data() const { return _can_use_inventory_data; }

private:
    /// Attempts to create inventory configuration (which schedules the report
    /// to be generated). Returns true if the config was created. Note that this
    /// also returns true if the config was created by another node, the return
    /// value lets us know if the inventory schedule creation should be retried
    /// later.
    ss::future<bool> maybe_create_inventory_config();

    /// Checks for the existence of an inventory report on a set frequency. If a
    /// report is found, it's date is checked to compare with the last date
    /// processed by this node. If the date is newer (or we have not processed
    /// any reports since startup) the report is downloaded and written to disk
    /// in the form of path hashes per NTP. Before processing data a flag is set
    /// to signal that data on disk is not ready. After successful processing
    /// the flag marks data ready for use.
    ss::future<> check_for_current_inventory();

    ss::future<bool>
    download_and_process_reports(cloud_storage::inventory::report_paths paths);

    void mark_processing_complete();
    void begin_processing();

    /// Before processing a report, we remove all previous hash files from disk.
    /// Since the report consumer writes hash files by overwriting exisiting
    /// files, even if this step fails we will not see an increased space usage.
    /// This step is still necessary to make sure stale files from previous runs
    /// of the report consumer are not used. For example if a previous
    /// invocation of report consumer (which produces files numbered in
    /// sequence) produced files numbered 0 through 20, and the current
    /// invocation will produce files 0 through 19, the scrubber will
    /// incorrectly read data of file 20 from the last invocation, producing
    /// false positives. To avoid this we delete all files for all NTPs up front
    /// before processing.
    ss::future<> clean_up_hash_directory();

    std::filesystem::path _hash_store_path;
    std::shared_ptr<leaders_provider> _leaders;
    std::shared_ptr<remote_provider> _remote;
    cloud_storage::inventory::inv_ops _ops;

    ss::gate _gate;
    ss::abort_source _as;
    retry_chain_node _rtc;
    bool _retry_creating_inv_config{false};

    ss::timer<ss::lowres_clock> _report_check_timer;
    std::optional<cloud_storage::inventory::report_datetime> _last_fetched;

    ss::lowres_clock::duration _inventory_report_check_interval;

    bool _can_use_inventory_data{false};

    // Controls if redpanda will create the report schedule on which the report
    // is generated by cloud storage provider. If this value is false and the
    // service is running, the schedule will not be created on service startup,
    // but the report will still be checked for by the service in the expected
    // path periodically. The expectation then is that the report will be
    // manually placed in the bucket by the user. For use in testing and
    // deployments where the cloud provider does not support inventory reports
    // but reports can be generated externally, and inventory based scrub is
    // still desired.
    bool _should_create_report_config{true};
};

} // namespace cluster

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

class inventory_service : public ss::sharded<inventory_service> {
public:
    inventory_service(
      std::filesystem::path hash_store_path,
      std::shared_ptr<leaders_provider> leaders,
      std::shared_ptr<remote_provider> remote,
      cloud_storage::inventory::inv_ops inv_ops,
      ss::lowres_clock::duration inventory_report_check_interval
      = std::chrono::hours{6});

    ss::future<> start();
    ss::future<> stop();

private:
    ss::future<> check_for_current_inventory();

    ss::future<bool>
    download_and_process_reports(cloud_storage::inventory::report_paths paths);

    // Emplaces marker which signals to scrubbers that data is ready for use.
    ss::future<>
      mark_processing_complete(cloud_storage::inventory::report_datetime) const;

    // Removes the marker from disk which will be used by scrubbers to decide if
    // data is complete and safe to use.
    ss::future<> begin_processing() const;

    std::filesystem::path _hash_store_path;
    std::shared_ptr<leaders_provider> _leaders;
    std::shared_ptr<remote_provider> _remote;
    cloud_storage::inventory::inv_ops _ops;

    ss::gate _gate;
    ss::abort_source _as;
    retry_chain_node _rtc;
    bool _try_creating_inv_config{false};

    ss::timer<ss::lowres_clock> _report_check_timer;
    std::optional<cloud_storage::inventory::report_datetime> _last_fetched;

    std::filesystem::path _marker;
    ss::lowres_clock::duration _inventory_report_check_interval;
};

} // namespace cluster

/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#pragma once

#include "cloud_storage/inventory/inv_ops.h"
#include "cloud_storage/inventory/types.h"
#include "model/metadata.h"

#include <seastar/core/gate.hh>
#include <seastar/core/sharded.hh>

namespace cloud_storage::inventory {

class report_consumer {
public:
    virtual ss::future<> consume_metadata(report_metadata) = 0;
};

class fetch_inventory_svc : public ss::sharded<fetch_inventory_svc> {
public:
    fetch_inventory_svc(
      cloud_storage::cloud_storage_api&,
      model::cloud_storage_backend,
      cloud_storage_clients::bucket_name,
      inventory_config_id,
      ss::sstring inventory_prefix,
      std::unique_ptr<report_consumer>);

    ss::future<> check_for_latest_report();

    ss::future<> stop();

private:
    cloud_storage::cloud_storage_api& _remote;
    ss::lowres_clock::duration _interval_between_probes;

    inv_ops _inv_ops;
    std::unique_ptr<report_consumer> _consumer;

    std::optional<report_datetime> _last_consumed_report_datetime{std::nullopt};
    bool _is_consuming_report{false};

    ss::abort_source _as;
    ss::gate _gate;
    ss::timer<ss::lowres_clock> _report_probe_timer;
};

} // namespace cloud_storage::inventory

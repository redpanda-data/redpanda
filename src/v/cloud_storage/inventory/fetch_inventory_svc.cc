/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_storage/inventory/fetch_inventory_svc.h"

#include "utils/retry_chain_node.h"

#include <utility>

namespace {

using namespace std::chrono_literals;

static constexpr auto interval_between_probes{3600s};

} // namespace

namespace cloud_storage::inventory {

fetch_inventory_svc::fetch_inventory_svc(
  cloud_storage::cloud_storage_api& remote,
  model::cloud_storage_backend backend,
  cloud_storage_clients::bucket_name bucket,
  inventory_config_id inventory_id,
  ss::sstring inventory_prefix,
  std::unique_ptr<report_consumer> consumer)
  : _remote{remote}
  , _interval_between_probes{interval_between_probes}
  , _inv_ops{make_inv_ops(
      backend,
      std::move(bucket),
      std::move(inventory_id),
      std::move(inventory_prefix))}
  , _consumer{std::move(consumer)} {
    _report_probe_timer.set_callback([this] {
        auto guard = _gate.hold();
        return check_for_latest_report();
    });
    _report_probe_timer.arm(ss::lowres_clock::now());
}

ss::future<> fetch_inventory_svc::check_for_latest_report() {
    if (_is_consuming_report) {
        co_return;
    }

    retry_chain_node rtc{_as};
    auto result = co_await _inv_ops.latest_report_metadata(_remote, rtc);
    if (result.has_error()) {
        _report_probe_timer.rearm(
          ss::lowres_clock::now() + _interval_between_probes);
        co_return;
    }

    auto metadata = result.value();
    if (
      !_last_consumed_report_datetime.has_value()
      || metadata.datetime > _last_consumed_report_datetime) {
        _is_consuming_report = true;
        ssx::spawn_with_gate(_gate, [this, md = std::move(metadata)]() mutable {
            return _consumer->consume_metadata(std::move(md)).finally([this] {
                _is_consuming_report = false;
                _report_probe_timer.rearm(
                  ss::lowres_clock::now() + _interval_between_probes);
            });
        });
    }
}

ss::future<> fetch_inventory_svc::stop() {
    if (!_as.abort_requested()) {
        _as.request_abort();
    }
    _report_probe_timer.cancel();

    co_return co_await _gate.close();
}

} // namespace cloud_storage::inventory

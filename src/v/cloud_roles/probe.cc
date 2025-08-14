/*
 * Copyright 2022 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_roles/probe.h"

#include "config/configuration.h"
#include "metrics/prometheus_sanitize.h"
#include "ssx/sformat.h"

#include <seastar/core/metrics.hh>

namespace cloud_roles {

auth_refresh_probe::auth_refresh_probe(ss::sstring tag)
  : _tag(std::move(tag)) {}

void auth_refresh_probe::setup_metrics() {
    if (config::shard_local_cfg().disable_metrics()) {
        return;
    }

    _metrics.add_group(
      _tag.empty()
        ? prometheus_sanitize::metrics_name("cloud_roles::auth_refresh")
        : prometheus_sanitize::metrics_name(
            ssx::sformat("cloud_roles::{}_auth_refresh", _tag)),
      {
        ss::metrics::make_counter(
          "successful_fetches",
          [this] { return _successful_fetches; },
          ss::metrics::description("Total successful credential fetches")),
        ss::metrics::make_counter(
          "fetch_errors",
          [this] { return _fetch_errors; },
          ss::metrics::description("Total errors while fetching")),
      });
}

} // namespace cloud_roles

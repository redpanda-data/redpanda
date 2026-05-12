/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_topics/level_one/common/file_io_probe.h"

#include "config/configuration.h"
#include "metrics/prometheus_sanitize.h"

#include <seastar/core/metrics.hh>

namespace cloud_topics::l1 {

file_io_probe::file_io_probe() { setup_metrics(); }

void file_io_probe::setup_metrics() {
    if (config::shard_local_cfg().disable_metrics()) {
        return;
    }

    namespace sm = ss::metrics;
    _metrics.add_group(
      prometheus_sanitize::metrics_name("cloud_topics_level_one_file_io"),
      {
        sm::make_counter(
          "concurrent_read_merges",
          [this] { return _concurrent_read_merges; },
          sm::description(
            "L1 reads that joined an already-in-flight download for the "
            "same extent on this shard and resolved with a cache hit on "
            "retry (an avoided S3 GET). Excludes joins that aborted or "
            "saw the in-flight download fail. Sustained rate indicates "
            "concurrent demand on hot extents.")),
        sm::make_counter(
          "merged_read_aborts",
          [this] { return _merged_read_aborts; },
          sm::description(
            "L1 reads that joined an in-flight download and were "
            "aborted before the download resolved.")),
      });
}

} // namespace cloud_topics::l1

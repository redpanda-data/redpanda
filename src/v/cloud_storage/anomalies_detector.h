/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#pragma once

#include "cloud_storage/fwd.h"
#include "cloud_storage/types.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "seastarx.h"
#include "utils/retry_chain_node.h"

#include <seastar/core/sstring.hh>

namespace cloud_storage {

/*
 * Utility class that detects anomalies in the data and metadata uploaded
 * by a partition to cloud storage.
 *
 * It performs the following steps:
 * 1. Download partition manifest
 * 2. Check for existence of spillover manifests
 * 3. Check for existence of segments referenced by partition manifest
 * 4. For each spillover manifest, check for existence of the referenced
 * segments
 */
class anomalies_detector {
public:
    anomalies_detector(
      cloud_storage_clients::bucket_name bucket,
      model::ntp ntp,
      model::initial_revision_id initial_rev,
      remote& remote,
      retry_chain_logger& logger,
      ss::abort_source& as);

    struct result {
        scrub_status status{scrub_status::full};
        anomalies detected;
        size_t ops{0};

        result& operator+=(result&&);
    };

    ss::future<result> run(retry_chain_node&);

    ss::future<anomalies_detector::result> download_and_check_spill_manifest(
      const ss::sstring& path, retry_chain_node& rtc_node);

    ss::future<anomalies_detector::result> check_manifest(
      const partition_manifest& manifest, retry_chain_node& rtc_node);

private:
    cloud_storage_clients::bucket_name _bucket;
    model::ntp _ntp;
    model::initial_revision_id _initial_rev;

    remote& _remote;
    retry_chain_logger& _logger;
    ss::abort_source& _as;
};

} // namespace cloud_storage

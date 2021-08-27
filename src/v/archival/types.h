/*
 * Copyright 2021 Vectorized, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/vectorizedio/redpanda/blob/master/licenses/rcl.md
 */

#pragma once

#include "cloud_storage/types.h"
#include "seastar/core/lowres_clock.hh"
#include "seastar/core/sstring.hh"
#include "seastar/util/bool_class.hh"
#include "seastarx.h"
#include "utils/named_type.h"

#include <filesystem>

namespace archival {

using cloud_storage::local_segment_path;
using cloud_storage::remote_manifest_path;
using cloud_storage::remote_segment_path;
using cloud_storage::s3_connection_limit;
using cloud_storage::segment_name;

using service_metrics_disabled
  = ss::bool_class<struct service_metrics_disabled_tag>;
using per_ntp_metrics_disabled
  = ss::bool_class<struct per_ntp_metrics_disabled_tag>;

using segment_time_limit
  = named_type<ss::lowres_clock::duration, struct segment_time_limit_tag>;

} // namespace archival

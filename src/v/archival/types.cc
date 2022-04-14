/*
 * Copyright 2021 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "archival/types.h"

#include <fmt/chrono.h>
#include <fmt/format.h>

namespace archival {

std::ostream&
operator<<(std::ostream& o, const std::optional<segment_time_limit>& tl) {
    if (tl) {
        fmt::print(o, "{}", std::chrono::milliseconds(tl.value()));
    } else {
        fmt::print(o, "N/A");
    }
    return o;
}

std::ostream& operator<<(std::ostream& o, const configuration& cfg) {
    fmt::print(
      o,
      "{{bucket_name: {}, interval: {}, initial_backoff: {}, "
      "segment_upload_timeout: {}, "
      "manifest_upload_timeout: {}, time_limit: {}}}",
      cfg.bucket_name,
      std::chrono::milliseconds(cfg.reconciliation_interval),
      std::chrono::milliseconds(cfg.initial_backoff),
      std::chrono::milliseconds(cfg.segment_upload_timeout),
      std::chrono::milliseconds(cfg.manifest_upload_timeout),
      cfg.time_limit);
    return o;
}

} // namespace archival

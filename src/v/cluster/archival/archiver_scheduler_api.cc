/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "archival/archiver_scheduler_api.h"

#include "archival/types.h"

#include <fmt/format.h>

namespace archival {

bool upload_resource_usage::operator==(
  const upload_resource_usage& o) const noexcept {
    return ntp == o.ntp && put_requests_used == o.put_requests_used
           && uploaded_bytes == o.uploaded_bytes && errc == o.errc
           && archiver_rtc.get().same_root(o.archiver_rtc.get());
}

archiver_scheduler_api::~archiver_scheduler_api() = default;

std::ostream& operator<<(std::ostream& o, const upload_resource_usage& s) {
    fmt::print(
      o,
      "upload_resource_usage(ntp={}, put_requests_used={}, uploaded_bytes={}, "
      "errc={})",
      s.ntp,
      s.put_requests_used,
      s.uploaded_bytes,
      s.errc);
    return o;
}

std::ostream& operator<<(std::ostream& o, const upload_resource_quota& t) {
    fmt::print(
      o,
      "upload_resource_quota(requests_quota={}, upload_size_quota={})",
      t.requests_quota,
      t.upload_size_quota);
    return o;
}

} // namespace archival

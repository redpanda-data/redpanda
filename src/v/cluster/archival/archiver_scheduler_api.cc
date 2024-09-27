/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cluster/archival/archiver_scheduler_api.h"

#include "cluster/archival/types.h"

#include <fmt/format.h>

namespace archival {

std::ostream& operator<<(std::ostream& o, const upload_resource_quota& t) {
    fmt::print(
      o,
      "upload_resource_quota(requests_quota={}, upload_size_quota={})",
      t.requests_quota,
      t.upload_size_quota);
    return o;
}

} // namespace archival

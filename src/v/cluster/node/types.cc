/*
 * Copyright 2021 Vectorized, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */
#include "types.h"

#include "utils/human.h"
#include "utils/to_string.h"

#include <fmt/chrono.h>
#include <fmt/ostream.h>

#include <chrono>

namespace cluster::node {

std::ostream& operator<<(std::ostream& o, const disk& d) {
    fmt::print(
      o,
      "{{path: {}, free: {}, total: {}}}",
      d.path,
      human::bytes(d.free),
      human::bytes(d.total));
    return o;
}

std::ostream& operator<<(std::ostream& o, const local_state& s) {
    fmt::print(
      o,
      "{{redpanda_version: {}, uptime: {}, disks: {}}}",
      s.redpanda_version,
      s.uptime,
      s.disks);
    return o;
}

} // namespace cluster::node
/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "storage/disk.h"

#include "utils/human.h"

#include <fmt/core.h>
#include <fmt/ostream.h>

namespace storage {

std::ostream& operator<<(std::ostream& o, const disk& d) {
    fmt::print(
      o,
      "{{path: {}, free: {}, total: {}, alert: {}, fsid: {}}}",
      d.path,
      human::bytes(d.free),
      human::bytes(d.total),
      d.alert,
      d.fsid);
    return o;
}

} // namespace storage

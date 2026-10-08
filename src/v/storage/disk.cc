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

namespace storage {

bool operator==(const disk& a, const disk& b) {
    return a.path == b.path && a.free == b.free && a.total == b.total
           && a.alert == b.alert;
}

fmt::iterator disk::format_to(fmt::iterator it) const {
    return fmt::format_to(
      it,
      "{{path: {}, free: {}, total: {}, alert: {}, fsid: {}}}",
      path,
      human::bytes(free),
      human::bytes(total),
      alert,
      fsid);
}

} // namespace storage

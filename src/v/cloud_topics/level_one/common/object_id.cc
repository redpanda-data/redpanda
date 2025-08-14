/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_topics/level_one/common/object_id.h"

namespace experimental::cloud_topics::l1 {

fmt::iterator object_extent::format_to(fmt::iterator it) const {
    return fmt::format_to(
      it, "{{id: {}, position: {}, size: {}}}", id, position, size);
}

} // namespace experimental::cloud_topics::l1

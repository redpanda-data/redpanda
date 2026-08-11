/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "datalake/coordinator/partition_state_override.h"

#include <fmt/ostream.h>

namespace datalake::coordinator {

std::ostream& operator<<(std::ostream& o, const partition_state_override& p) {
    if (p.last_committed.has_value()) {
        fmt::print(o, "{{last_committed: {}}}", p.last_committed.value());
    } else {
        fmt::print(o, "{{last_committed: nullopt}}");
    }
    return o;
}

} // namespace datalake::coordinator

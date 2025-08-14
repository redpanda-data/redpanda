/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#include "iceberg/table_identifier.h"

#include <fmt/core.h>
#include <fmt/ostream.h>

namespace iceberg {
std::ostream& operator<<(std::ostream& o, const table_identifier& id) {
    fmt::print(o, "{{ns: {}, table: {}}}", fmt::join(id.ns, "/"), id.table);
    return o;
}
} // namespace iceberg

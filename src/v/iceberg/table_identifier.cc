// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0
#include "iceberg/table_identifier.h"

namespace iceberg {
std::ostream& operator<<(std::ostream& o, const table_identifier& id) {
    fmt::print(o, "{{ns: {}, table: {}}}", fmt::join(id.ns, "/"), id.table);
    return o;
}
} // namespace iceberg

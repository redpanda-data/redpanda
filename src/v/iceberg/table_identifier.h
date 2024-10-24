// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0
#pragma once
#include "base/seastarx.h"
#include "container/fragmented_vector.h"

#include <seastar/core/sstring.hh>

namespace iceberg {
struct table_identifier {
    chunked_vector<ss::sstring> ns;
    ss::sstring table;

    table_identifier copy() const {
        return table_identifier{
          .ns = ns.copy(),
          .table = table,
        };
    }
};
std::ostream& operator<<(std::ostream& o, const table_identifier& id);
} // namespace iceberg

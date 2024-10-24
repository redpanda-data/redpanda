// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0
#pragma once

#include "iceberg/table_metadata.h"
#include "iceberg/table_update.h"

namespace iceberg::table_update {

enum class outcome {
    success = 0,

    // Something went wrong, e.g. some ID was missing that we expected to
    // exist. The table arg is left in an unspecified state.
    unexpected_state,
};

// Applies the given update to the given table metadata.
//
// NOTE: this is a deterministic update to the logical, in-memory metadata for
// actions that have occurred and now need to be reflected in the table. More
// complex operations (e.g. that require IO, like appending files and rewriting
// manifests) do not belong here.
outcome apply(const update&, table_metadata&);

} // namespace iceberg::table_update

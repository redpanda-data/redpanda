/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "serde/parquet/column_array.h"
#include "serde/parquet/schema.h"
#include "serde/parquet/value.h"

namespace serde::parquet {

/// \brief Assemble records from columnar data with definition/repetition
/// levels.
///
/// This is the inverse of shred_record(). The fundamental correctness
/// property: for any schema S and value V, assemble(shred(V)) == V.
chunked_vector<group_value>
assemble_records(const schema_element& schema, const columnar_batch& batch);

} // namespace serde::parquet

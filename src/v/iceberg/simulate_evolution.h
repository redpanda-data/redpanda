/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#pragma once

#include "base/outcome.h"
#include "container/chunked_vector.h"
#include "iceberg/compatibility_types.h"
#include "iceberg/datatypes.h"

namespace iceberg {

struct simulation_step_failure {
    schema_evolution_errc errc;
    /// 0-indexed: failure at step N means evolving the accumulated schema
    /// built from versions 0..N-1 with version N failed.
    size_t step;
};

using simulation_result = checked<struct_type, simulation_step_failure>;

/// Simulates iceberg table schema evolution across a sequence of struct_types.
///
/// Starting from schema_sequence[0] as the initial table schema, evolves with
/// each subsequent type in order. Uses an empty partition spec for evolution.
/// Fields in the input types may have placeholder IDs; real IDs are assigned
/// internally during simulation.
///
/// @param schema_sequence ordered sequence of struct_types representing
///        successive schema versions to replay through evolution.
/// @return the accumulated struct_type after all evolutions, or a
///         simulation_step_failure identifying which step failed and why.
simulation_result
simulate_evolution(chunked_vector<struct_type> schema_sequence);

} // namespace iceberg

/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "iceberg/simulate_evolution.h"

namespace iceberg {

simulation_result
simulate_evolution(chunked_vector<struct_type> /*schema_sequence*/) {
    return simulation_step_failure{
      .errc = schema_evolution_errc::invalid_state, .step = 0};
}

} // namespace iceberg

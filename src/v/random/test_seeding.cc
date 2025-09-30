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

#include "random/test_seeding.h"

#include "random/generators.h"

namespace random_generators {
// Reset the global seed for the random_generators::global() object, so
// the default for testing, so that unit tests see the same series of random
// numbers.
void reset_seed_for_tests() { internal::increment_seed_generation(); }

} // namespace random_generators

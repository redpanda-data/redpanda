/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */
#pragma once

#include "pandaproxy/schema_registry/subject_name_strategy.h"
#include "random/generators.h"

#include <vector>

namespace tests {

inline pandaproxy::schema_registry::subject_name_strategy
random_subject_name_strategy() {
    return random_generators::random_choice(
      std::vector<pandaproxy::schema_registry::subject_name_strategy>{
        pandaproxy::schema_registry::subject_name_strategy::topic_name,
        pandaproxy::schema_registry::subject_name_strategy::record_name,
        pandaproxy::schema_registry::subject_name_strategy::topic_record_name});
}

} // namespace tests

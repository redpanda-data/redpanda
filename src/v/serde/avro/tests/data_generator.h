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

#include <avro/Compiler.hh>
#include <avro/Encoder.hh>
#include <avro/Generic.hh>
#include <avro/Schema.hh>

#include <optional>

namespace testing {

struct generator_state {
    int level{0};
};
avro::GenericDatum generate_datum(
  const avro::NodePtr& node,
  generator_state& state,
  int max_nesting_level,
  std::optional<size_t> elements_in_collection = std::nullopt);

} // namespace testing

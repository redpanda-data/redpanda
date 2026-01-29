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

#include "iceberg/conversion/json_schema/ir.h"

#include <seastar/core/sstring.hh>

#include <utility>

namespace iceberg::conversion::json_schema::testing {

struct generator_config {
    std::pair<size_t, size_t> string_length_range{0, 32};
    int max_nesting_level{10};
};

class generator {
public:
    explicit generator(generator_config config)
      : _config(config) {}

    ss::sstring generate_json(const subschema& schema);

private:
    ss::sstring generate_json_impl(int level, const subschema& schema);

    generator_config _config;
};

} // namespace iceberg::conversion::json_schema::testing

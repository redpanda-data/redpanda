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

struct avro_generator_config {
    std::optional<size_t> elements_in_collection{};
    std::pair<size_t, size_t> string_length_range{0, 32};
    int max_nesting_level{10};
};

class avro_generator {
public:
    explicit avro_generator(avro_generator_config c)
      : _config(c) {}

    avro::GenericDatum generate_datum(const avro::NodePtr& node) {
        return generate_datum_impl(1, node);
    }

private:
    avro::GenericDatum
    generate_datum_impl(int level, const avro::NodePtr& node);
    avro_generator_config _config;
};

} // namespace testing

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

#include "iceberg/conversion/json_schema/tests/data_generator.h"

#include "random/generators.h"

#include <fmt/core.h>

namespace iceberg::conversion::json_schema::testing {

namespace {

ss::sstring random_json_string(const generator_config& config) {
    auto [min, max] = config.string_length_range;
    return random_generators::gen_alphanum_string(
      random_generators::get_int(min, max));
}

} // namespace

ss::sstring generator::generate_json(const subschema& schema) {
    return generate_json_impl(1, schema);
}

ss::sstring generator::generate_json_impl(int level, const subschema& schema) {
    if (schema.types().size() != 1) {
        throw std::runtime_error(
          "Only single type schemas are supported in this generator");
    }

    const auto type = schema.types().front();

    switch (type) {
    case json_value_type::null:
        return "null";
    case json_value_type::boolean:
        return random_generators::random_choice({true, false}) ? "true"
                                                               : "false";
    case json_value_type::integer:
        return fmt::format("{}", random_generators::get_int<int64_t>());
    case json_value_type::number:
        return fmt::format("{}", random_generators::get_real<double>());
    case json_value_type::string: {
        if (schema.format().has_value()) {
            switch (schema.format().value()) {
            case format::date_time:
                return "\"2025-01-15T12:30:45Z\"";
            case format::date:
                return "\"2025-01-15\"";
            case format::time:
                return "\"12:30:45Z\"";
            }
        }
        return fmt::format("\"{}\"", random_json_string(_config));
    }
    case json_value_type::array: {
        if (level >= _config.max_nesting_level) {
            return "[]";
        }
        auto items = schema.items();
        if (std::holds_alternative<std::monostate>(items)) {
            return "[]";
        }
        ss::sstring result = "[";
        auto count = random_generators::get_int<size_t>(1, 3);
        for (size_t i = 0; i < count; ++i) {
            if (i > 0) {
                result += ",";
            }
            if (
              auto* single_item
              = std::get_if<std::reference_wrapper<const subschema>>(&items)) {
                result += generate_json_impl(level + 1, single_item->get());
            }
        }
        result += "]";
        return result;
    }
    case json_value_type::object: {
        if (level >= _config.max_nesting_level) {
            return "{}";
        }
        ss::sstring result = "{";
        bool first = true;
        for (auto [key, sub] : schema.properties()) {
            if (!first) {
                result += ",";
            }
            first = false;
            result += fmt::format(
              "\"{}\":{}", key, generate_json_impl(level + 1, sub));
        }
        result += "}";
        return result;
    }
    }
}

} // namespace iceberg::conversion::json_schema::testing

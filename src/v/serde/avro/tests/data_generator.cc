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
#include "serde/avro/tests/data_generator.h"

#include "base/seastarx.h"
#include "random/generators.h"
#include "utils/uuid.h"

#include <seastar/util/defer.hh>

namespace testing {
static constexpr auto seconds_in_day = 24 * 60 * 60;

std::vector<uint8_t> generate_decimal(size_t max_size = 16) {
    std::vector<uint8_t> bytes;
    for (size_t i = 0; i < max_size; ++i) {
        bytes.push_back(random_generators::get_int<uint8_t>());
    }

    return bytes;
}

ss::sstring random_string(const avro_generator_config& config) {
    auto [min, max] = config.string_length_range;
    return random_generators::gen_alphanum_string(
      random_generators::get_int(min, max));
}

::avro::GenericDatum
avro_generator::generate_datum_impl(int level, const avro::NodePtr& node) {
    auto get_elements_count = [this, level]() -> size_t {
        if (level >= _config.max_nesting_level) {
            return 0;
        }
        return _config.elements_in_collection.value_or(
          random_generators::get_int<size_t>(10));
    };
    ::avro::GenericDatum datum{node};
    switch (node->type()) {
    case avro::AVRO_STRING: {
        if (node->logicalType().type() == avro::LogicalType::UUID) {
            datum.value<std::string>() = fmt::to_string(uuid_t::create());
            return datum;
        }
        auto v = random_string(_config);
        datum.value<std::string>() = v;
        return datum;
    }
    case avro::AVRO_BYTES: {
        if (node->logicalType().type() == avro::LogicalType::DECIMAL) {
            datum.value<std::vector<uint8_t>>() = generate_decimal();
            return datum;
        }
        auto sz = random_generators::get_int(512);
        std::vector<uint8_t> bytes;
        bytes.reserve(sz);
        for (int i = 0; i < sz; ++i) {
            bytes.push_back(random_generators::get_int<uint8_t>());
        }
        datum.value<std::vector<uint8_t>>() = bytes;
        return datum;
    }
    case avro::AVRO_INT: {
        auto max = std::numeric_limits<int32_t>::max();
        if (node->logicalType().type() == avro::LogicalType::TIME_MILLIS) {
            max = seconds_in_day * 1000;
        } else if (node->logicalType().type() == avro::LogicalType::DATE) {
            max = 365;
        }
        datum.value<int32_t>() = random_generators::get_int<int32_t>(max);
        return datum;
    }
    case avro::AVRO_ENUM: {
        ::avro::GenericEnum e{node};

        datum.value<::avro::GenericEnum>().set(
          random_generators::get_int<int64_t>(node->names() - 1));
        return datum;
    }
    case avro::AVRO_LONG: {
        auto max = std::numeric_limits<int64_t>::max();
        if (node->logicalType().type() == avro::LogicalType::TIME_MICROS) {
            max = seconds_in_day * 1000000L;
        } else if (
          node->logicalType().type() == avro::LogicalType::TIMESTAMP_MILLIS) {
            max = max / 1000;
        }
        datum.value<int64_t>() = random_generators::get_int<int64_t>(max);
        return datum;
    }
    case avro::AVRO_FLOAT:
        datum.value<float>() = random_generators::get_real<float>();
        return datum;
    case avro::AVRO_DOUBLE:
        datum.value<double>() = random_generators::get_real<double>();
        return datum;
    case avro::AVRO_BOOL:
        datum.value<bool>() = random_generators::random_choice({true, false});
        return datum;
    case avro::AVRO_NULL:
        return {};
    case avro::AVRO_RECORD: {
        ::avro::GenericRecord record{node};
        for (size_t i = 0; i < record.fieldCount(); ++i) {
            record.fieldAt(i) = generate_datum_impl(level + 1, node->leafAt(i));
        }
        return {node, record};
    }
    case avro::AVRO_ARRAY: {
        ::avro::GenericArray array{node};

        auto sz = get_elements_count();
        for (size_t i = 0; i < sz; ++i) {
            array.value().push_back(
              generate_datum_impl(level + 1, array.schema()->leafAt(0)));
        }
        return {node, array};
    }
    case avro::AVRO_MAP: {
        ::avro::GenericMap map{node};
        auto sz = get_elements_count();
        for (size_t i = 0; i < sz; ++i) {
            auto key = random_generators::gen_alphanum_string(
              random_generators::get_int(16));

            map.value().emplace_back(
              key, generate_datum_impl(level + 1, map.schema()->leafAt(1)));
        }
        return {node, map};
    }
    case avro::AVRO_SYMBOLIC: {
        auto resolved = ::avro::resolveSymbol(node);
        return generate_datum_impl(level + 1, resolved);
    }
    case avro::AVRO_UNION: {
        ::avro::GenericUnion u{node};
        int branch = 0;
        // some of the unions are recursive, prevent infinite recursion by
        // choosing a plain type instead of the record
        if (level >= _config.max_nesting_level) {
            for (size_t i = 0; i < node->leaves(); i++) {
                if (node->leafAt(i)->type() != avro::AVRO_RECORD) {
                    branch = i;
                    break;
                }
            }
        } else {
            branch = random_generators::get_int(u.schema()->leaves() - 1);
        }
        u.selectBranch(branch);

        u.datum() = generate_datum_impl(level + 1, u.schema()->leafAt(branch));
        return {node, u};
    }
    case avro::AVRO_FIXED: {
        ::avro::GenericFixed fixed{node};
        if (node->logicalType().type() == avro::LogicalType::DECIMAL) {
            fixed.value() = generate_decimal(node->fixedSize());
            datum.value<::avro::GenericFixed>() = fixed;
            return datum;
        }
        fixed.value().reserve(node->fixedSize());
        for (size_t i = 0; i < node->fixedSize(); ++i) {
            fixed.value()[i] = random_generators::get_int<uint8_t>();
        }
        fixed.value().shrink_to_fit();
        datum.value<::avro::GenericFixed>() = fixed;
        return datum;
    }
    case avro::AVRO_UNKNOWN:
        throw std::runtime_error("unsupported avro type");
    }
}

} // namespace testing

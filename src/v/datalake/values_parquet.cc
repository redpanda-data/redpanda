/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#include "datalake/values_parquet.h"

namespace datalake {
namespace {
struct primitive_value_converting_visitor {
    parquet_conversion_outcome operator()(iceberg::boolean_value v) {
        return serde::parquet::boolean_value{.val = v.val};
    }
    parquet_conversion_outcome operator()(iceberg::int_value v) {
        return serde::parquet::int32_value{.val = v.val};
    }
    parquet_conversion_outcome operator()(iceberg::long_value v) {
        return serde::parquet::int64_value{.val = v.val};
    }
    parquet_conversion_outcome operator()(iceberg::float_value v) {
        return serde::parquet::float32_value{.val = v.val};
    }
    parquet_conversion_outcome operator()(iceberg::double_value v) {
        return serde::parquet::float64_value{.val = v.val};
    }
    parquet_conversion_outcome operator()(iceberg::date_value v) {
        return serde::parquet::int32_value{.val = v.val};
    }
    parquet_conversion_outcome operator()(iceberg::time_value v) {
        return serde::parquet::int64_value{.val = v.val};
    }
    parquet_conversion_outcome operator()(iceberg::timestamp_value v) {
        return serde::parquet::int64_value{.val = v.val};
    }
    parquet_conversion_outcome operator()(iceberg::timestamptz_value v) {
        return serde::parquet::int64_value{.val = v.val};
    }
    parquet_conversion_outcome operator()(iceberg::string_value v) {
        return serde::parquet::byte_array_value{.val = std::move(v.val)};
    }
    parquet_conversion_outcome operator()(iceberg::uuid_value v) {
        iobuf buffer;
        buffer.append(v.val.uuid().data, v.val.uuid().size());
        return serde::parquet::fixed_byte_array_value{.val = std::move(buffer)};
    }
    parquet_conversion_outcome operator()(iceberg::fixed_value v) {
        return serde::parquet::fixed_byte_array_value{.val = std::move(v.val)};
    }
    parquet_conversion_outcome operator()(iceberg::binary_value v) {
        return serde::parquet::byte_array_value{.val = std::move(v.val)};
    }
    parquet_conversion_outcome operator()(iceberg::decimal_value v) {
        iobuf buffer;
        // big endian, starts from msb
        auto low_part = ss::cpu_to_be(Int128Low64(v.val));
        auto high_part = ss::cpu_to_be(Int128High64(v.val));
        buffer.append(
          reinterpret_cast<const char*>(&high_part), sizeof(int64_t));
        buffer.append(
          reinterpret_cast<const char*>(&low_part), sizeof(uint64_t));

        return serde::parquet::fixed_byte_array_value{.val = std::move(buffer)};
    }
};
struct value_converting_visitor {
    ss::future<parquet_conversion_outcome>
    operator()(iceberg::primitive_value value) {
        co_return std::visit(
          primitive_value_converting_visitor{}, std::move(value));
    }
    ss::future<parquet_conversion_outcome>
    operator()(std::unique_ptr<iceberg::struct_value> value) {
        serde::parquet::group_value group;
        group.reserve(value->fields.size());
        for (auto& field : value->fields) {
            if (!field.has_value()) {
                group.emplace_back(serde::parquet::null_value{});
                continue;
            }
            auto result = co_await to_parquet_value(std::move(*field));
            if (result.has_error()) {
                co_return result.error();
            }

            group.emplace_back(std::move(result.value()));
        }
        co_return group;
    }
    ss::future<parquet_conversion_outcome>
    operator()(std::unique_ptr<iceberg::list_value> list) {
        serde::parquet::repeated_value repeated;
        repeated.reserve(list->elements.size());
        for (auto& element : list->elements) {
            serde::parquet::group_value element_wrapper;
            if (!element.has_value()) {
                element_wrapper.emplace_back(serde::parquet::null_value{});
            } else {
                auto result = co_await to_parquet_value(std::move(*element));
                if (result.has_error()) {
                    co_return result.error();
                }
                element_wrapper.emplace_back(std::move(result.value()));
            }
            repeated.emplace_back(std::move(element_wrapper));
        }

        serde::parquet::group_value list_wrapper;
        list_wrapper.emplace_back(std::move(repeated));
        co_return list_wrapper;
    }
    ss::future<parquet_conversion_outcome>
    operator()(std::unique_ptr<iceberg::map_value> map) {
        serde::parquet::repeated_value repeated;
        repeated.reserve(map->kvs.size());
        for (auto& kv : map->kvs) {
            serde::parquet::group_value group;
            group.reserve(2);
            auto key_result = co_await to_parquet_value(std::move(kv.key));
            if (key_result.has_error()) {
                co_return key_result.error();
            }
            group.emplace_back(std::move(key_result.value()));
            if (kv.val.has_value()) {
                auto val = co_await to_parquet_value(std::move(kv.val.value()));
                if (val.has_error()) {
                    co_return val.error();
                }
                group.emplace_back(std::move(val.value()));
            } else {
                group.emplace_back(serde::parquet::null_value{});
            }
            repeated.emplace_back(std::move(group));
        }
        serde::parquet::group_value map_wrapper;
        map_wrapper.emplace_back(std::move(repeated));
        co_return map_wrapper;
    }
};
} // namespace

ss::future<parquet_conversion_outcome> to_parquet_value(iceberg::value value) {
    return std::visit(value_converting_visitor{}, std::move(value));
}

} // namespace datalake

/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#pragma once

#include "container/fragmented_vector.h"
#include "iceberg/datatypes.h"
#include "iceberg/values.h"

#include <arrow/type.h>

#include <optional>

namespace datalake {

class struct_schema_converter;

class arrow_schema_translator {
public:
    explicit arrow_schema_translator(iceberg::struct_type&& schema);

    // Wrap constructor to return optional on failure.
    static std::optional<arrow_schema_translator>
    create(iceberg::struct_type&& schema) {
        try {
            return std::make_optional<arrow_schema_translator>(
              std::move(schema));
        } catch (...) {
            return std::nullopt;
        }
    }

    ~arrow_schema_translator();

    std::shared_ptr<arrow::Schema> build_arrow_schema();

private:
    iceberg::struct_type _schema;

    // Top-level struct that represents the whole schema.
    std::unique_ptr<struct_schema_converter> _struct_converter;
};
} // namespace datalake

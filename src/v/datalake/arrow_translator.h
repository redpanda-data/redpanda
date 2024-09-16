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

class struct_converter;

class arrow_translator {
public:
    explicit arrow_translator(iceberg::struct_type&& schema);

    // Wrap constructor to return optional on failure.
    static std::optional<arrow_translator>
    create(iceberg::struct_type&& schema) {
        try {
            return std::make_optional<arrow_translator>(std::move(schema));
        } catch (...) {
            return std::nullopt;
        }
    }

    ~arrow_translator();

    std::shared_ptr<arrow::Schema> build_arrow_schema();
    void add_data(iceberg::value value);

    // Returns an arrow:Array for all of the data that has been added since the
    // translator was created or the last take_chunk call. It then clears the
    // data in the translator so that it's ready to accept more.
    std::shared_ptr<arrow::Array> take_chunk();

private:
    iceberg::struct_type _schema;

    // Top-level struct that represents the whole schema.
    std::unique_ptr<struct_converter> _struct_converter;
};
} // namespace datalake

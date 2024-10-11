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

#include "base/outcome.h"
#include "coordinator/data_file.h"
#include "datalake/schemaless_translator.h"
#include "iceberg/datatypes.h"
#include "iceberg/values.h"
#include "serde/envelope.h"

#include <cstddef>
#include <memory>

namespace datalake {
enum class data_writer_error {
    ok = 0,
    parquet_conversion_error,
    file_io_error,
    no_data,
};

struct data_writer_error_category : std::error_category {
    const char* name() const noexcept override { return "Data Writer Error"; }

    std::string message(int ev) const override {
        switch (static_cast<data_writer_error>(ev)) {
        case data_writer_error::ok:
            return "Ok";
        case data_writer_error::parquet_conversion_error:
            return "Parquet Conversion Error";
        case data_writer_error::file_io_error:
            return "File IO Error";
        case data_writer_error::no_data:
            return "No data";
        }
    }

    static const std::error_category& error_category() {
        static data_writer_error_category e;
        return e;
    }
};

inline std::error_code make_error_code(data_writer_error e) noexcept {
    return {static_cast<int>(e), data_writer_error_category::error_category()};
}

class data_writer {
public:
    virtual ~data_writer() = default;

    virtual ss::future<data_writer_error> add_data_struct(
      iceberg::struct_value /* data */, int64_t /* approx_size */)
      = 0;

    virtual ss::future<result<coordinator::data_file, data_writer_error>>
    finish() = 0;
};

class data_writer_factory {
public:
    virtual ~data_writer_factory() = default;

    virtual ss::future<result<ss::shared_ptr<data_writer>, data_writer_error>>
      create_writer(iceberg::struct_type /* schema */) = 0;
};

} // namespace datalake

namespace std {
template<>
struct is_error_code_enum<datalake::data_writer_error> : true_type {};
} // namespace std

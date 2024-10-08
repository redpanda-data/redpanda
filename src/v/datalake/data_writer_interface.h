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

};

struct data_writer_result
  : serde::envelope<
      data_writer_result,
      serde::version<0>,
      serde::compat_version<0>> {
    ss::sstring file_path = "";
    size_t record_count = 0;
    size_t file_size_bytes = 0;

    auto serde_fields() { return std::tie(record_count); }
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

    virtual ss::future<result<data_writer_result, data_writer_error>>
    finish() = 0;
};

class data_writer_factory {
public:
    virtual ~data_writer_factory() = default;

    virtual std::unique_ptr<data_writer>
      create_writer(iceberg::struct_type /* schema */) = 0;
};

} // namespace datalake

namespace std {
template<>
struct is_error_code_enum<datalake::data_writer_error> : true_type {};
} // namespace std

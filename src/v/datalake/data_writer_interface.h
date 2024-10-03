/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#include "datalake/schemaless_translator.h"
#include "iceberg/datatypes.h"
#include "iceberg/values.h"
#include "serde/envelope.h"

#include <cstddef>
#include <memory>

#pragma once

namespace datalake {
enum class data_writer_error {
    ok = 0,
    uh_oh,
};

struct data_writer_result
  : serde::envelope<
      data_writer_result,
      serde::version<0>,
      serde::compat_version<0>> {
    size_t row_count = 0;

    auto serde_fields() { return std::tie(row_count); }
};

class data_writer {
public:
    virtual ~data_writer() = default;

    virtual ss::future<data_writer_error> add_data_struct(
      iceberg::struct_value /* data */, int64_t /* approx_size */)
      = 0;

    virtual ss::future<data_writer_result> finish() = 0;
};

class data_writer_factory {
public:
    virtual ~data_writer_factory() = default;

    virtual std::unique_ptr<data_writer>
      create_writer(iceberg::struct_type /* schema */) = 0;
};

struct data_writer_error_category : std::error_category {
    const char* name() const noexcept override { return "Data Writer Error"; }

    std::string message(int ev) const override {
        switch (static_cast<data_writer_error>(ev)) {
        case data_writer_error::ok:
            return "Ok";
        case data_writer_error::uh_oh:
            return "Uh oh!";
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

} // namespace datalake

namespace std {
template<>
struct is_error_code_enum<datalake::data_writer_error> : true_type {};
} // namespace std

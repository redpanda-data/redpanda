// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0
#pragma once

#include "datalake/data_writer_interface.h"
#include "datalake/schemaless_translator.h"
#include "iceberg/datatypes.h"
#include "iceberg/values.h"

#include <seastar/core/future.hh>
#include <seastar/core/shared_ptr.hh>

#include <cstdint>
#include <memory>
namespace datalake {

class test_data_writer : public data_writer {
public:
    explicit test_data_writer(iceberg::struct_type schema)
      : _schema(std::move(schema))
      , _result{} {}

    ss::future<bool> add_data_struct(
      iceberg::struct_value /* data */, int64_t /* approx_size */) override {
        _result.row_count++;
        return ss::make_ready_future<bool>(false);
    }

    ss::future<data_writer_result> finish() override {
        return ss::make_ready_future<data_writer_result>(_result);
    }

private:
    iceberg::struct_type _schema;
    data_writer_result _result;
};
class test_data_writer_factory : public data_writer_factory {
public:
    std::unique_ptr<data_writer>
    create_writer(iceberg::struct_type schema) override {
        return std::make_unique<test_data_writer>(std::move(schema));
    }

private:
    iceberg::struct_type _schema;
};

} // namespace datalake

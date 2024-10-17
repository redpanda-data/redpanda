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
    explicit test_data_writer(iceberg::struct_type schema, bool return_error)
      : _schema(std::move(schema))
      , _result{}
      , _return_error{return_error} {}

    ss::future<data_writer_error> add_data_struct(
      iceberg::struct_value /* data */, int64_t /* approx_size */) override {
        _result.row_count++;
        data_writer_error status
          = _return_error ? data_writer_error::parquet_conversion_error
                          : data_writer_error::ok;
        return ss::make_ready_future<data_writer_error>(status);
    }

    ss::future<result<local_file_metadata, data_writer_error>>
    finish() override {
        return ss::make_ready_future<
          result<local_file_metadata, data_writer_error>>(_result);
    }

private:
    iceberg::struct_type _schema;
    local_file_metadata _result;
    bool _return_error;
};
class test_data_writer_factory : public data_writer_factory {
public:
    explicit test_data_writer_factory(bool return_error)
      : _return_error{return_error} {}

    ss::future<result<std::unique_ptr<data_writer>, data_writer_error>>
    create_writer(iceberg::struct_type schema) override {
        co_return std::make_unique<test_data_writer>(
          std::move(schema), _return_error);
    }

private:
    iceberg::struct_type _schema;
    bool _return_error;
};

} // namespace datalake

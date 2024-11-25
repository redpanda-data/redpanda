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
#include "datalake/serde_parquet_writer.h"
#include "iceberg/datatypes.h"
#include "iceberg/values.h"
#include "utils/null_output_stream.h"

#include <seastar/core/future.hh>
#include <seastar/core/shared_ptr.hh>

#include <cstdint>
#include <memory>
namespace datalake {

class test_data_writer : public parquet_file_writer {
public:
    explicit test_data_writer(
      const iceberg::struct_type& schema, bool return_error)
      : _schema(schema.copy())
      , _result{}
      , _return_error{return_error} {}

    ss::future<writer_error> add_data_struct(
      iceberg::struct_value /* data */, int64_t /* approx_size */) override {
        _result.row_count++;
        writer_error status = _return_error
                                ? writer_error::parquet_conversion_error
                                : writer_error::ok;
        return ss::make_ready_future<writer_error>(status);
    }

    ss::future<result<local_file_metadata, writer_error>> finish() override {
        return ss::make_ready_future<result<local_file_metadata, writer_error>>(
          _result);
    }

private:
    iceberg::struct_type _schema;
    local_file_metadata _result;
    bool _return_error;
};
class test_data_writer_factory : public parquet_file_writer_factory {
public:
    explicit test_data_writer_factory(bool return_error)
      : _return_error{return_error} {}

    ss::future<result<std::unique_ptr<parquet_file_writer>, writer_error>>
    create_writer(const iceberg::struct_type& schema) override {
        co_return std::make_unique<test_data_writer>(
          std::move(schema), _return_error);
    }

private:
    iceberg::struct_type _schema;
    bool _return_error;
};

class test_serde_parquet_data_writer : public parquet_file_writer {
public:
    explicit test_serde_parquet_data_writer(
      std::unique_ptr<parquet_ostream> writer)
      : _writer(std::move(writer))
      , _result{} {}

    ss::future<writer_error>
    add_data_struct(iceberg::struct_value data, int64_t sz) override {
        auto write_result = co_await _writer->add_data_struct(
          std::move(data), sz);
        _result.row_count++;
        co_return write_result;
    }

    ss::future<result<local_file_metadata, writer_error>> finish() override {
        return ss::make_ready_future<result<local_file_metadata, writer_error>>(
          _result);
    }

private:
    std::unique_ptr<parquet_ostream> _writer;
    local_file_metadata _result;
};

class test_serde_parquet_writer_factory : public parquet_file_writer_factory {
public:
    ss::future<result<std::unique_ptr<parquet_file_writer>, writer_error>>
    create_writer(const iceberg::struct_type& schema) override {
        auto ostream_writer = co_await _serde_parquet_factory.create_writer(
          schema, utils::make_null_output_stream());

        co_return std::make_unique<test_serde_parquet_data_writer>(
          std::move(ostream_writer));
    }

private:
    serde_parquet_writer_factory _serde_parquet_factory;
};

} // namespace datalake

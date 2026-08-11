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
class noop_mem_tracker : public writer_mem_tracker {
public:
    ss::future<reservation_error>
    reserve_bytes(size_t, ss::abort_source&) noexcept override {
        if (std::exchange(_oom_on_next_reserve, false)) {
            co_return reservation_error::out_of_memory;
        } else {
            co_return reservation_error::ok;
        }
    }
    ss::future<> free_bytes(size_t, ss::abort_source&) override {
        return ss::make_ready_future<>();
    }
    void release() override {}
    writer_disk_tracker& disk() override { return _disk; }

    void inject_oom_on_next_reserve() { _oom_on_next_reserve = true; }

private:
    class noop_disk_tracker : public writer_disk_tracker {
    public:
        ss::future<reservation_error>
        reserve_bytes(size_t, ss::abort_source&) noexcept override {
            return ss::make_ready_future<reservation_error>(
              reservation_error::ok);
        }
        ss::future<> free_bytes(size_t, ss::abort_source&) override {
            return ss::make_ready_future<>();
        }
        void release() override {}
        void release_unused() override {}
    };

    noop_disk_tracker _disk;
    bool _oom_on_next_reserve{false};
};

class test_data_writer : public parquet_file_writer {
public:
    explicit test_data_writer(
      const iceberg::struct_type& schema, bool return_error)
      : _schema(schema.copy())
      , _result{}
      , _return_error{return_error} {}

    ss::future<writer_error> add_data_struct(
      iceberg::struct_value /* data */,
      int64_t /* approx_size */,
      ss::abort_source&) override {
        _result.row_count++;
        writer_error status = _return_error
                                ? writer_error::parquet_conversion_error
                                : writer_error::ok;
        return ss::make_ready_future<writer_error>(status);
    }

    size_t buffered_bytes() const override { return 0; }

    size_t flushed_bytes() const override { return 0; }

    ss::future<writer_error> flush() override {
        return ss::make_ready_future<writer_error>(writer_error::ok);
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
    create_writer(
      const iceberg::struct_type& schema, ss::abort_source&) override {
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

    ss::future<writer_error> add_data_struct(
      iceberg::struct_value data, int64_t sz, ss::abort_source& as) override {
        auto write_result = co_await _writer->add_data_struct(
          std::move(data), sz, as);
        _result.row_count++;
        co_return write_result;
    }

    size_t buffered_bytes() const override { return _writer->buffered_bytes(); }

    size_t flushed_bytes() const override { return _writer->flushed_bytes(); }

    ss::future<writer_error> flush() override {
        return ss::make_ready_future<writer_error>(writer_error::ok);
    }

    ss::future<result<local_file_metadata, writer_error>> finish() override {
        auto result = co_await _writer->finish();
        if (result != writer_error::ok) {
            co_return result;
        }
        co_return _result;
    }

private:
    std::unique_ptr<parquet_ostream> _writer;
    local_file_metadata _result;
};

class test_serde_parquet_writer_factory : public parquet_file_writer_factory {
public:
    ss::future<result<std::unique_ptr<parquet_file_writer>, writer_error>>
    create_writer(
      const iceberg::struct_type& schema, ss::abort_source&) override {
        auto ostream_writer = co_await _serde_parquet_factory.create_writer(
          schema, utils::make_null_output_stream(), _mem_tracker);

        co_return std::make_unique<test_serde_parquet_data_writer>(
          std::move(ostream_writer));
    }

private:
    serde_parquet_writer_factory _serde_parquet_factory;
    noop_mem_tracker _mem_tracker;
};

} // namespace datalake

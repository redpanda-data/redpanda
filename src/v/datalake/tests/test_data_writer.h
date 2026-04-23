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

#include "bytes/iostream.h"
#include "container/chunked_hash_map.h"
#include "datalake/data_writer_interface.h"
#include "datalake/serde_parquet_writer.h"
#include "iceberg/datatypes.h"
#include "iceberg/values.h"
#include "utils/null_output_stream.h"
#include "utils/uuid.h"

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
        co_return std::move(_result);
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

    void set_path(local_path p) { _result.path = std::move(p); }

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
        _result.size_bytes = _writer->flushed_bytes()
                             + _writer->buffered_bytes();
        auto result = co_await _writer->finish();
        if (result.has_error()) {
            co_return result.error();
        }
        _result.parquet_metadata = std::move(result.value());
        co_return std::move(_result);
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

/// \brief Writer factory that captures parquet file data as iobufs.
///
/// Like test_serde_parquet_writer_factory, but writes to in-memory iobufs
/// instead of null streams. After the multiplexer finishes, each captured
/// file can be uploaded to mock S3 for committer testing.
///
/// Usage:
///   capturing_parquet_writer_factory factory;
///   // ... use factory with record_multiplexer ...
///   // After multiplex:
///   for (auto& pf : finished_files.data_files) {
///       auto& data = factory.files.at(pf.local_file.path());
///       manifest_io.upload_object_bytes(uri, data.copy());
///   }
class capturing_parquet_writer_factory : public parquet_file_writer_factory {
public:
    ss::future<result<std::unique_ptr<parquet_file_writer>, writer_error>>
    create_writer(
      const iceberg::struct_type& schema, ss::abort_source&) override {
        auto path_key = fmt::format("captured-{}.parquet", uuid_t::create());
        // Insert iobuf into the map. The reference remains stable because
        // chunked_hash_map doesn't invalidate references on insert.
        auto [it, _] = files.emplace(path_key, iobuf{});

        auto ostream_writer = co_await _serde_parquet_factory.create_writer(
          schema, make_iobuf_ref_output_stream(it->second), _mem_tracker);

        auto writer = std::make_unique<test_serde_parquet_data_writer>(
          std::move(ostream_writer));
        // Set the path so finish() returns it in local_file_metadata.
        writer->set_path(local_path(path_key));
        co_return std::move(writer);
    }

    /// Captured parquet file data, keyed by local_file_metadata path.
    chunked_hash_map<ss::sstring, iobuf> files;

private:
    serde_parquet_writer_factory _serde_parquet_factory;
    noop_mem_tracker _mem_tracker;
};

} // namespace datalake

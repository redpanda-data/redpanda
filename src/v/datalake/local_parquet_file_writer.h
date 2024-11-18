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

#include <seastar/core/file.hh>

namespace datalake {

/**
 * Class wrapping a parquet writer and handling all FILE I/O operations
 * necessary to write a temporary local parquet file. The class handles aborts
 * and deletes the local file if the error occurs.
 */
class local_parquet_file_writer : public parquet_file_writer {
public:
    local_parquet_file_writer(
      local_path, ss::shared_ptr<parquet_ostream_factory>);

    ss::future<checked<std::nullopt_t, writer_error>>
    initialize(const iceberg::struct_type&);

    ss::future<writer_error> add_data_struct(
      iceberg::struct_value /* data */, int64_t /* approx_size */) final;

    ss::future<result<local_file_metadata, writer_error>> finish() final;

private:
    ss::future<> abort();

    local_path _output_file_path;
    ss::file _output_file;
    size_t _row_count{0};
    size_t _raw_bytes_count{0};

    std::unique_ptr<parquet_ostream> _writer;
    ss::shared_ptr<parquet_ostream_factory> _writer_factory;
    bool _initialized{false};
};

class local_parquet_file_writer_factory : public parquet_file_writer_factory {
public:
    local_parquet_file_writer_factory(
      local_path base_directory,
      ss::sstring file_name_prefix,
      ss::shared_ptr<parquet_ostream_factory>);

    ss::future<result<std::unique_ptr<parquet_file_writer>, writer_error>>
    create_writer(const iceberg::struct_type& schema) final;

private:
    local_path create_filename() const;

    local_path _base_directory;
    ss::sstring _file_name_prefix;
    ss::shared_ptr<parquet_ostream_factory> _writer_factory;
};

} // namespace datalake

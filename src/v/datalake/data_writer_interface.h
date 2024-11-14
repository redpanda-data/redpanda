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
#include "datalake/base_types.h"
#include "iceberg/datatypes.h"
#include "iceberg/values.h"

#include <seastar/core/iostream.hh>

#include <cstddef>

namespace datalake {

enum class writer_error {
    ok = 0,
    parquet_conversion_error,
    file_io_error,
    no_data,
};

struct data_writer_error_category : std::error_category {
    const char* name() const noexcept final { return "Data Writer Error"; }

    std::string message(int ev) const final;

    static const std::error_category& error_category() {
        static data_writer_error_category e;
        return e;
    }
};

inline std::error_code make_error_code(writer_error e) noexcept {
    return {static_cast<int>(e), data_writer_error_category::error_category()};
}

/**
 * Parquet writer interface. The writer should write parquet serialized data to
 * the output stream provided during its creation.
 */
class parquet_ostream {
public:
    explicit parquet_ostream() = default;
    parquet_ostream(const parquet_ostream&) = delete;
    parquet_ostream(parquet_ostream&&) = default;
    parquet_ostream& operator=(const parquet_ostream&) = delete;
    parquet_ostream& operator=(parquet_ostream&&) = default;
    virtual ~parquet_ostream() = default;

    virtual ss::future<writer_error>
      add_data_struct(iceberg::struct_value, size_t) = 0;

    virtual ss::future<writer_error> finish() = 0;
};

class parquet_ostream_factory {
public:
    parquet_ostream_factory() = default;
    parquet_ostream_factory(const parquet_ostream_factory&) = default;
    parquet_ostream_factory(parquet_ostream_factory&&) = delete;
    parquet_ostream_factory& operator=(const parquet_ostream_factory&)
      = default;
    parquet_ostream_factory& operator=(parquet_ostream_factory&&) = delete;

    virtual ~parquet_ostream_factory() = default;

    virtual ss::future<std::unique_ptr<parquet_ostream>>
    create_writer(const iceberg::struct_type&, ss::output_stream<char>) = 0;
};

/**
 * Interface of a parquet file writer. The file writer finishes by returning
 * file metadata. In future we may want to change the return type of this
 * interface to me more generic and allow to express that writer can return
 * either a local file path or a remote path.
 */
class parquet_file_writer {
public:
    parquet_file_writer() = default;
    parquet_file_writer(const parquet_file_writer&) = delete;
    parquet_file_writer(parquet_file_writer&&) = default;
    parquet_file_writer& operator=(const parquet_file_writer&) = delete;
    parquet_file_writer& operator=(parquet_file_writer&&) = delete;

    virtual ~parquet_file_writer() = default;

    virtual ss::future<writer_error> add_data_struct(
      iceberg::struct_value /* data */, int64_t /* approx_size */)
      = 0;

    virtual ss::future<result<local_file_metadata, writer_error>> finish() = 0;
};

class parquet_file_writer_factory {
public:
    parquet_file_writer_factory() = default;
    parquet_file_writer_factory(const parquet_file_writer_factory&) = delete;
    parquet_file_writer_factory(parquet_file_writer_factory&&) = default;
    parquet_file_writer_factory& operator=(const parquet_file_writer_factory&)
      = delete;
    parquet_file_writer_factory& operator=(parquet_file_writer_factory&&)
      = default;
    virtual ~parquet_file_writer_factory() = default;

    virtual ss::future<
      result<std::unique_ptr<parquet_file_writer>, writer_error>>
    create_writer(const iceberg::struct_type& /* schema */) = 0;
};

} // namespace datalake

namespace std {
template<>
struct is_error_code_enum<datalake::writer_error> : true_type {};
} // namespace std

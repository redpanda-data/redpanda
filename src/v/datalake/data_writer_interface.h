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
#include "iceberg/datatypes.h"
#include "iceberg/values.h"

#include <cstddef>

namespace datalake {
/**
 * Definitions of local and remote paths, as the name indicates the local path
 * is always pointing to the location on local disk wheras the remote path is a
 * path of the object in the object store.
 */
using local_path = named_type<std::filesystem::path, struct local_path_tag>;
using remote_path = named_type<std::filesystem::path, struct remote_path_tag>;

/**
 * Simple type describing local parquet file metadata with its path and basic
 * statistics
 */
struct local_file_metadata {
    local_path path;
    size_t row_count = 0;
    size_t size_bytes = 0;
    int hour = 0;

    friend std::ostream&
    operator<<(std::ostream& o, const local_file_metadata& r);
};

enum class data_writer_error {
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

inline std::error_code make_error_code(data_writer_error e) noexcept {
    return {static_cast<int>(e), data_writer_error_category::error_category()};
}

class data_writer {
public:
    data_writer() = default;
    data_writer(const data_writer&) = delete;
    data_writer(data_writer&&) = default;
    data_writer& operator=(const data_writer&) = delete;
    data_writer& operator=(data_writer&&) = delete;

    virtual ~data_writer() = default;

    virtual ss::future<data_writer_error> add_data_struct(
      iceberg::struct_value /* data */, int64_t /* approx_size */)
      = 0;

    virtual ss::future<result<local_file_metadata, data_writer_error>>
    finish() = 0;
};

class data_writer_factory {
public:
    data_writer_factory() = default;
    data_writer_factory(const data_writer_factory&) = delete;
    data_writer_factory(data_writer_factory&&) = default;
    data_writer_factory& operator=(const data_writer_factory&) = delete;
    data_writer_factory& operator=(data_writer_factory&&) = default;
    virtual ~data_writer_factory() = default;

    virtual ss::future<result<ss::shared_ptr<data_writer>, data_writer_error>>
      create_writer(iceberg::struct_type /* schema */) = 0;
};

} // namespace datalake

namespace std {
template<>
struct is_error_code_enum<datalake::data_writer_error> : true_type {};
} // namespace std

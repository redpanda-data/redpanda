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
#include "model/fundamental.h"
#include "serde/envelope.h"

#include <cstddef>
#include <memory>

#pragma once

namespace datalake {

// Corresponds to a single file.
struct data_file_result
  : serde::envelope<
      data_file_result,
      serde::version<0>,
      serde::compat_version<0>> {
    size_t row_count = 0;

    auto serde_fields() { return std::tie(row_count); }

    // new stuff
    size_t file_size_bytes = 0;
    ss::sstring remote_path = "";
    // TODO: add kafka schema id

    friend std::ostream&
    operator<<(std::ostream& o, const data_file_result&) {
        return o;
    }

    auto serde_fields() { return std::tie(row_count, file_size_bytes, remote_path); }
};

struct translated_offset_range
  : serde::envelope<
      translated_offset_range,
      serde::version<0>,
      serde::compat_version<0>> {
    kafka::offset start_offset;
    kafka::offset last_offset;
    chunked_vector<data_file_result> files;
};

class data_writer {
public:
    virtual ~data_writer() = default;

    // TODO: error type?
    virtual bool add_data_struct(
      iceberg::struct_value /* data */, int64_t /* approx_size */)
      = 0;

    virtual data_file_result finish() = 0;
};

class data_writer_factory {
public:
    virtual ~data_writer_factory() = default;

    virtual std::unique_ptr<data_writer>
      create_writer(iceberg::struct_type /* schema */) = 0;
};
} // namespace datalake

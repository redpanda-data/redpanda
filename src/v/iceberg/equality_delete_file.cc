/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "iceberg/equality_delete_file.h"

#include "bytes/iostream.h"
#include "serde/parquet/writer.h"

namespace iceberg {

ss::future<delete_file_result> write_equality_delete_file(
  equality_delete_options opts,
  chunked_vector<serde::parquet::group_value> rows) {
    iobuf file;
    serde::parquet::writer w(
      {.schema = std::move(opts.parquet_schema), .compress = opts.compress},
      make_iobuf_ref_output_stream(file));
    co_await w.init();

    auto record_count = rows.size();
    for (auto& row : rows) {
        co_await w.write_row(std::move(row));
    }

    co_await w.close();

    data_file df{
      .content_type = data_file_content_type::equality_deletes,
      .file_path = uri{},
      .file_format = data_file_format::parquet,
      .partition = partition_key{},
      .record_count = record_count,
      .file_size_bytes = file.size_bytes(),
      .equality_ids = std::move(opts.equality_field_ids),
    };

    co_return delete_file_result{
      .file_data = std::move(file),
      .manifest_entry = std::move(df),
    };
}

} // namespace iceberg

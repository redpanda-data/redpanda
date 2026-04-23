/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#include "datalake/tests/e2e_test_utils.h"

#include "datalake/coordinator/data_file.h"
#include "iceberg/values_bytes.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/file.hh>
#include <seastar/core/seastar.hh>

#include <stdexcept>

namespace datalake::tests {

namespace {

/// Upload a single local parquet file to S3 and return its coordinator
/// data_file representation.
ss::future<coordinator::data_file> upload_one(
  iceberg::manifest_io& io,
  const partitioning_writer::partitioned_file& pf,
  bool is_delete) {
    // Build the full remote path: base / partition / filename
    auto remote = pf.data_location() / pf.partition_key_path()
                  / pf.local_file.path().filename();

    // Read the local file into an iobuf.
    auto file = co_await ss::open_file_dma(
      pf.local_file.path().string(), ss::open_flags::ro);
    auto size = co_await file.size();
    auto buf = co_await file.dma_read_bulk<char>(0, size);
    co_await file.close();
    iobuf data;
    data.append(std::move(buf));

    // Upload.
    auto uri = io.to_uri(remote);
    auto res = co_await io.upload_object_bytes(uri, std::move(data));
    if (res.has_error()) {
        throw std::runtime_error(fmt::format("Failed to upload file: {}", uri));
    }

    // Convert to coordinator data_file.
    coordinator::data_file df;
    df.remote_path = remote.string();
    df.row_count = pf.local_file.row_count;
    df.file_size_bytes = pf.local_file.size_bytes;
    df.table_schema_id = static_cast<int32_t>(pf.schema_id());
    df.partition_spec_id = static_cast<int32_t>(pf.partition_spec_id());
    df.is_delete = is_delete;

    // Serialize partition key values.
    if (pf.partition_key.val) {
        for (const auto& field : pf.partition_key.val->fields) {
            if (field.has_value()) {
                df.partition_key.push_back(iceberg::value_to_bytes(*field));
            } else {
                df.partition_key.push_back(std::nullopt);
            }
        }
    }

    // Convert key field IDs.
    if (pf.key_field_ids) {
        chunked_vector<int32_t> ids;
        for (auto id : *pf.key_field_ids) {
            ids.push_back(static_cast<int32_t>(id()));
        }
        df.delete_key_field_ids = std::move(ids);
    }

    co_return df;
}

ss::future<> upload_list(
  iceberg::manifest_io& io,
  const chunked_vector<partitioning_writer::partitioned_file>& pfs,
  bool is_delete,
  chunked_vector<coordinator::data_file>& out) {
    for (const auto& pf : pfs) {
        out.push_back(co_await upload_one(io, pf, is_delete));
    }
}

} // namespace

ss::future<coordinator::translated_offset_range> upload_and_convert(
  iceberg::manifest_io& io,
  const record_multiplexer::write_result& result,
  const record_multiplexer::finished_files& files) {
    coordinator::translated_offset_range tor;
    tor.start_offset = result.start_offset;
    tor.last_offset = result.last_offset;
    tor.kafka_bytes_processed = result.kafka_bytes_processed;

    co_await upload_list(io, files.data_files, false, tor.files);
    co_await upload_list(io, files.delete_files, true, tor.files);
    co_await upload_list(io, files.dlq_files, false, tor.dlq_files);

    co_return tor;
}

} // namespace datalake::tests

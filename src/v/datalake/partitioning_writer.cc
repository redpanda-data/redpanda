/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#include "datalake/partitioning_writer.h"

#include "base/vlog.h"
#include "datalake/data_writer_interface.h"
#include "datalake/logger.h"
#include "datalake/partition_key_path.h"

#include <exception>

namespace datalake {

std::ostream&
operator<<(std::ostream& o, const partitioning_writer::partitioned_file& f) {
    fmt::print(
      o,
      "{{local_file: {}, schema_id: {}, partition_spec_id: {}, "
      "partition_key: {}}}",
      f.local_file,
      f.schema_id,
      f.partition_spec_id,
      f.partition_key.val);
    return o;
}

ss::future<> partitioning_writer::flush() {
    return ss::max_concurrent_for_each(writers_, 10, [](auto& entry) {
        return entry.second->flush().then([](writer_error err) {
            if (err == writer_error::ok) {
                return ss::make_ready_future();
            }
            return ss::make_exception_future<>(std::runtime_error(
              fmt::format("Error flushing parquet file writer: {}", err)));
        });
    });
}

ss::future<writer_error> partitioning_writer::add_data(
  iceberg::struct_value val, int64_t approx_size, ss::abort_source& as) {
    iceberg::partition_key pk;
    try {
        pk = iceberg::partition_key::create(val, accessors_, spec_);
    } catch (...) {
        vlog(
          datalake_log.error,
          "Error {} while partitioning value: {}",
          std::current_exception(),
          val);
        co_return writer_error::parquet_conversion_error;
    }
    auto writer_iter = writers_.find(pk);
    if (writer_iter == writers_.end()) {
        auto writer_res = co_await writer_factory_.create_writer(type_, as);
        if (writer_res.has_error()) {
            vlog(
              datalake_log.error,
              "Failed to create new writer: {}",
              writer_res.error());
            co_return writer_res.error();
        }
        auto new_iter = writers_.emplace(
          pk.copy(), std::move(writer_res.value()));
        writer_iter = new_iter.first;
    }
    auto& writer = writer_iter->second;
    auto write_res = co_await writer->add_data_struct(
      std::move(val), approx_size, as);
    if (write_res != writer_error::ok) {
        auto is_shutdown_error = write_res == writer_error::shutting_down;
        vlogl(
          datalake_log,
          is_recoverable_error(write_res) || is_shutdown_error
            ? ss::log_level::debug
            : ss::log_level::error,
          "Failed to add data: {}",
          write_res);
        co_return write_res;
    }
    co_return write_res;
}

size_t partitioning_writer::buffered_bytes() const {
    size_t result = 0;
    for (const auto& [_, writer] : writers_) {
        result += writer->buffered_bytes();
    }
    return result;
}

size_t partitioning_writer::flushed_bytes() const {
    size_t result = 0;
    for (const auto& [_, writer] : writers_) {
        result += writer->flushed_bytes();
    }
    return result;
}

ss::future<
  result<chunked_vector<partitioning_writer::partitioned_file>, writer_error>>
partitioning_writer::finish() && {
    chunked_vector<partitioned_file> files;
    auto first_error = writer_error::ok;
    // TODO: parallelize me!
    for (auto& [pk, writer] : writers_) {
        auto file_res = co_await writer->finish();
        if (file_res.has_error()) {
            auto is_shutdown_error = file_res.error()
                                     == writer_error::shutting_down;
            vlogl(
              datalake_log,
              is_shutdown_error ? ss::log_level::debug : ss::log_level::error,
              "Failed to finish writer: {}",
              file_res.error());
            if (first_error == writer_error::ok) {
                first_error = file_res.error();
            }
            // Even on error, move on so that we can close all the writers.
            continue;
        }
        auto partition_key_path_res = partition_key_to_path(spec_, pk);
        if (partition_key_path_res.has_error()) {
            vlog(
              datalake_log.error,
              "Failed to convert partition key to remote path - {}",
              partition_key_path_res.error().what());
            continue;
        }

        files.push_back(partitioned_file{
          .local_file = std::move(file_res.value()),
          .data_location = remote_prefix_,
          .schema_id = schema_id_,
          .partition_spec_id = spec_.spec_id,
          .partition_key = std::move(pk),
          .partition_key_path = std::move(partition_key_path_res.value())});
    }
    if (first_error != writer_error::ok) {
        co_return first_error;
    }
    co_return files;
}

} // namespace datalake

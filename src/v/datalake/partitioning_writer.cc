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
#include "datalake/table_definition.h"
#include "iceberg/struct_accessor.h"

#include <exception>

namespace datalake {

namespace {
const auto hourly_spec = hour_partition_spec();
const auto default_schema = schemaless_struct_type();
const auto default_accessors = iceberg::struct_accessor::from_struct_type(
  default_schema);
} // namespace

ss::future<writer_error>
partitioning_writer::add_data(iceberg::struct_value val, int64_t approx_size) {
    iceberg::partition_key pk;
    try {
        pk = iceberg::partition_key::create(
          val, default_accessors, hourly_spec);
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
        auto writer_res = co_await writer_factory_.create_writer(type_);
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
      std::move(val), approx_size);
    if (write_res != writer_error::ok) {
        vlog(datalake_log.error, "Failed to add data: {}", write_res);
        co_return write_res;
    }
    co_return write_res;
}

ss::future<result<chunked_vector<local_file_metadata>, writer_error>>
partitioning_writer::finish() && {
    chunked_vector<local_file_metadata> files;
    auto first_error = writer_error::ok;
    // TODO: parallelize me!
    for (auto& [pk, writer] : writers_) {
        int hour = std::get<iceberg::int_value>(
                     std::get<iceberg::primitive_value>(*pk.val->fields[0]))
                     .val;
        auto file_res = co_await writer->finish();
        if (file_res.has_error()) {
            vlog(
              datalake_log.error,
              "Failed to finish writer: {}",
              file_res.error());
            if (first_error == writer_error::ok) {
                first_error = file_res.error();
            }
            // Even on error, move on so that we can close all the writers.
            continue;
        }
        auto& file = file_res.value();
        file.hour = hour;
        files.emplace_back(std::move(file));
    }
    if (first_error != writer_error::ok) {
        co_return first_error;
    }
    co_return files;
}

} // namespace datalake

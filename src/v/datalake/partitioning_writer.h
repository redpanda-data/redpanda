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

#include "container/chunked_hash_map.h"
#include "datalake/data_writer_interface.h"
#include "iceberg/datatypes.h"
#include "iceberg/partition_key.h"
#include "iceberg/schema.h"
#include "iceberg/values.h"

namespace iceberg {
struct struct_type;
} // namespace iceberg

namespace datalake {

// A wrapper around multiple data writers that all share the same schema and
// are partitioned by an Iceberg partition key.
//
// Uses the default partition spec to partition. As such, this class expects
// that schemas and values given as inputs are constructed with the default
// ("schemaless") schema and fields at the front.
class partitioning_writer {
public:
    explicit partitioning_writer(
      parquet_file_writer_factory& factory,
      iceberg::schema::id_t schema_id,
      iceberg::struct_type type,
      iceberg::partition_spec spec,
      remote_path remote_prefix)
      : writer_factory_(factory)
      , schema_id_(schema_id)
      , type_(std::move(type))
      , accessors_(iceberg::struct_accessor::from_struct_type(type_))
      , spec_(std::move(spec))
      , remote_prefix_(std::move(remote_prefix)) {}

    // Adds the given value to the writer corresponding to the value's
    // partition key.
    //
    // Expects that the input value abides by the schema denoted by `type_`.
    ss::future<writer_error>
    add_data(iceberg::struct_value, int64_t approx_size, ss::abort_source&);

    size_t buffered_bytes() const;
    size_t flushed_bytes() const;

    // Flushes all the inflight writers.
    ss::future<> flush();

    struct partitioned_file {
        local_file_metadata local_file;
        remote_path data_location;
        iceberg::schema::id_t schema_id;
        iceberg::partition_spec::id_t partition_spec_id;
        iceberg::partition_key partition_key;
        remote_path partition_key_path;

        friend std::ostream& operator<<(std::ostream&, const partitioned_file&);
    };

    // Finishes and returns the list of local files written by the underlying
    // writers, with the appropriate partitioning metadata filled in.
    ss::future<result<chunked_vector<partitioned_file>, writer_error>>
    finish() &&;

private:
    // Factory for data writers.
    parquet_file_writer_factory& writer_factory_;

    iceberg::schema::id_t schema_id_;
    // The Iceberg message type for the underlying writer. Expected to include
    // Redpanda-specific fields, e.g. a timestamp field for partitioning.
    const iceberg::struct_type type_;
    iceberg::struct_accessor::ids_accessor_map_t accessors_;
    iceberg::partition_spec spec_;
    remote_path remote_prefix_;

    // Map of partition keys to their corresponding data file writers.
    chunked_hash_map<
      iceberg::partition_key,
      std::unique_ptr<parquet_file_writer>>
      writers_;
};

} // namespace datalake

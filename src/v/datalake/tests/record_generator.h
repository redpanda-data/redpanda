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

#include "base/seastarx.h"
#include "bytes/iobuf.h"
#include "container/chunked_hash_map.h"
#include "model/record.h"
#include "model/timestamp.h"
#include "pandaproxy/schema_registry/types.h"
#include "storage/record_batch_builder.h"
#include "utils/named_type.h"

#include <seastar/core/future.hh>

namespace schema {
class registry;
} // namespace schema

namespace datalake::tests {

class record_generator {
public:
    explicit record_generator(schema::registry* sr)
      : _sr(sr) {}
    using error = named_type<ss::sstring, struct error_tag>;

    // Registers the given schema with the given name.
    ss::future<checked<std::nullopt_t, error>>
    register_avro_schema(std::string_view name, std::string_view schema);

    // Adds a record of the given schema to the builder.
    ss::future<checked<std::nullopt_t, error>> add_random_avro_record(
      storage::record_batch_builder&,
      std::string_view schema_name,
      std::optional<iobuf> key);

private:
    chunked_hash_map<std::string_view, pandaproxy::schema_registry::schema_id>
      _id_by_name;
    schema::registry* _sr;
};

} // namespace datalake::tests

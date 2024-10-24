/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#include "datalake/record_schema_resolver.h"

#include "base/vlog.h"
#include "datalake/logger.h"
#include "datalake/schema_identifier.h"
#include "datalake/schema_registry.h"
#include "iceberg/schema_avro.h"
#include "pandaproxy/schema_registry/types.h"
#include "schema/registry.h"

#include <seastar/coroutine/as_future.hh>

#include <exception>

namespace datalake {

namespace {

namespace ppsr = pandaproxy::schema_registry;
struct schema_translating_visitor {
    schema_translating_visitor(iobuf b, ppsr::schema_id id)
      : buf_no_id(std::move(b))
      , id(id) {}
    // Buffer without the schema ID.
    iobuf buf_no_id;
    ppsr::schema_id id;

    ss::future<checked<type_and_buf, record_schema_resolver::errc>>
    operator()(const ppsr::avro_schema_definition& avro_def) {
        const auto& avro_schema = avro_def();
        try {
            // TODO: we should use Avro value conversion instead of this
            // Iceberg-specific code.
            auto type = iceberg::type_from_avro(
              avro_schema.root(), iceberg::with_field_ids::no);
            co_return type_and_buf{
              .type = resolved_type{
                .schema = avro_schema,
                .id = { .schema_id = id, .protobuf_offsets = std::nullopt, },
                .type = std::move(type),
                .type_name = avro_schema.root()->name().fullname(),
              },
              .parsable_buf = std::move(buf_no_id),
            };
        } catch (...) {
            vlog(
              datalake_log.error,
              "Avro schema translation failed: {}",
              std::current_exception());
            co_return record_schema_resolver::errc::translation_error;
        }
    }
    ss::future<checked<type_and_buf, record_schema_resolver::errc>>
    operator()(const ppsr::protobuf_schema_definition&) {
        // XXX: a subsequent PR will add protobuf support.
        co_return type_and_buf::make_raw_binary(std::move(buf_no_id));
    }
    ss::future<checked<type_and_buf, record_schema_resolver::errc>>
    operator()(const ppsr::json_schema_definition&) {
        co_return type_and_buf::make_raw_binary(std::move(buf_no_id));
    }
};

} // namespace

type_and_buf type_and_buf::make_raw_binary(iobuf b) {
    return type_and_buf{
      .type = std::nullopt,
      .parsable_buf = std::move(b),
    };
}

ss::future<checked<type_and_buf, record_schema_resolver::errc>>
record_schema_resolver::resolve_buf_type(iobuf b) const {
    schema_message_data schema_id_res;
    try {
        // NOTE: Kafka's serialization protocol relies on a magic byte to
        // indicate if we have a schema. This has room for false positives, and
        // we can't say for sure if an error is the result of the record not
        // having a schema. Just translate to binary.
        auto res = get_value_schema_id(b);
        if (res.has_error()) {
            vlog(
              datalake_log.trace,
              "Error parsing schema ID; using binary type: {}",
              res.error());
            co_return type_and_buf::make_raw_binary(std::move(b));
        }
        schema_id_res = std::move(res.value());
    } catch (...) {
        vlog(
          datalake_log.trace,
          "Error parsing schema ID; using binary type: {}",
          std::current_exception());
        co_return type_and_buf::make_raw_binary(std::move(b));
    }
    auto schema_id = schema_id_res.schema_id;
    auto buf_no_id = std::move(schema_id_res.shared_message_data);

    // TODO: It'd be nice to cache these -- translation interval instills a
    // natural limit to concurrency so a cache wouldn't grow huge.
    auto schema_fut = co_await ss::coroutine::as_future(
      sr_.get_valid_schema(schema_id));
    if (schema_fut.failed()) {
        vlog(
          datalake_log.warn,
          "Error getting schema from registry: {}",
          schema_fut.get_exception());
        // TODO: make schema::registry tell us whether the issue was transient.
        co_return errc::registry_error;
    }
    auto resolved_schema = std::move(schema_fut.get());
    co_return co_await resolved_schema.visit(
      schema_translating_visitor{std::move(buf_no_id), schema_id});
}

} // namespace datalake

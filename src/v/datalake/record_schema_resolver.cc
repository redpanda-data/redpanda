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
#include "datalake/schema_avro.h"
#include "datalake/schema_identifier.h"
#include "datalake/schema_protobuf.h"
#include "datalake/schema_registry.h"
#include "pandaproxy/schema_registry/protobuf.h"
#include "pandaproxy/schema_registry/types.h"
#include "schema/registry.h"

#include <seastar/coroutine/as_future.hh>

#include <google/protobuf/descriptor.h>

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

    ss::future<checked<type_and_buf, type_resolver::errc>>
    operator()(ppsr::avro_schema_definition&& avro_def) {
        const auto& avro_schema = avro_def();
        try {
            auto result = datalake::type_to_iceberg(avro_schema.root());
            if (result.has_error()) {
                vlog(
                  datalake_log.error,
                  "Avro schema translation failed: {}",
                  result.error());
                co_return type_resolver::errc::translation_error;
            }

            co_return type_and_buf{
              .type = resolved_type{
                .schema = avro_schema,
                .id = { .schema_id = id, .protobuf_offsets = std::nullopt, },
                .type = std::move(result.value()),
                .type_name = avro_schema.root()->name().fullname(),
              },
              .parsable_buf = std::move(buf_no_id),
                };

        } catch (...) {
            vlog(
              datalake_log.error,
              "Avro schema translation failed: {}",
              std::current_exception());
            co_return type_resolver::errc::translation_error;
        }
    }
    ss::future<checked<type_and_buf, type_resolver::errc>>
    operator()(ppsr::protobuf_schema_definition&& pb_def) {
        const google::protobuf::Descriptor* d;
        proto_offsets_message_data offsets;
        try {
            auto offsets_res = get_proto_offsets(buf_no_id);
            if (offsets_res.has_error()) {
                co_return type_resolver::errc::bad_input;
            }
            offsets = std::move(offsets_res.value());
            // TODO: maybe there's another caching opportunity here.
            auto d_res = descriptor(pb_def, offsets.protobuf_offsets);
            if (d_res.has_error()) {
                co_return type_resolver::errc::bad_input;
            }
            d = &d_res.value().get();
        } catch (...) {
            vlog(
              datalake_log.error,
              "Error getting protobuf offsets from buffer: {}",
              std::current_exception());
            co_return type_resolver::errc::bad_input;
        }
        try {
            auto type = type_to_iceberg(*d).value();
            co_return type_and_buf{
              .type = resolved_type{
                .schema = wrapped_protobuf_descriptor { *d, std::move(pb_def) },
                .id = {.schema_id = id, .protobuf_offsets = std::move(offsets.protobuf_offsets)},
                .type = std::move(type),
                .type_name = d->name(),
              },
              .parsable_buf = std::move(offsets.shared_message_data),
            };
        } catch (...) {
            vlog(
              datalake_log.error,
              "Protobuf schema translation failed: {}",
              std::current_exception());
            co_return type_resolver::errc::translation_error;
        }
    }
    ss::future<checked<type_and_buf, type_resolver::errc>>
    operator()(ppsr::json_schema_definition&&) {
        co_return type_and_buf::make_raw_binary(std::move(buf_no_id));
    }
};

} // namespace

std::ostream& operator<<(std::ostream& o, const type_resolver::errc& e) {
    switch (e) {
    case type_resolver::errc::registry_error:
        return o << "type_resolver::errc::registry_error";
    case type_resolver::errc::translation_error:
        return o << "type_resolver::errc::translation_error";
    case type_resolver::errc::bad_input:
        return o << "type_resolver::errc::bad_input";
    }
}

type_and_buf type_and_buf::make_raw_binary(iobuf b) {
    return type_and_buf{
      .type = std::nullopt,
      .parsable_buf = std::move(b),
    };
}

ss::future<checked<type_and_buf, type_resolver::errc>>
binary_type_resolver::resolve_buf_type(iobuf b) const {
    co_return type_and_buf::make_raw_binary(std::move(b));
}

ss::future<checked<type_and_buf, type_resolver::errc>>
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
    co_return co_await std::move(resolved_schema)
      .visit(schema_translating_visitor{std::move(buf_no_id), schema_id});
}

} // namespace datalake

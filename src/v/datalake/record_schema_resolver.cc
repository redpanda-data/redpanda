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

checked<resolved_type, type_resolver::errc> translate_avro_schema(
  const ppsr::avro_schema_definition& avro_def, ppsr::schema_id id) {
    const auto& avro_schema = avro_def();
    try {
        auto result = datalake::type_to_iceberg(avro_schema.root());
        if (result.has_error()) {
            vlog(
              datalake_log.error,
              "Avro schema translation failed: {}",
              result.error());
            return type_resolver::errc::translation_error;
        }

        return resolved_type{
                .schema = avro_schema,
                .id = { .schema_id = id, .protobuf_offsets = std::nullopt, },
                .type = std::move(result.value()),
                .type_name = avro_schema.root()->name().fullname(),
              };
    } catch (...) {
        vlog(
          datalake_log.error,
          "Avro schema translation failed: {}",
          std::current_exception());
        return type_resolver::errc::translation_error;
    }
}

checked<resolved_type, type_resolver::errc> translate_protobuf_schema(
  ppsr::protobuf_schema_definition&& pb_def,
  ppsr::schema_id id,
  std::vector<int32_t> protobuf_offsets) {
    // TODO: maybe there's another caching opportunity here.
    auto d_res = descriptor(pb_def, protobuf_offsets);
    if (d_res.has_error()) {
        return type_resolver::errc::bad_input;
    }
    const auto* d = &d_res.value().get();
    try {
        auto type = type_to_iceberg(*d).value();
        return resolved_type{
          .schema = wrapped_protobuf_descriptor{*d, std::move(pb_def)},
          .id
          = {.schema_id = id, .protobuf_offsets = std::move(protobuf_offsets)},
          .type = std::move(type),
          .type_name = d->name(),
        };
    } catch (...) {
        vlog(
          datalake_log.error,
          "Protobuf schema translation failed: {}",
          std::current_exception());
        return type_resolver::errc::translation_error;
    }
}

struct schema_translating_visitor {
    schema_translating_visitor(iobuf b, ppsr::schema_id id)
      : buf_no_id(std::move(b))
      , id(id) {}
    // Buffer without the schema ID.
    iobuf buf_no_id;
    ppsr::schema_id id;

    checked<type_and_buf, type_resolver::errc>
    operator()(ppsr::avro_schema_definition&& avro_def) {
        auto tr_res = translate_avro_schema(avro_def, id);
        if (tr_res.has_error()) {
            return tr_res.error();
        }
        return type_and_buf{
          .type = std::move(tr_res.value()),
          .parsable_buf = std::move(buf_no_id)};
    }

    checked<type_and_buf, type_resolver::errc>
    operator()(ppsr::protobuf_schema_definition&& pb_def) {
        auto offsets_res = get_proto_offsets(buf_no_id);
        if (offsets_res.has_error()) {
            return type_resolver::errc::bad_input;
        }
        auto offsets = std::move(offsets_res.value());

        auto tr_res = translate_protobuf_schema(
          std::move(pb_def), id, std::move(offsets.protobuf_offsets));
        if (tr_res.has_error()) {
            return tr_res.error();
        }

        return type_and_buf{
          .type = std::move(tr_res.value()),
          .parsable_buf = std::move(offsets.shared_message_data)};
    }

    checked<type_and_buf, type_resolver::errc>
    operator()(ppsr::json_schema_definition&&) {
        return type_resolver::errc::bad_input;
    }
};

struct from_identifier_visitor {
    from_identifier_visitor(schema_identifier ident)
      : ident(std::move(ident)) {}

    schema_identifier ident;

    checked<resolved_type, type_resolver::errc>
    operator()(ppsr::avro_schema_definition&& avro_def) {
        if (ident.protobuf_offsets) {
            return type_resolver::errc::bad_input;
        }
        return translate_avro_schema(avro_def, ident.schema_id);
    }
    checked<resolved_type, type_resolver::errc>
    operator()(ppsr::protobuf_schema_definition&& pb_def) {
        if (!ident.protobuf_offsets) {
            return type_resolver::errc::bad_input;
        }
        return translate_protobuf_schema(
          std::move(pb_def),
          ident.schema_id,
          std::move(ident.protobuf_offsets.value()));
    }
    checked<resolved_type, type_resolver::errc>
    operator()(ppsr::json_schema_definition&&) {
        return type_resolver::errc::bad_input;
    }
};

ss::future<checked<ppsr::valid_schema, type_resolver::errc>>
get_schema(schema::registry& sr, ppsr::schema_id id) {
    if (!sr.is_enabled()) {
        vlog(datalake_log.warn, "Schema registry is not enabled");
        // TODO: should we treat this as transient?
        co_return type_resolver::errc::translation_error;
    }
    // TODO: It'd be nice to cache these -- translation interval instills a
    // natural limit to concurrency so a cache wouldn't grow huge.
    auto schema_fut = co_await ss::coroutine::as_future(
      sr.get_valid_schema(id));
    if (schema_fut.failed()) {
        vlog(
          datalake_log.warn,
          "Error getting schema from registry: {}",
          schema_fut.get_exception());
        co_return type_resolver::errc::registry_error;
    }
    auto resolved_schema = std::move(schema_fut.get());
    if (!resolved_schema.has_value()) {
        vlog(
          datalake_log.trace,
          "Schema ID {} not in registry; using binary type",
          id);
        co_return type_resolver::errc::bad_input;
    }
    co_return std::move(resolved_schema.value());
}

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

type_and_buf type_and_buf::make_raw_binary(std::optional<iobuf> b) {
    return type_and_buf{
      .type = std::nullopt,
      .parsable_buf = std::move(b),
    };
}

ss::future<checked<type_and_buf, type_resolver::errc>>
binary_type_resolver::resolve_buf_type(std::optional<iobuf> b) const {
    co_return type_and_buf::make_raw_binary(std::move(b));
}

ss::future<checked<resolved_type, type_resolver::errc>>
binary_type_resolver::resolve_identifier(schema_identifier) const {
    // method is not expected to be called, as this resolver always returns
    // nullopt type.
    co_return type_resolver::errc::translation_error;
}

ss::future<checked<type_and_buf, type_resolver::errc>>
record_schema_resolver::resolve_buf_type(std::optional<iobuf> b) const {
    if (!b.has_value()) {
        vlog(datalake_log.trace, "Ignoring tombstone value");
        co_return errc::bad_input;
    }
    // NOTE: Kafka's serialization protocol relies on a magic byte to
    // indicate if we have a schema. This has room for false positives, and
    // we can't say for sure if an error is the result of the record not
    // having a schema. Just translate to binary.
    auto res = get_value_schema_id(*b);
    if (res.has_error()) {
        vlog(datalake_log.trace, "Error parsing schema ID: {}", res.error());
        co_return errc::bad_input;
    }
    auto schema_id_res = std::move(res.value());
    auto schema_id = schema_id_res.schema_id;
    auto buf_no_id = std::move(schema_id_res.shared_message_data);

    auto schema_res = co_await get_schema(sr_, schema_id);
    if (schema_res.has_error()) {
        co_return schema_res.error();
    }

    co_return std::move(schema_res.value())
      .visit(schema_translating_visitor{std::move(buf_no_id), schema_id});
}

ss::future<checked<resolved_type, type_resolver::errc>>
record_schema_resolver::resolve_identifier(schema_identifier ident) const {
    auto schema_res = co_await get_schema(sr_, ident.schema_id);
    if (schema_res.has_error()) {
        co_return schema_res.error();
    }

    co_return std::move(schema_res.value())
      .visit(from_identifier_visitor{std::move(ident)});
}

} // namespace datalake

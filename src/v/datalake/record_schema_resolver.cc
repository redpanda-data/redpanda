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
#include "datalake/conversion_outcome.h"
#include "datalake/logger.h"
#include "datalake/schema_identifier.h"
#include "datalake/schema_registry.h"
#include "iceberg/avro_utils.h"
#include "iceberg/datatypes.h"
#include "iceberg/schema_avro.h"
#include "iceberg/values.h"
#include "iceberg/values_avro.h"
#include "pandaproxy/schema_registry/types.h"
#include "schema/registry.h"

#include <seastar/coroutine/as_future.hh>

#include <avro/Decoder.hh>
#include <avro/Generic.hh>
#include <avro/GenericDatum.hh>

#include <exception>

namespace datalake {

namespace {

iceberg::value binary_val(iobuf b) {
    auto val = std::make_unique<iceberg::struct_value>();
    val->fields.emplace_back(iceberg::binary_value{std::move(b)});
    return iceberg::value{std::move(val)};
}

// Represents an object that can be converted into an Iceberg schema.
// NOTE: these aren't exactly just the Kafka schemas from the registry:
// Protobuf Kafka schemas FileDescriptors rather than Descriptors, and require
// additional information to get the Descriptors.
using resolved_schema
  = std::variant<std::reference_wrapper<const avro::ValidSchema>>;

struct type_and_buf {
    // The resolved schema that corresponds to the type.
    resolved_schema schema;

    // Protobuf offsets used to resolve the Protobuf schema, if applicable.
    std::optional<std::vector<int32_t>> pb_offsets;

    // The schema (and offsets, for protobuf), translated into an
    // Iceberg-compatible type. Note, the field IDs may not necessarily
    // correspond to their final IDs in the catalog.
    iceberg::field_type type;

    // Part of a record field (key or value) that conforms to the given Iceberg
    // field type.
    iobuf parsable_buf;
};

// TODO: if this file gets too unwieldy, move value translation out of here!
struct value_translating_visitor {
    // Buffer ready to be parsed, e.g. no schema ID or protobuf offsets.
    iobuf parsable_buf;
    const iceberg::field_type& type;

    ss::future<optional_value_outcome> operator()(const avro::ValidSchema& s) {
        avro::GenericDatum d(s);
        try {
            auto in = std::make_unique<iceberg::avro_iobuf_istream>(
              std::move(parsable_buf));
            auto decoder = avro::validatingDecoder(s, avro::binaryDecoder());
            decoder->init(*in);
            avro::decode(*decoder, d);
        } catch (...) {
            co_return value_conversion_exception(fmt::format(
              "Error reading Avro buffer: {}", std::current_exception()));
        }
        co_return iceberg::val_from_avro(d, type, iceberg::field_required::yes);
    }
};

namespace ppsr = pandaproxy::schema_registry;
struct schema_translating_visitor {
    enum class errc {
        invalid_buf,
        schema_translation_error,
        not_supported,
    };
    // Buffer without the schema ID.
    iobuf buf_no_id;

    ss::future<checked<type_and_buf, errc>>
    operator()(const ppsr::avro_schema_definition& avro_def) {
        const auto& avro_schema = avro_def();
        try {
            // TODO: we should use Avro value conversion instead of this
            // Iceberg-specific code.
            auto type = iceberg::type_from_avro(
              avro_schema.root(), iceberg::with_field_ids::no);
            co_return type_and_buf{
              .schema = avro_schema,
              .pb_offsets = std::nullopt,
              .type = std::move(type),
              .parsable_buf = std::move(buf_no_id),
            };
        } catch (...) {
            vlog(
              datalake_log.error,
              "Avro schema translation failed: {}",
              std::current_exception());
            co_return errc::schema_translation_error;
        }
    }
    ss::future<checked<type_and_buf, errc>>
    operator()(const ppsr::protobuf_schema_definition&) {
        // TODO: will be implemented in a subsequent commit!
        co_return errc::not_supported;
    }
    ss::future<checked<type_and_buf, errc>>
    operator()(const ppsr::json_schema_definition&) {
        co_return errc::not_supported;
    }
};

} // namespace

resolved_buf resolved_buf::make_raw_binary(iobuf b) {
    return resolved_buf{
      .schema_identifier = std::nullopt,
      .type = std::nullopt,
      .val = binary_val(std::move(b)),
    };
}

ss::future<checked<resolved_buf, record_schema_resolver::errc>>
record_schema_resolver::resolve_buf_schema(iobuf b) const {
    schema_message_data schema_id_res;
    try {
        // NOTE: Kafka's serialization protocol relies on a magic byte to
        // indicate if we have a schema. This has room for false positives, and
        // we can't say for sure if an error is the result of the record not
        // having a schema. Just translate to binary.
        auto res = get_value_schema_id(b);
        if (res.has_error()) {
            co_return resolved_buf::make_raw_binary(std::move(b));
        }
        schema_id_res = std::move(res.value());
    } catch (...) {
        co_return resolved_buf::make_raw_binary(std::move(b));
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
    auto typed_buf_res = co_await resolved_schema.visit(
      schema_translating_visitor{std::move(buf_no_id)});
    if (typed_buf_res.has_error()) {
        switch (typed_buf_res.error()) {
        case schema_translating_visitor::errc::invalid_buf:
            // TODO: an external dead-letter queue may be desirable here.
            // TODO: metric for data translation errors.
        case schema_translating_visitor::errc::not_supported:
        case schema_translating_visitor::errc::schema_translation_error:
            // TODO: metric for schema translation errors.
            co_return resolved_buf::make_raw_binary(std::move(b));
        }
    }
    auto& typed_buf = typed_buf_res.value();
    auto val = co_await std::visit(
      value_translating_visitor{
        std::move(typed_buf.parsable_buf), typed_buf.type},
      typed_buf.schema);
    if (val.has_error()) {
        vlog(datalake_log.error, "Error converting buffer: {}", val.error());
        // TODO: metric for data translation errors.
        co_return resolved_buf::make_raw_binary(std::move(b));
    }
    co_return resolved_buf{
      .schema_identifier
      = schema_identifier{
          .schema_id = schema_id, .protobuf_offsets = std::move(typed_buf.pb_offsets)},
      .type = std::move(typed_buf.type),
      .val = std::move(val.value()),
    };
}

} // namespace datalake

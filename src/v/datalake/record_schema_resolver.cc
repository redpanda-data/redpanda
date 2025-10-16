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
#include "config/configuration.h"
#include "datalake/logger.h"
#include "datalake/schema_identifier.h"
#include "datalake/schema_registry.h"
#include "iceberg/conversion/ir_json.h"
#include "iceberg/conversion/json_schema/frontend.h"
#include "iceberg/conversion/schema_avro.h"
#include "iceberg/conversion/schema_json.h"
#include "iceberg/conversion/schema_protobuf.h"
#include "iceberg/datatypes.h"
#include "metrics/prometheus_sanitize.h"
#include "pandaproxy/schema_registry/json.h"
#include "pandaproxy/schema_registry/protobuf.h"
#include "pandaproxy/schema_registry/types.h"
#include "schema/registry.h"

#include <seastar/core/metrics.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/coroutine/as_future.hh>

#include <google/protobuf/descriptor.h>

#include <algorithm>
#include <exception>
#include <optional>

namespace datalake {

namespace {

namespace ppsr = pandaproxy::schema_registry;

checked<resolved_type, type_resolver::errc> translate_avro_schema(
  const ppsr::avro_schema_definition& avro_def,
  ppsr::schema_id id,
  shared_schema_t schema) {
    const auto& avro_schema = avro_def();
    try {
        auto result = iceberg::type_to_iceberg(avro_schema.root());
        if (result.has_error()) {
            vlog(
              datalake_log.error,
              "Avro schema translation failed: {}",
              result.error());
            return type_resolver::errc::translation_error;
        }

        return resolved_type{
                .schema = resolved_schema(std::cref(avro_schema), std::move(schema)),
                .id = { .schema_id = id, .protobuf_offsets = std::nullopt, },
                .type = std::move(result.value()),
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
  const ppsr::protobuf_schema_definition& pb_def,
  ppsr::schema_id id,
  std::vector<int32_t> protobuf_offsets,
  shared_schema_t schema) {
    // TODO: maybe there's another caching opportunity here.
    auto d_res = descriptor(pb_def, protobuf_offsets);
    if (d_res.has_error()) {
        vlog(
          datalake_log.error,
          "Failed to resolve Protobuf descriptor (missing offsets?): {}",
          d_res.error());
        return type_resolver::errc::bad_input;
    }
    const auto* d = &d_res.value().get();
    try {
        auto type = iceberg::type_to_iceberg(*d).value();
        return resolved_type{
          .schema = resolved_schema(*d, std::move(schema)),
          .id
          = {.schema_id = id, .protobuf_offsets = std::move(protobuf_offsets)},
          .type = std::move(type),
        };
    } catch (...) {
        vlog(
          datalake_log.error,
          "Protobuf schema translation failed: {}",
          std::current_exception());
        return type_resolver::errc::translation_error;
    }
}

checked<resolved_type, type_resolver::errc> translate_json_schema(
  const ppsr::json_schema_definition& json_def, ppsr::schema_id id) {
    try {
        auto& doc = document(json_def());
        auto fc = iceberg::conversion::json_schema::frontend{};
        // todo figure out
        // todo is this cached anywhere?
        auto json_schema = fc.compile(
          doc, "https://example.com/schema.json", std::nullopt);
        auto iceberg_ir = iceberg::type_to_ir(json_schema);
        if (iceberg_ir.has_error()) {
            vlog(
              datalake_log.error,
              "JSON schema translation to Iceberg IR failed: {}",
              iceberg_ir.error());
            return type_resolver::errc::translation_error;
        }

        auto root_struct = iceberg::type_to_iceberg(iceberg_ir.value());
        if (root_struct.has_error()) {
            vlog(
              datalake_log.error,
              "JSON schema translation to Iceberg type failed: {}",
              root_struct.error());
            return type_resolver::errc::translation_error;
        }

        return resolved_type{
          .schema = resolved_schema(
            ss::make_shared<iceberg::json_conversion_ir>(
              std::move(iceberg_ir.value()))),
          .id = {.schema_id = id, .protobuf_offsets = std::nullopt},
          .type = std::move(root_struct.value())};
    } catch (iceberg::conversion::json_schema::unsupported_feature_error& e) {
        vlog(
          datalake_log.warn,
          "JSON schema translation failed due to unsupported feature: {}",
          e.what());
        return type_resolver::errc::translation_error;
    } catch (...) {
        vlog(
          datalake_log.error,
          "JSON schema translation failed: {}",
          std::current_exception());
        return type_resolver::errc::translation_error;
    }
}

struct schema_translating_visitor {
    schema_translating_visitor(
      iobuf b, ppsr::schema_id id, shared_schema_t schema)
      : buf_no_id(std::move(b))
      , id(id)
      , schema(std::move(schema)) {}
    // Buffer without the schema ID.
    iobuf buf_no_id;
    ppsr::schema_id id;
    shared_schema_t schema;

    checked<type_and_buf, type_resolver::errc>
    operator()(const ppsr::avro_schema_definition& avro_def) {
        auto tr_res = translate_avro_schema(avro_def, id, schema);
        if (tr_res.has_error()) {
            return tr_res.error();
        }
        return type_and_buf{
          .type = std::move(tr_res.value()),
          .parsable_buf = std::move(buf_no_id)};
    }

    checked<type_and_buf, type_resolver::errc>
    operator()(const ppsr::protobuf_schema_definition& pb_def) {
        auto offsets_res = get_proto_offsets(buf_no_id);
        if (offsets_res.has_error()) {
            return type_resolver::errc::bad_input;
        }
        auto offsets = std::move(offsets_res.value());

        auto tr_res = translate_protobuf_schema(
          pb_def, id, std::move(offsets.protobuf_offsets), schema);
        if (tr_res.has_error()) {
            return tr_res.error();
        }

        return type_and_buf{
          .type = std::move(tr_res.value()),
          .parsable_buf = std::move(offsets.shared_message_data)};
    }

    checked<type_and_buf, type_resolver::errc>
    operator()(const ppsr::json_schema_definition& json_def) {
        auto tr_res = translate_json_schema(json_def, id);
        if (tr_res.has_error()) {
            return tr_res.error();
        }
        return type_and_buf{
          .type = std::move(tr_res.value()),
          .parsable_buf = std::move(buf_no_id)};
    }
};

struct from_identifier_visitor {
    from_identifier_visitor(schema_identifier ident, shared_schema_t schema)
      : ident(std::move(ident))
      , schema(std::move(schema)) {}

    schema_identifier ident;
    shared_schema_t schema;

    checked<resolved_type, type_resolver::errc>
    operator()(const ppsr::avro_schema_definition& avro_def) {
        if (ident.protobuf_offsets) {
            return type_resolver::errc::bad_input;
        }
        return translate_avro_schema(avro_def, ident.schema_id, schema);
    }
    checked<resolved_type, type_resolver::errc>
    operator()(const ppsr::protobuf_schema_definition& pb_def) {
        if (!ident.protobuf_offsets) {
            return type_resolver::errc::bad_input;
        }
        return translate_protobuf_schema(
          pb_def,
          ident.schema_id,
          std::move(ident.protobuf_offsets.value()),
          schema);
    }
    checked<resolved_type, type_resolver::errc>
    operator()(const ppsr::json_schema_definition& json_def) {
        if (ident.protobuf_offsets) {
            return type_resolver::errc::bad_input;
        }

        return translate_json_schema(json_def, ident.schema_id);
    }
};

ss::future<checked<shared_schema_t, type_resolver::errc>> get_schema(
  schema::registry* sr,
  std::optional<std::reference_wrapper<schema_cache>> cache,
  ppsr::schema_id id) {
    if (!sr->is_enabled()) {
        vlog(datalake_log.warn, "Schema registry is not enabled");
        // TODO: should we treat this as transient?
        co_return type_resolver::errc::translation_error;
    }
    if (cache.has_value()) {
        auto cached_schema = cache->get().get_value(id);

        if (cached_schema) {
            co_return std::move(*cached_schema);
        }
    }
    auto schema_fut = co_await ss::coroutine::as_future(
      sr->get_valid_schema(id));
    if (schema_fut.failed()) {
        vlog(
          datalake_log.warn,
          "Error getting schema from registry: {}",
          schema_fut.get_exception());
        co_return type_resolver::errc::registry_error;
    }
    auto resolved_schema = std::move(schema_fut.get());
    if (!resolved_schema.has_value()) {
        vlog(datalake_log.trace, "Schema ID {} not in registry", id);
        co_return type_resolver::errc::bad_input;
    }
    auto shared_schema = ss::make_shared(std::move(resolved_schema.value()));
    if (cache.has_value()) {
        cache->get().try_insert(id, shared_schema);
    }
    co_return std::move(shared_schema);
}

} // namespace

chunked_schema_cache::chunked_schema_cache(
  chunked_schema_cache::cache_t::config c)
  : cache_(c) {}

void chunked_schema_cache::start() { setup_metrics(); }

void chunked_schema_cache::stop() { metrics_.clear(); }

ss::optimized_optional<
  ss::shared_ptr<pandaproxy::schema_registry::valid_schema>>
chunked_schema_cache::get_value(
  const pandaproxy::schema_registry::schema_id& id) {
    return cache_.get_value(id);
}
bool chunked_schema_cache::try_insert(
  const key_t& key, ss::shared_ptr<val_t> val) {
    return cache_.try_insert(key, std::move(val));
}

void chunked_schema_cache::setup_metrics() {
    namespace sm = ss::metrics;

    if (config::shard_local_cfg().disable_metrics()) {
        return;
    }

    metrics_.add_group(
      prometheus_sanitize::metrics_name("datalake:schema_cache"),
      {
        sm::make_counter(
          "misses",
          [this] {
              auto stats = cache_.stat();
              return stats.access_count - stats.hit_count;
          },
          sm::description("The number of times a schema wasn't in the cache.")),
        sm::make_counter(
          "hits",
          [this] {
              auto stats = cache_.stat();
              return stats.hit_count;
          },
          sm::description("The number of times a schema was in the cache.")),
      });
}

resolved_schema::resolved_schema(ss::shared_ptr<iceberg::json_conversion_ir> ir)
  : shared_schema_(std::move(ir))
  , schema_(
      *std::get<ss::shared_ptr<iceberg::json_conversion_ir>>(shared_schema_)) {}

std::ostream& operator<<(std::ostream& o, const type_resolver::errc& e) {
    switch (e) {
    case type_resolver::errc::registry_error:
        return o << "type_resolver::errc::registry_error";
    case type_resolver::errc::translation_error:
        return o << "type_resolver::errc::translation_error";
    case type_resolver::errc::bad_input:
        return o << "type_resolver::errc::bad_input";
    case type_resolver::errc::invalid_config:
        return o << "type_resolver::errc::invalid_config";
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
test_binary_type_resolver::resolve_buf_type(std::optional<iobuf> b) const {
    if (injected_error_.has_value()) {
        co_return *injected_error_;
    }
    co_return co_await binary_type_resolver::resolve_buf_type(std::move(b));
}

ss::future<checked<resolved_type, type_resolver::errc>>
test_binary_type_resolver::resolve_identifier(schema_identifier id) const {
    if (injected_error_.has_value()) {
        co_return *injected_error_;
    }
    co_return co_await binary_type_resolver::resolve_identifier(std::move(id));
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

    auto schema_res = co_await get_schema(&sr_, cache_, schema_id);
    if (schema_res.has_error()) {
        co_return schema_res.error();
    }

    auto shared_schema = schema_res.value();
    co_return shared_schema->visit(
      schema_translating_visitor{
        std::move(buf_no_id), schema_id, shared_schema});
}

ss::future<checked<resolved_type, type_resolver::errc>>
record_schema_resolver::resolve_identifier(schema_identifier ident) const {
    auto schema_res = co_await get_schema(&sr_, cache_, ident.schema_id);
    if (schema_res.has_error()) {
        co_return schema_res.error();
    }

    auto shared_schema = schema_res.value();
    co_return shared_schema->visit(
      from_identifier_visitor{std::move(ident), shared_schema});
}

latest_subject_schema_resolver::latest_subject_schema_resolver(
  schema::registry& sr,
  ppsr::subject subject,
  std::optional<ss::sstring> protobuf_message_name,
  config::binding<std::chrono::milliseconds> cache_duration,
  std::optional<std::reference_wrapper<schema_cache>> sc)
  : sr_(&sr)
  , subject_(std::move(subject))
  , protobuf_message_name_(std::move(protobuf_message_name))
  , cache_duration_(std::move(cache_duration))
  , cache_(sc) {}

namespace {

checked<std::vector<int32_t>, type_resolver::errc> compute_message_offsets(
  const ppsr::protobuf_schema_definition& pb_def,
  std::string_view message_full_name) {
    auto d_res = ppsr::descriptor(pb_def, message_full_name);
    if (d_res.has_error()) {
        return type_resolver::errc::invalid_config;
    }
    // Build up the offsets by walking the descriptor tree
    std::vector<int> offsets;
    for (const google::protobuf::Descriptor* d = &d_res.value().get();
         d != nullptr;
         d = d->containing_type()) {
        offsets.push_back(d->index());
    }
    std::ranges::reverse(offsets);
    return offsets;
}

} // namespace

ss::future<checked<type_and_buf, type_resolver::errc>>
latest_subject_schema_resolver::resolve_buf_type(std::optional<iobuf> b) const {
    auto duration = std::chrono::duration_cast<ss::lowres_clock::duration>(
      cache_duration_());
    if (
      latest_cached_schema_
      && latest_cached_schema_->created_time + duration
           > ss::lowres_clock::now()) {
        co_return type_and_buf{
          .type = std::make_optional(latest_cached_schema_->type.copy()),
          .parsable_buf = std::move(b),
        };
    } else {
        latest_cached_schema_ = std::nullopt;
    }
    auto latest_schema_fut
      = co_await ss::coroutine::as_future<ppsr::stored_schema>(
        sr_->get_subject_schema(subject_, /*subject_version=*/std::nullopt));
    if (latest_schema_fut.failed()) {
        latest_schema_fut.ignore_ready_future();
        co_return type_resolver::errc::registry_error;
    }
    auto latest_schema = std::move(latest_schema_fut.get());
    auto schema_res = co_await get_schema(sr_, cache_, latest_schema.id);
    if (schema_res.has_error()) {
        co_return schema_res.error();
    }
    auto shared_schema = schema_res.value();
    auto resolve_res = shared_schema->visit(
      ss::make_visitor(
        [this, &latest_schema, &shared_schema](
          const ppsr::protobuf_schema_definition& pb_def)
          -> checked<resolved_type, type_resolver::errc> {
            std::vector<int32_t> offsets;
            if (const auto& explicit_name = protobuf_message_name_) {
                auto res = compute_message_offsets(pb_def, *explicit_name);
                if (res.has_error()) {
                    return res.error();
                }
                offsets = std::move(res.value());
            } else {
                offsets = {0};
            }
            return translate_protobuf_schema(
              pb_def, latest_schema.id, offsets, std::move(shared_schema));
        },
        [&latest_schema,
         &shared_schema](const ppsr::avro_schema_definition& def)
          -> checked<resolved_type, type_resolver::errc> {
            return translate_avro_schema(
              def, latest_schema.id, std::move(shared_schema));
        },
        [&latest_schema](const ppsr::json_schema_definition& def)
          -> checked<resolved_type, type_resolver::errc> {
            return translate_json_schema(def, latest_schema.id);
        }));
    if (resolve_res.has_error()) {
        co_return resolve_res.error();
    }
    auto resolved = std::move(resolve_res.value());
    latest_cached_schema_.emplace(resolved.copy(), ss::lowres_clock::now());
    co_return type_and_buf{
      .type = std::move(resolved),
      .parsable_buf = std::move(b),
    };
}

ss::future<checked<resolved_type, type_resolver::errc>>
latest_subject_schema_resolver::resolve_identifier(
  schema_identifier ident) const {
    auto schema_res = co_await get_schema(sr_, cache_, ident.schema_id);
    if (schema_res.has_error()) {
        co_return schema_res.error();
    }
    auto shared_schema = schema_res.value();
    co_return shared_schema->visit(
      from_identifier_visitor{std::move(ident), shared_schema});
}

resolved_type resolved_type::copy() const {
    return {
      .schema = schema,
      .id = id,
      .type = iceberg::make_copy(type),
    };
}
} // namespace datalake

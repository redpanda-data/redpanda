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
  schema_identifier&& id,
  shared_schema_t schema) {
    auto d_res = descriptor(pb_def, id.protobuf_offsets.value());
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
          .id = std::move(id),
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

struct from_identifier_visitor {
    from_identifier_visitor(schema_identifier&& ident, shared_schema_t&& schema)
      : ident(std::move(ident))
      , schema(std::move(schema)) {}

    schema_identifier ident;
    shared_schema_t schema;

    checked<resolved_type, type_resolver::errc>
    operator()(const ppsr::avro_schema_definition& avro_def) {
        if (ident.protobuf_offsets) {
            return type_resolver::errc::bad_input;
        }
        return translate_avro_schema(
          avro_def, ident.schema_id, std::move(schema));
    }
    checked<resolved_type, type_resolver::errc>
    operator()(const ppsr::protobuf_schema_definition& pb_def) {
        if (!ident.protobuf_offsets) {
            return type_resolver::errc::bad_input;
        }
        return translate_protobuf_schema(
          pb_def, std::move(ident), std::move(schema));
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
      sr->get_valid_schema({ppsr::default_context, id}));
    if (schema_fut.failed()) {
        auto ex = schema_fut.get_exception();
        vlog(datalake_log.warn, "Error getting schema from registry: {}", ex);
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

checked<shared_resolved_type_t, type_resolver::errc> get_resolved_type(
  schema_identifier&& ident,
  shared_schema_t&& schema,
  std::optional<std::reference_wrapper<resolved_type_cache>> cache) {
    if (cache.has_value()) {
        auto cached_val = cache->get().get_value(ident);
        if (cached_val) {
            return *cached_val;
        }
    }

    auto* schema_ptr = schema.get();
    auto resolve_res = schema_ptr->visit(
      from_identifier_visitor{std::move(ident), std::move(schema)});
    if (resolve_res.has_error()) {
        return resolve_res.error();
    }

    auto shared_val = ss::make_shared<resolved_type>(
      std::move(resolve_res.value()));
    if (cache.has_value()) {
        cache->get().try_insert(shared_val->id, shared_val);
    }

    return shared_val;
}

} // namespace

template<typename Key, typename Value>
struct datalake_cache_traits;

template<>
struct datalake_cache_traits<
  pandaproxy::schema_registry::schema_id,
  pandaproxy::schema_registry::valid_schema> {
    static constexpr const char* metrics_group_name = "datalake:schema_cache";
    static constexpr const char* item_label = "a schema";
};

template<>
struct datalake_cache_traits<schema_identifier, resolved_type> {
    static constexpr const char* metrics_group_name
      = "datalake:resolved_type_cache";
    static constexpr const char* item_label = "an Iceberg type";
};

template<typename Key, typename Value>
chunked_datalake_cache<Key, Value>::chunked_datalake_cache(
  typename cache_t::config c)
  : cache_(c) {}

template<typename Key, typename Value>
void chunked_datalake_cache<Key, Value>::start() {
    setup_metrics();
}

template<typename Key, typename Value>
void chunked_datalake_cache<Key, Value>::stop() {
    metrics_.clear();
}

template<typename Key, typename Value>
ss::optimized_optional<ss::shared_ptr<Value>>
chunked_datalake_cache<Key, Value>::get_value(const key_t& id) {
    return cache_.get_value(id);
}

template<typename Key, typename Value>
bool chunked_datalake_cache<Key, Value>::try_insert(
  const key_t& key, ss::shared_ptr<val_t> val) {
    return cache_.try_insert(key, std::move(val));
}

template<typename Key, typename Value>
void chunked_datalake_cache<Key, Value>::setup_metrics() {
    namespace sm = ss::metrics;
    using traits = datalake_cache_traits<Key, Value>;

    if (config::shard_local_cfg().disable_metrics()) {
        return;
    }

    metrics_.add_group(
      prometheus_sanitize::metrics_name(traits::metrics_group_name),
      {
        sm::make_counter(
          "misses",
          [this] {
              auto stats = cache_.stat();
              return stats.access_count - stats.hit_count;
          },
          sm::description(
            fmt::format(
              "The number of times {} wasn't in the cache.",
              traits::item_label))),
        sm::make_counter(
          "hits",
          [this] {
              auto stats = cache_.stat();
              return stats.hit_count;
          },
          sm::description(
            fmt::format(
              "The number of times {} was in the cache.", traits::item_label))),
      });
}

template class chunked_datalake_cache<
  pandaproxy::schema_registry::schema_id,
  pandaproxy::schema_registry::valid_schema>;
template class chunked_datalake_cache<schema_identifier, resolved_type>;

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
      .type = {},
      .parsable_buf = std::move(b),
    };
}

ss::future<checked<type_and_buf, type_resolver::errc>>
binary_type_resolver::resolve_buf_type(std::optional<iobuf> b) const {
    co_return type_and_buf::make_raw_binary(std::move(b));
}

ss::future<checked<shared_resolved_type_t, type_resolver::errc>>
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

ss::future<checked<shared_resolved_type_t, type_resolver::errc>>
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

    struct ident_and_buf {
        schema_identifier ident;
        iobuf parsable_buf;
    };

    auto ident_res
      = schema_res.value()->visit(
        ss::
          make_visitor(
            [&](const ppsr::protobuf_schema_definition&)
              -> checked<ident_and_buf, type_resolver::errc> {
                auto offsets_res = get_proto_offsets(buf_no_id);
                if (offsets_res.has_error()) {
                    return type_resolver::errc::bad_input;
                }
                auto offsets = std::move(offsets_res.value());
                return ident_and_buf{
            .ident = {.schema_id = schema_id,
                      .protobuf_offsets = std::move(offsets.protobuf_offsets)},
            .parsable_buf = std::move(offsets.shared_message_data),
          };
            },
            [&](const auto&) -> checked<ident_and_buf, type_resolver::errc> {
                return ident_and_buf{
                  .ident
                  = {.schema_id = schema_id, .protobuf_offsets = std::nullopt},
                  .parsable_buf = std::move(buf_no_id),
                };
            }));
    if (ident_res.has_error()) {
        co_return ident_res.error();
    }
    auto [ident, parsable_buf] = std::move(ident_res.value());

    auto resolve_res = get_resolved_type(
      std::move(ident), std::move(schema_res.value()), resolved_type_cache_);
    if (resolve_res.has_error()) {
        co_return resolve_res.error();
    }

    co_return type_and_buf{
      .type = std::move(resolve_res.value()),
      .parsable_buf = std::move(parsable_buf),
    };
}

ss::future<checked<shared_resolved_type_t, type_resolver::errc>>
record_schema_resolver::resolve_identifier(schema_identifier ident) const {
    auto schema_res = co_await get_schema(&sr_, cache_, ident.schema_id);
    if (schema_res.has_error()) {
        co_return schema_res.error();
    }

    co_return get_resolved_type(
      std::move(ident), std::move(schema_res.value()), resolved_type_cache_);
}

latest_subject_schema_resolver::latest_subject_schema_resolver(
  schema::registry& sr,
  ppsr::subject subject,
  std::optional<ss::sstring> protobuf_message_name,
  config::binding<std::chrono::milliseconds> cache_duration,
  std::optional<std::reference_wrapper<schema_cache>> sc,
  std::optional<std::reference_wrapper<resolved_type_cache>> rc)
  : sr_(&sr)
  , subject_(std::move(subject))
  , protobuf_message_name_(std::move(protobuf_message_name))
  , cache_ttl_(std::move(cache_duration))
  , cache_(sc)
  , resolved_type_cache_(rc) {}

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
    const auto now = ss::lowres_clock::now();
    const auto cache_ttl
      = std::chrono::duration_cast<ss::lowres_clock::duration>(cache_ttl_());

    if (schema_lookup_cache_.age(now) < cache_ttl) {
        if (schema_lookup_cache_.entry().has_value()) {
            co_return type_and_buf{
              .type = schema_lookup_cache_.entry().value(),
              .parsable_buf = std::move(b),
            };
        } else {
            if (datalake_log.is_enabled(ss::log_level::trace)) {
                vlog(
                  datalake_log.trace,
                  "Using negative schema lookup cache for subject {}. "
                  "Refreshing "
                  "in {}s",
                  subject_,
                  std::chrono::duration_cast<std::chrono::seconds>(
                    schema_lookup_cache_.time_until_expiry(now, cache_ttl))
                    .count());
            }
            co_return schema_lookup_cache_.entry().error();
        }
    }

    auto last_sync_time_fut
      = co_await ss::coroutine::as_future<ss::lowres_clock::time_point>(
        sr_->sync(cache_ttl));
    if (last_sync_time_fut.failed()) {
        auto ex = last_sync_time_fut.get_exception();
        vlog(datalake_log.warn, "Error syncing schema registry: {}", ex);
        co_return type_resolver::errc::registry_error;
    }
    const auto last_sync_time = last_sync_time_fut.get();

    auto latest_schema_fut
      = co_await ss::coroutine::as_future<ppsr::stored_schema>(
        sr_->get_subject_schema(
          {ppsr::default_context, subject_}, /*subject_version=*/std::nullopt));
    if (latest_schema_fut.failed()) {
        auto ex = latest_schema_fut.get_exception();
        vlog(
          datalake_log.warn,
          "Error getting subject schema ({}) from registry: {}.",
          subject_,
          ex);
        schema_lookup_cache_ = schema_lookup_cache(
          type_resolver::errc::registry_error, last_sync_time);
        co_return type_resolver::errc::registry_error;
    }
    auto latest_schema = std::move(latest_schema_fut.get());
    auto schema_res = co_await get_schema(sr_, cache_, latest_schema.id);
    if (schema_res.has_error()) {
        schema_lookup_cache_ = schema_lookup_cache(
          schema_res.error(), last_sync_time);
        co_return schema_res.error();
    }

    auto schema_id_res = schema_res.value()->visit(
      ss::make_visitor(
        [this, &latest_schema](const ppsr::protobuf_schema_definition& pb_def)
          -> checked<schema_identifier, type_resolver::errc> {
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
            return schema_identifier{
              .schema_id = latest_schema.id,
              .protobuf_offsets = std::move(offsets),
            };
        },
        [&latest_schema](
          const auto&) -> checked<schema_identifier, type_resolver::errc> {
            return schema_identifier{
              .schema_id = latest_schema.id,
              .protobuf_offsets = std::nullopt,
            };
        }));
    if (schema_id_res.has_error()) {
        schema_lookup_cache_ = schema_lookup_cache(
          schema_id_res.error(), last_sync_time);
        co_return schema_id_res.error();
    }

    auto resolve_res = get_resolved_type(
      std::move(schema_id_res.value()),
      std::move(schema_res.value()),
      resolved_type_cache_);
    if (resolve_res.has_error()) {
        schema_lookup_cache_ = schema_lookup_cache(
          resolve_res.error(), last_sync_time);
        co_return resolve_res.error();
    }

    auto resolved = std::move(resolve_res.value());

    vlog(
      datalake_log.trace,
      "Updated latest schema cache for subject {} and schema ID {}",
      subject_,
      latest_schema.id);
    schema_lookup_cache_ = schema_lookup_cache(resolved, last_sync_time);

    co_return type_and_buf{
      .type = std::move(resolved),
      .parsable_buf = std::move(b),
    };
}

ss::future<checked<shared_resolved_type_t, type_resolver::errc>>
latest_subject_schema_resolver::resolve_identifier(
  schema_identifier ident) const {
    auto schema_res = co_await get_schema(sr_, cache_, ident.schema_id);
    if (schema_res.has_error()) {
        co_return schema_res.error();
    }

    co_return get_resolved_type(
      std::move(ident), std::move(schema_res.value()), resolved_type_cache_);
}

} // namespace datalake

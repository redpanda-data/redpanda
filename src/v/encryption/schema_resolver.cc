/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "encryption/schema_resolver.h"

#include "base/vlog.h"
#include "encryption/schema_annotation_parser.h"

#include <seastar/core/coroutine.hh>
#include <seastar/coroutine/as_future.hh>
#include <seastar/util/log.hh>

#include <avro/Compiler.hh>

static ss::logger enclog("encryption");

namespace encryption {

schema_resolver::schema_resolver(
  schema_fetcher fetcher, ss::sstring default_kms_key_id)
  : _fetcher(std::make_unique<schema_fetcher>(std::move(fetcher)))
  , _default_kms_key_id(std::move(default_kms_key_id)) {}

void schema_resolver::register_rules(
  model::topic topic, topic_encryption_config config) {
    // Invalidate any cached result for this topic when rules change.
    _cache.erase(topic);
    _configs.insert_or_assign(std::move(topic), std::move(config));
}

ss::future<std::optional<encryption_schema>>
schema_resolver::resolve(model::topic topic) const {
    // Check the resolved cache first.
    if (auto it = _cache.find(topic); it != _cache.end()) {
        auto& cached = it->second;
        co_return encryption_schema{
          .format = cached.format,
          .handle = cached.handle,
          .tagged_fields = cached.tagged_fields,
        };
    }

    // Look up pre-configured (manual) rules first.
    auto cfg_it = _configs.find(topic);
    if (cfg_it != _configs.end()) {
        auto result = build_schema(cfg_it->second);
        if (!result.has_value()) {
            co_return std::nullopt;
        }

        // Cache the resolved schema. const_cast is safe here because we are
        // populating a lazy cache that does not change observable state.
        auto& mutable_cache
          = const_cast<chunked_hash_map<model::topic, encryption_schema>&>(
            _cache);
        mutable_cache.emplace(
          topic,
          encryption_schema{
            .format = result->format,
            .handle = result->handle,
            .tagged_fields = result->tagged_fields,
          });
        co_return std::move(result);
    }

    // No manual rules; try the registry path if available.
    if (_fetcher) {
        // Check negative cache for topics known to have no annotations.
        if (_no_encryption_cache.contains(topic)) {
            co_return std::nullopt;
        }
        co_return co_await resolve_from_registry(topic);
    }

    co_return std::nullopt;
}

bool schema_resolver::has_encryption_rules_cached(
  const model::topic& topic) const {
    return _cache.contains(topic) || _configs.contains(topic);
}

std::optional<encryption_schema>
schema_resolver::build_schema(const topic_encryption_config& config) {
    if (config.rules.empty() || config.field_tags.empty()) {
        return std::nullopt;
    }

    std::vector<tagged_field> tagged_fields;
    tagged_fields.reserve(config.field_tags.size());

    for (const auto& field : config.field_tags) {
        // Find the first rule whose tag matches this field's tag.
        for (const auto& rule : config.rules) {
            if (rule.tag == field.tag) {
                tagged_fields.push_back(
                  tagged_field{
                    .path = field.path,
                    .tag = field.tag,
                    .kek_name = rule.kek_name,
                  });
                break;
            }
        }
    }

    if (tagged_fields.empty()) {
        return std::nullopt;
    }

    return encryption_schema{
      .format = config.format,
      .handle = config.handle,
      .tagged_fields = std::move(tagged_fields),
    };
}

ss::future<std::optional<encryption_schema>>
schema_resolver::resolve_from_registry(const model::topic& topic) const {
    // Derive the subject name: "{topic}-value"
    auto subject_name = ss::sstring(topic()) + "-value";

    // Fetch the latest schema for the subject via the fetcher callback.
    vlog(enclog.trace, "Fetching schema for subject '{}'", subject_name);
    auto fetch_fut = co_await ss::coroutine::as_future(
      (*_fetcher)(subject_name));
    if (fetch_fut.failed()) {
        vlog(
          enclog.trace,
          "Schema fetch failed for subject '{}': {}",
          subject_name,
          fetch_fut.get_exception());
        fetch_fut.ignore_ready_future();
        co_return std::nullopt;
    }
    auto fetched = std::move(fetch_fut.get());
    if (!fetched.has_value()) {
        vlog(enclog.trace, "No schema found for subject '{}'", subject_name);
        co_return std::nullopt;
    }

    auto& schema_text = fetched->schema_text;
    vlog(
      enclog.trace,
      "Got schema for subject '{}', type={}, text length={}",
      subject_name,
      static_cast<int>(fetched->type),
      schema_text.size());

    // Parse encryption annotations based on schema type.
    std::vector<field_encryption_annotation> annotations;
    schema_format format;
    schema_handle handle;

    switch (fetched->type) {
    case fetched_schema_type::avro: {
        annotations = parse_avro_encryption_annotations(schema_text);
        format = schema_format::avro;
        // Parse the Avro schema to get a ValidSchema handle.
        try {
            auto valid = std::make_shared<::avro::ValidSchema>();
            *valid = ::avro::compileJsonSchemaFromString(schema_text);
            handle = std::move(valid);
        } catch (...) {
            handle = std::monostate{};
        }
        break;
    }
    case fetched_schema_type::json: {
        annotations = parse_json_schema_encryption_annotations(schema_text);
        format = schema_format::json;
        handle = std::monostate{};
        break;
    }
    case fetched_schema_type::protobuf:
        // Protobuf annotation parsing is not yet supported.
        co_return std::nullopt;
    }

    vlog(
      enclog.trace,
      "Parsed {} encryption annotations for subject '{}'",
      annotations.size(),
      subject_name);

    // No annotations means no encryption for this topic.
    if (annotations.empty()) {
        auto& mutable_no_enc_cache
          = const_cast<chunked_hash_map<model::topic, bool>&>(
            _no_encryption_cache);
        mutable_no_enc_cache.emplace(topic, true);
        co_return std::nullopt;
    }

    // Build tagged_fields from annotations.
    std::vector<tagged_field> tagged_fields;
    tagged_fields.reserve(annotations.size());
    for (auto& ann : annotations) {
        tagged_fields.push_back(
          tagged_field{
            .path = std::move(ann.path),
            .tag = "ENCRYPT",
            .kek_name = std::move(ann.kek_name),
          });
    }

    auto result = encryption_schema{
      .format = format,
      .handle = std::move(handle),
      .tagged_fields = std::move(tagged_fields),
    };

    // Cache the resolved schema.
    auto& mutable_cache
      = const_cast<chunked_hash_map<model::topic, encryption_schema>&>(_cache);
    mutable_cache.emplace(
      topic,
      encryption_schema{
        .format = result.format,
        .handle = result.handle,
        .tagged_fields = result.tagged_fields,
      });
    co_return std::move(result);
}

} // namespace encryption

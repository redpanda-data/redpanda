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

#pragma once

#include "encryption/field_transformer.h"
#include "model/fundamental.h"

#include <seastar/core/future.hh>
#include <seastar/core/sstring.hh>

#include <functional>
#include <optional>
#include <vector>

namespace encryption {

/// An encryption rule describes which tag to match and which KEK to use for
/// encryption. Rules are evaluated in order; the first matching rule for a
/// given tag wins.
struct encryption_rule {
    ss::sstring tag;
    ss::sstring kek_name;
};

/// A pre-configured description of which fields carry which tags for a topic's
/// value schema. This is the input that drives resolution: for each field,
/// the resolver matches its tag against the ordered rule list to determine
/// the KEK.
struct field_tag_mapping {
    std::vector<ss::sstring> path;
    ss::sstring tag;
};

/// Configuration for a topic's encryption: the ordered rule list and the
/// field-to-tag mappings extracted from the schema.
struct topic_encryption_config {
    schema_format format{schema_format::avro};
    schema_handle handle;
    std::vector<encryption_rule> rules;
    std::vector<field_tag_mapping> field_tags;
};

/// Schema type returned by the schema fetcher, mirroring the registry types
/// without depending on them.
enum class fetched_schema_type { avro, json, protobuf };

/// Result of fetching a schema from the registry via the schema_fetcher.
struct fetched_schema {
    ss::sstring schema_text;
    fetched_schema_type type;
};

/// Callback type for fetching the latest schema for a subject from the schema
/// registry. Decouples schema_resolver from the registry's dependency tree.
/// Returns nullopt if the subject does not exist or the registry is
/// unavailable.
using schema_fetcher = ss::noncopyable_function<
  ss::future<std::optional<fetched_schema>>(const ss::sstring& subject)>;

/// Resolves encryption schemas for topics.
///
/// In production, reads schemas from the schema registry and parses
/// encryption annotations to derive field encryption rules. For testing,
/// the default constructor and register_rules() API allow manual rule
/// registration without a registry.
class schema_resolver {
public:
    schema_resolver() = default;

    /// Production constructor that accepts a schema fetcher callback and a
    /// default KMS key ID from the cluster configuration.
    explicit schema_resolver(
      schema_fetcher fetcher, ss::sstring default_kms_key_id);

    /// Register encryption configuration for a topic. This is the test-friendly
    /// API; production code derives rules from schema annotations.
    void register_rules(model::topic topic, topic_encryption_config config);

    /// Resolve encryption schema for a topic. Returns nullopt if no encryption
    /// rules exist for the topic.
    ///
    /// When manual rules are registered (via register_rules()), those take
    /// precedence. Otherwise, if a schema fetcher is configured, the resolver
    /// fetches the latest schema for the "{topic}-value" subject, parses
    /// encryption annotations, and builds the encryption_schema.
    ss::future<std::optional<encryption_schema>>
    resolve(model::topic topic) const;

    /// Check if any encryption rules exist for a topic (synchronous cache
    /// check). Checks both manual rules and registry-resolved cache.
    bool has_encryption_rules_cached(const model::topic& topic) const;

private:
    /// Fetcher callback for production schema lookups from the registry.
    std::unique_ptr<schema_fetcher> _fetcher;

    /// Default KMS key ID from cluster configuration.
    ss::sstring _default_kms_key_id;

    /// Resolved cache: topic -> encryption_schema. Populated on first
    /// resolve() call for each topic.
    chunked_hash_map<model::topic, encryption_schema> _cache;

    /// Topics that resolved to no encryption annotations from the registry.
    chunked_hash_map<model::topic, bool> _no_encryption_cache;

    /// Pre-configured rules per topic.
    chunked_hash_map<model::topic, topic_encryption_config> _configs;

    /// Build an encryption_schema from a topic_encryption_config by matching
    /// field tags against the ordered rule list.
    static std::optional<encryption_schema>
    build_schema(const topic_encryption_config& config);

    /// Fetch schema via the fetcher callback and build encryption_schema from
    /// parsed annotations.
    ss::future<std::optional<encryption_schema>>
    resolve_from_registry(const model::topic& topic) const;
};

} // namespace encryption

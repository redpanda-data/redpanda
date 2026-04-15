/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "pandaproxy/schema_registry/iceberg_compat.h"

#include "container/chunked_vector.h"
#include "iceberg/compatibility_types.h"
#include "iceberg/conversion/schema_avro.h"
#include "iceberg/simulate_evolution.h"
#include "pandaproxy/schema_registry/sharded_store.h"

#include <seastar/core/coroutine.hh>

#include <avro/Compiler.hh>
#include <avro/ValidSchema.hh>
#include <fmt/core.h>

namespace pandaproxy::schema_registry {

namespace {

bool has_iceberg_flag(const schema_definition& def) {
    const auto& meta = def.meta();
    if (!meta.has_value() || !meta->properties.has_value()) {
        return false;
    }
    auto it = meta->properties->find(ss::sstring{iceberg_compat_metadata_key});
    return it != meta->properties->end() && it->second == "true";
}

using iceberg_convert_result = checked<iceberg::struct_type, ss::sstring>;

iceberg_convert_result schema_to_iceberg(const schema_definition& def) {
    if (def.type() != schema_type::avro) {
        return ss::sstring{
          "iceberg compatibility check only supports Avro schemas"};
    }
    try {
        auto raw_str = def.shared_raw()().linearize_to_string();
        auto valid = avro::compileJsonSchemaFromString(
          std::string{raw_str.data(), raw_str.size()});
        auto result = iceberg::type_to_iceberg(valid.root());
        if (result.has_error()) {
            return ss::sstring{fmt::format(
              "failed to convert schema to iceberg type: {}",
              result.error().what())};
        }
        return std::move(result.value());
    } catch (const std::exception& e) {
        return ss::sstring{
          fmt::format("failed to parse avro schema: {}", e.what())};
    }
}

} // namespace

ss::future<std::optional<ss::sstring>> check_iceberg_compatibility(
  sharded_store& store,
  const context_subject& sub,
  const schema_definition& candidate_def) {
    if (!has_iceberg_flag(candidate_def)) {
        co_return std::nullopt;
    }

    auto versions = co_await store.get_subject_versions(
      sub, include_deleted::yes);
    if (versions.empty()) {
        co_return std::nullopt;
    }

    // TODO: this replays the full version history on every registration.
    // For subjects with many versions, cache the accumulated iceberg
    // struct_type to avoid O(N) store lookups and conversions.
    chunked_vector<iceberg::struct_type> sequence;
    sequence.reserve(versions.size() + 1);

    for (const auto& entry : versions) {
        auto def = co_await store.get_schema_definition({sub.ctx, entry.id});
        auto convert_res = schema_to_iceberg(def);
        if (convert_res.has_error()) {
            co_return fmt::format(
              "iceberg compatibility check failed for version {}: {}",
              entry.version(),
              convert_res.error());
        }
        sequence.push_back(std::move(convert_res.value()));
    }

    auto candidate_res = schema_to_iceberg(candidate_def);
    if (candidate_res.has_error()) {
        co_return fmt::format(
          "iceberg compatibility check failed for candidate: {}",
          candidate_res.error());
    }
    sequence.push_back(std::move(candidate_res.value()));

    auto sim_result = iceberg::simulate_evolution(std::move(sequence));
    if (sim_result.has_error()) {
        const auto& failure = sim_result.error();
        // Map the internal step index to user-visible context.
        // The sequence is [v1, ..., vN, candidate]. Step i means
        // "evolving the schema accumulated from elements 0..i-1 with
        // element i failed." The previous element (i-1) is the last
        // version successfully incorporated.
        ss::sstring context;
        if (failure.step == 0) {
            context = "initial schema is invalid";
        } else if (failure.step <= versions.size()) {
            auto prev_ver = versions[failure.step - 1].version();
            context = fmt::format(
              "incompatible with schema accumulated through version {}",
              prev_ver);
        } else {
            context = "incompatible with accumulated schema";
        }
        co_return fmt::format("{}: {}", context, failure.errc);
    }

    co_return std::nullopt;
}

} // namespace pandaproxy::schema_registry

/*
 * Copyright 2021 Vectorized, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "pandaproxy/schema_registry/types.h"

#include <seastar/core/sharded.hh>

namespace pandaproxy::schema_registry {

class store;

///\brief Dispatch requests to shards based on a a hash of the
/// subject or schema_id
class sharded_store {
public:
    ss::future<> start(ss::smp_service_group sg);
    ss::future<> stop();

    ///\brief Make the canonical form of the schema
    ss::future<canonical_schema> make_canonical_schema(unparsed_schema schema);

    ///\brief Check the schema parses with the native format
    ss::future<void> validate_schema(const canonical_schema& schema);

    ///\brief Construct a schema in the native format
    ss::future<valid_schema> make_valid_schema(const canonical_schema& schema);

    struct insert_result {
        schema_version version;
        schema_id id;
        bool inserted;
    };

    ss::future<insert_result> project_ids(const canonical_schema& schema);

    ss::future<bool> upsert(
      seq_marker marker,
      canonical_schema schema,
      schema_id id,
      schema_version version,
      is_deleted deleted);

    ss::future<subject_schema> has_schema(const canonical_schema& schema);

    ///\brief Return a schema definition by id.
    ss::future<canonical_schema_definition>
    get_schema_definition(const schema_id& id);

    ///\brief Return a list of subject-versions for the shema id.
    ss::future<std::vector<subject_version>>
    get_schema_subject_versions(schema_id id);

    ///\brief Return a schema by subject and version (or latest).
    ss::future<subject_schema> get_subject_schema(
      const subject& sub,
      std::optional<schema_version> version,
      include_deleted inc_dec);

    ///\brief Return a list of subjects.
    ss::future<std::vector<subject>> get_subjects(include_deleted inc_del);

    ///\brief Return a list of versions and associated schema_id.
    ss::future<std::vector<schema_version>>
    get_versions(const subject& sub, include_deleted inc_del);

    ///\brief Delete a subject.
    ss::future<std::vector<schema_version>> delete_subject(
      seq_marker marker, const subject& sub, permanent_delete permanent);

    ss::future<is_deleted> is_subject_deleted(const subject& sub);

    ss::future<is_deleted> is_subject_version_deleted(
      const subject& sub, const schema_version version);

    ///\brief Get sequence number history (errors out if not soft-deleted)
    ss::future<std::vector<seq_marker>>
    get_subject_written_at(const subject& sub);

    ///\brief Get sequence number history (errors out if not soft-deleted)
    ss::future<std::vector<seq_marker>>
    get_subject_version_written_at(const subject& sub, schema_version version);

    ///\brief Delete a subject version
    ss::future<bool>
    delete_subject_version(const subject& sub, schema_version version);

    ///\brief Get the global compatibility level.
    ss::future<compatibility_level> get_compatibility();

    ///\brief Get the compatibility level for a subject, or fallback to global.
    ss::future<compatibility_level>
    get_compatibility(const subject& sub, default_to_global fallback);

    ///\brief Set the global compatibility level.
    ss::future<bool> set_compatibility(compatibility_level compatibility);

    ///\brief Set the compatibility level for a subject.
    ss::future<bool> set_compatibility(
      seq_marker marker, const subject& sub, compatibility_level compatibility);

    ///\brief Clear the compatibility level for a subject.
    ss::future<bool> clear_compatibility(const subject& sub);

    ///\brief Check if the provided schema is compatible with the subject and
    /// version, according the the current compatibility.
    ///
    /// If the compatibility level is transitive, then all versions are checked,
    /// otherwise checks are against the version provided and newer.
    ss::future<bool>
    is_compatible(schema_version version, const canonical_schema& new_schema);

private:
    ss::future<bool>
    upsert_schema(schema_id id, canonical_schema_definition def);

    struct insert_subject_result {
        schema_version version;
        bool inserted;
    };
    ss::future<insert_subject_result> insert_subject(
      subject sub, canonical_schema::references refs, schema_id id);

    ss::future<bool> upsert_subject(
      seq_marker marker,
      subject sub,
      canonical_schema::references refs,
      schema_version version,
      schema_id id,
      is_deleted deleted);

    ss::future<> maybe_update_max_schema_id(schema_id id);

    ss::future<schema_id> project_schema_id();

    ss::smp_submit_to_options _smp_opts;
    ss::sharded<store> _store;

    ///\brief Access must occur only on shard 0.
    schema_id _next_schema_id{1};
};

} // namespace pandaproxy::schema_registry

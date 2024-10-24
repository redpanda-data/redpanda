/*
 * Copyright 2021 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "container/fragmented_vector.h"
#include "pandaproxy/schema_registry/types.h"

#include <seastar/core/sharded.hh>

namespace pandaproxy::schema_registry {

class store;

///\brief Dispatch requests to shards based on a a hash of the
/// subject or schema_id
class sharded_store {
public:
    ss::future<> start(is_mutable mut, ss::smp_service_group sg);
    ss::future<> stop();

    ///\brief Make the canonical form of the schema
    ss::future<canonical_schema> make_canonical_schema(
      unparsed_schema schema, normalize norm = normalize::no);

    ///\brief Check the schema parses with the native format
    ss::future<void> validate_schema(canonical_schema schema);

    ///\brief Construct a schema in the native format
    ss::future<valid_schema> make_valid_schema(canonical_schema schema);

    struct has_schema_result {
        std::optional<schema_id> id;
        std::optional<schema_version> version;
    };
    ss::future<has_schema_result> get_schema_version(subject_schema schema);

    struct insert_result {
        schema_version version;
        schema_id id;
        bool inserted;
    };
    ss::future<insert_result> project_ids(subject_schema schema);

    ss::future<bool> upsert(
      seq_marker marker,
      canonical_schema schema,
      schema_id id,
      schema_version version,
      is_deleted deleted);

    ss::future<bool> upsert(
      seq_marker marker,
      unparsed_schema schema,
      schema_id id,
      schema_version version,
      is_deleted deleted);

    ss::future<bool> has_schema(schema_id id);
    ss::future<subject_schema> has_schema(
      canonical_schema schema, include_deleted inc_del = include_deleted::no);

    ///\brief Return a schema definition by id.
    ss::future<canonical_schema_definition> get_schema_definition(schema_id id);

    ///\brief Return a list of subject-versions for the shema id.
    ss::future<chunked_vector<subject_version>>
    get_schema_subject_versions(schema_id id);

    ///\brief Return a list of subjects for the schema id.
    ss::future<chunked_vector<subject>>
    get_schema_subjects(schema_id id, include_deleted inc_del);

    ///\brief Return a schema by subject and version (or latest).
    ss::future<subject_schema> get_subject_schema(
      subject sub,
      std::optional<schema_version> version,
      include_deleted inc_dec);

    ///\brief Return a list of subjects.
    ss::future<chunked_vector<subject>> get_subjects(
      include_deleted inc_del,
      std::optional<ss::sstring> subject_prefix = std::nullopt);

    ///\brief Return whether there are any subjects.
    ss::future<bool> has_subjects(include_deleted inc_del);

    ///\brief Return a list of versions and associated schema_id.
    ss::future<std::vector<schema_version>>
    get_versions(subject sub, include_deleted inc_del);

    ///\brief Return whether there are any references to a subject version.
    ss::future<bool> is_referenced(subject sub, schema_version ver);

    ///\brief Return the schema_ids that reference a subject version.
    ss::future<std::vector<schema_id>>
    referenced_by(subject sub, std::optional<schema_version> ver);

    ///\brief Delete a subject.
    ss::future<std::vector<schema_version>>
    delete_subject(seq_marker marker, subject sub, permanent_delete permanent);

    ss::future<is_deleted> is_subject_deleted(subject sub);

    ss::future<is_deleted>
    is_subject_version_deleted(subject sub, schema_version version);

    ///\brief Get sequence number history (errors out if not soft-deleted)
    ss::future<std::vector<seq_marker>> get_subject_written_at(subject sub);

    ///\brief Get sequence number history of subject config. Subject need
    /// not be soft-deleted first
    ss::future<std::vector<seq_marker>>
    get_subject_config_written_at(subject sub);

    ///\brief Get sequence number history of subject mode. Subject need
    /// not be soft-deleted first
    ss::future<std::vector<seq_marker>>
    get_subject_mode_written_at(subject sub);

    ///\brief Get sequence number history (errors out if not soft-deleted)
    ss::future<std::vector<seq_marker>>
    get_subject_version_written_at(subject sub, schema_version version);

    ///\brief Delete a subject version
    /// \param force Override checks for soft-delete first.
    ss::future<bool> delete_subject_version(
      subject sub, schema_version version, force f = force::no);

    ///\brief Get the global mode.
    ss::future<mode> get_mode();

    ///\brief Get the mode for a subject, or fallback to global.
    ss::future<mode> get_mode(subject sub, default_to_global fallback);

    ///\brief Set the global mode.
    /// \param force Override checks, always apply action
    ss::future<bool> set_mode(mode m, force f);

    ///\brief Set the mode for a subject.
    /// \param force Override checks, always apply action
    ss::future<bool> set_mode(seq_marker marker, subject sub, mode m, force f);

    ///\brief Clear the mode for a subject.
    /// \param force Override checks, always apply action
    ss::future<bool> clear_mode(seq_marker marker, subject sub, force f);

    ///\brief Get the global compatibility level.
    ss::future<compatibility_level> get_compatibility();

    ///\brief Get the compatibility level for a subject, or fallback to global.
    ss::future<compatibility_level>
    get_compatibility(subject sub, default_to_global fallback);

    ///\brief Set the global compatibility level.
    ss::future<bool> set_compatibility(compatibility_level compatibility);

    ///\brief Set the compatibility level for a subject.
    ss::future<bool> set_compatibility(
      seq_marker marker, subject sub, compatibility_level compatibility);

    ///\brief Clear the compatibility level for a subject.
    ss::future<bool> clear_compatibility(seq_marker marker, subject sub);

    ///\brief Check if the provided schema is compatible with the subject and
    /// version, according the the current compatibility.
    ///
    /// If the compatibility level is transitive, then all versions are checked,
    /// otherwise checks are against the version provided and newer.
    ss::future<bool>
    is_compatible(schema_version version, canonical_schema new_schema);

    ///\brief Check if the provided schema is compatible with the subject and
    /// version, according the the current compatibility, with the result
    /// optionally accompanied by a vector of detailed error messages.
    ///
    /// If the compatibility level is transitive, then all versions are checked,
    /// otherwise checks are against the version provided and newer.
    ss::future<compatibility_result> is_compatible(
      schema_version version, canonical_schema new_schema, verbose is_verbose);

    ss::future<bool> has_version(const subject&, schema_id, include_deleted);

    //// \brief Throw if the store is not mutable
    void check_mode_mutability(force f) const;

private:
    ss::future<compatibility_result> do_is_compatible(
      schema_version version, canonical_schema new_schema, verbose is_verbose);

    ss::future<bool>
    upsert_schema(schema_id id, canonical_schema_definition def);
    ss::future<> delete_schema(schema_id id);

    struct insert_subject_result {
        schema_version version;
        bool inserted;
    };
    ss::future<insert_subject_result> insert_subject(subject sub, schema_id id);

    ss::future<bool> upsert_subject(
      seq_marker marker,
      subject sub,
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

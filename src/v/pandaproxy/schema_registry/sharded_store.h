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

#include "container/chunked_vector.h"
#include "pandaproxy/schema_registry/schema_getter.h"
#include "pandaproxy/schema_registry/types.h"

#include <seastar/core/sharded.hh>

namespace pandaproxy::schema_registry {

class store;

///\brief Dispatch requests to shards based on a a hash of the
/// subject or schema_id
class sharded_store final : public schema_getter {
public:
    explicit sharded_store() = default;
    ~sharded_store() override = default;
    ss::future<> start(is_mutable mut, ss::smp_service_group sg);
    ss::future<> stop();

    ///\brief Make the canonical form of the schema
    ss::future<subject_schema> make_canonical_schema(
      subject_schema schema,
      normalize norm = normalize::no,
      bool consider_always_normalize_config = true);

    ///\brief Check the schema parses with the native format
    ss::future<void> validate_schema(subject_schema schema);

    ///\brief Reformat schema to the output format
    ss::future<schema_definition>
    format_schema(schema_definition schema, output_format format);

    ///\brief Construct a schema in the native format
    ss::future<valid_schema> make_valid_schema(subject_schema schema);

    struct insert_result {
        schema_version version;
        schema_id id;
        bool inserted;
    };
    ss::future<insert_result> project_ids(stored_schema schema);

    ss::future<bool> upsert(
      seq_marker marker,
      subject_schema schema,
      schema_id id,
      schema_version version,
      is_deleted deleted);

    // This function will try to compile all marked schemas.
    // It should be called every time new schemas are loaded from
    // the topic into the store.
    ss::future<> process_marked_schemas();

    ss::future<bool> has_schema(context_schema_id id);
    ss::future<stored_schema> has_schema(
      subject_schema schema, include_deleted inc_del = include_deleted::no);

    ///\brief Return a schema definition by id.
    ss::future<schema_definition>
    get_schema_definition(context_schema_id id) override;

    ///\brief Return a schema definition by id.
    ss::future<schema_definition>
    get_schema_definition(context_schema_id id, output_format format);

    ss::future<std::optional<schema_definition>>
    maybe_get_schema_definition(context_schema_id id) override;

    ///\brief Return a list of subject-versions for the shema id.
    ss::future<chunked_vector<subject_version>>
    get_schema_subject_versions(context_schema_id id);

    ///\brief Return a list of subject-versions for the subject. Returns an
    /// empty vector if the subject does not exist.
    ss::future<chunked_vector<subject_version_entry>>
    get_subject_versions(context_subject sub, include_deleted inc_del);

    ///\brief Return a list of subjects for the schema id.
    ss::future<chunked_vector<context_subject>>
    get_schema_subjects(context_schema_id id, include_deleted inc_del);

    ///\brief Return a schema by subject and version (or latest).
    ss::future<stored_schema> get_subject_schema(
      context_subject sub,
      std::optional<schema_version> version,
      include_deleted inc_dec) final;

    ///\brief Return the id of a schema by subject and version (or latest).
    ss::future<context_schema_id>
    get_id(context_subject sub, std::optional<schema_version> version);

    ///\brief Return a list of subjects.
    ss::future<chunked_vector<context_subject>> get_subjects(
      include_deleted inc_del,
      std::optional<ss::sstring> subject_prefix = std::nullopt);

    ///\brief Return whether there are any subjects.
    ss::future<bool> has_subjects(context ctx, include_deleted inc_del);

    ///\brief Return a list of versions and associated schema_id.
    ss::future<chunked_vector<schema_version>>
    get_versions(context_subject sub, include_deleted inc_del);
    ///\brief Return whether there are any references to a subject version.
    ss::future<bool>
    is_referenced(context_subject sub, std::optional<schema_version> ver);

    ///\brief Return the schema_ids that reference a subject version.
    ss::future<chunked_vector<context_schema_id>>
    referenced_by(context_subject sub, std::optional<schema_version> ver);
    ///\brief Delete a subject.
    ss::future<chunked_vector<schema_version>> delete_subject(
      seq_marker marker, context_subject sub, permanent_delete permanent);

    ss::future<is_deleted> is_subject_deleted(context_subject sub);

    ss::future<is_deleted>
    is_subject_version_deleted(context_subject sub, schema_version version);

    ///\brief Get sequence number history (errors out if not soft-deleted)
    ss::future<chunked_vector<seq_marker>>
    get_subject_written_at(context_subject sub);

    ///\brief Get sequence number history of subject config. Subject need
    /// not be soft-deleted first
    ss::future<chunked_vector<seq_marker>>
    get_subject_config_written_at(context_subject sub);

    ///\brief Get sequence number history of subject mode. Subject need
    /// not be soft-deleted first
    ss::future<chunked_vector<seq_marker>>
    get_subject_mode_written_at(context_subject sub);

    ///\brief Get sequence number history (errors out if not soft-deleted)
    ss::future<chunked_vector<seq_marker>>
    get_subject_version_written_at(context_subject sub, schema_version version);
    ///\brief Delete a subject version
    /// \param force Override checks for soft-delete first.
    ss::future<bool> delete_subject_version(
      context_subject sub, schema_version version, force f = force::no);
    ///\brief Get the mode of a context.
    ss::future<mode> get_mode(context ctx, default_to_global fallback);

    ///\brief Get the mode for a subject, or fallback to global.
    ss::future<mode> get_mode(context_subject sub, default_to_global fallback);

    ///\brief Set the mode of a context.
    /// \param force Override checks, always apply action
    ss::future<bool> set_mode(seq_marker marker, context ctx, mode m, force f);

    ///\brief Set the mode for a subject.
    /// \param force Override checks, always apply action
    ss::future<bool>
    set_mode(seq_marker marker, context_subject sub, mode m, force f);

    ///\brief Clear the mode of a context.
    /// \param force Override checks, always apply action
    ss::future<bool> clear_mode(context ctx, force f);
    ///\brief Clear the mode for a subject.
    /// \param force Override checks, always apply action
    ss::future<bool>
    clear_mode(seq_marker marker, context_subject sub, force f);

    /// \brief Return the seq_marker write history of a context, but only
    /// mode keys
    ss::future<chunked_vector<seq_marker>>
    get_context_mode_written_at(context ctx);

    ///\brief Get the compatibility level for a context, or fallback to global.
    ss::future<compatibility_level>
    get_compatibility(context ctx, default_to_global fallback);

    ///\brief Get the compatibility level for a subject, or fallback to global.
    ss::future<compatibility_level>
    get_compatibility(context_subject sub, default_to_global fallback);

    ///\brief Set the compatibility level of a context.
    ss::future<bool> set_compatibility(
      seq_marker marker, context ctx, compatibility_level compatibility);

    ///\brief Set the compatibility level for a subject.
    ss::future<bool> set_compatibility(
      seq_marker marker,
      context_subject sub,
      compatibility_level compatibility);

    ///\brief Clear the compatibility level of a context.
    ss::future<bool> clear_compatibility(context ctx);
    ///\brief Clear the compatibility level for a subject.
    ss::future<bool>
    clear_compatibility(seq_marker marker, context_subject sub);

    /// \brief Return the seq_marker write history of a context, but only
    /// config keys
    ss::future<chunked_vector<seq_marker>>
    get_context_config_written_at(context ctx);

    ///\brief Check if the provided schema is compatible with the subject and
    /// version, according the the current compatibility.
    ///
    /// If the compatibility level is transitive, then all versions are checked,
    /// otherwise checks are against the version provided and newer.
    ss::future<bool>
    is_compatible(schema_version version, subject_schema new_schema);

    ///\brief Check if the provided schema is compatible with the subject and
    /// version, according the the current compatibility, with the result
    /// optionally accompanied by a vector of detailed error messages.
    ///
    /// If the compatibility level is transitive, then all versions are checked,
    /// otherwise checks are against the version provided and newer.
    ss::future<compatibility_result> is_compatible(
      schema_version version, subject_schema new_schema, verbose is_verbose);

    ss::future<bool> has_version(context_subject, schema_id, include_deleted);

    //// \brief Throw if the store is not mutable
    void check_mode_mutability(force f) const;

    ///\brief Look up the id of a schema by definition
    ss::future<std::optional<schema_id>>
    get_schema_id(context ctx, schema_definition def) const;

    /// \brief List all materialized contexts (always includes default_context)
    ss::future<chunked_vector<context>> get_materialized_contexts() const;

    /// \brief Check if a context is materialized (has a CONTEXT record)
    ss::future<bool> is_context_materialized(context ctx) const;

    /// \brief Set the materialized state of a context
    ss::future<> set_context_materialized(context ctx, bool materialized);

private:
    ss::future<compatibility_result> do_is_compatible(
      schema_version version, subject_schema new_schema, verbose is_verbose);

    ss::future<bool> upsert_schema(
      context_schema_id id, schema_definition def, bool mark_schema);
    ss::future<> delete_schema(context_schema_id id);

    ss::future<bool> upsert_subject(
      seq_marker marker,
      context_subject sub,
      schema_version version,
      schema_id id,
      is_deleted deleted);

    ss::future<schema_id> project_schema_id(context ctx);

    ss::smp_submit_to_options _smp_opts;
    ss::sharded<store> _store;
};

} // namespace pandaproxy::schema_registry

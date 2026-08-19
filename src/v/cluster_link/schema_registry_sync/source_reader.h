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

#include "base/seastarx.h"
#include "cluster_link/model/types.h"
#include "container/chunked_vector.h"
#include "pandaproxy/schema_registry/types.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/future.hh>
#include <seastar/core/sstring.hh>

#include <cstdint>
#include <expected>
#include <optional>

namespace cluster_link::schema_registry_sync {

namespace ppsr = pandaproxy::schema_registry;

/// Classifies why a source Schema Registry read failed.
enum class source_error_kind : uint8_t {
    /// The operation failed but the source is still reachable; the sync can
    /// continue with the next item.
    operation_failed,
    /// The source Schema Registry is unreachable; the whole sync should back
    /// off.
    source_unavailable,
    /// The subject does not exist in the source (HTTP 404).
    subject_not_found,
    /// The numeric schema id does not resolve in the searched source context
    /// (HTTP 404 / error_code 40403). The id probe expects this on every tick,
    /// so it must not be counted as a sync error.
    schema_id_not_found,
    /// The source refuses or does not implement the requested endpoint, as
    /// opposed to failing one call. The caller should skip whatever needed
    /// it -- not treat the source as down or count an error -- but may ask
    /// again later. Produced only by `list_schema_id_subject_versions`, the
    /// one endpoint the sync can do without.
    endpoint_unavailable,
};

struct source_error {
    source_error_kind kind;
    ss::sstring message;
};

template<typename T>
using source_result = std::expected<T, source_error>;

/// A config read from the source: the compatibility-level override (nullopt
/// when the source has none) plus any unsupported config fields, for the
/// caller's unsupported-feature policy.
struct source_config_read {
    std::optional<ppsr::compatibility_level> compatibility;
    chunked_vector<ppsr::unsupported_feature> unsupported;
};

/// \brief Abstraction over a source Schema Registry, scoped to one link.
///
/// Reads are split into discovery (list subjects/versions) and fetch (read a
/// specific schema) so the sync can decide what to import before pulling
/// schema bodies. Production uses the HTTP-backed reader; tests inject a fake.
class source_reader {
public:
    source_reader() = default;
    source_reader(const source_reader&) = delete;
    source_reader& operator=(const source_reader&) = delete;
    source_reader(source_reader&&) = delete;
    source_reader& operator=(source_reader&&) = delete;
    virtual ~source_reader() = default;

    virtual ss::future<source_result<chunked_vector<ppsr::context>>>
    list_contexts(ss::abort_source&) = 0;

    virtual ss::future<source_result<chunked_vector<ppsr::context_subject>>>
    list_subjects(ppsr::context, ss::abort_source&) = 0;

    virtual ss::future<source_result<chunked_vector<ppsr::schema_version>>>
    list_subject_versions(
      ppsr::context_subject, ppsr::include_deleted, ss::abort_source&) = 0;

    /// Lists every (subject, version) pair in the given context backed by the
    /// given numeric schema id -- tail sync's discovery probe. The result is
    /// not narrowed by the link's scope (the caller applies `in_scope`), and
    /// the pairs are unordered. An id the source never allocated (or
    /// hard-deleted) yields `schema_id_not_found`; an allocated id whose
    /// every version is soft-deleted yields an empty list instead -- only
    /// existence tells the probe whether the id space is exhausted.
    virtual ss::future<source_result<chunked_vector<ppsr::subject_version>>>
    list_schema_id_subject_versions(
      ppsr::schema_id, ppsr::context, ss::abort_source&) = 0;

    /// Reads a specific subject version's schema. The reconcile engine's
    /// schema-body fetch path: called for every node it discovers and imports.
    /// Returns the schema projected into Redpanda's supported model plus any
    /// unsupported fields the source carried but Redpanda cannot store, for the
    /// caller to apply its unsupported-feature policy.
    virtual ss::future<source_result<ppsr::source_schema_read>>
    read_subject_version(
      ppsr::context_subject, ppsr::schema_version, ss::abort_source&) = 0;

    /// Reads the source's own (non-inherited) mode override for a subject or
    /// context: nullopt for no explicit override (subject_mode_not_found), a
    /// value to mirror, or an operation_failed error for a mode Redpanda cannot
    /// represent (e.g. FORWARD), which the caller counts rather than treating
    /// as nullopt.
    virtual ss::future<source_result<std::optional<ppsr::mode>>>
    read_mode(ppsr::context_subject, ss::abort_source&) = 0;

    /// As read_mode, for the compatibility-level override plus any unsupported
    /// config fields.
    virtual ss::future<source_result<source_config_read>>
    read_config(ppsr::context_subject, ss::abort_source&) = 0;

    /// Releases any resources the reader holds (e.g. an HTTP transport). Called
    /// once before the reader is destroyed; the default is a no-op for readers
    /// that hold nothing. After stop() no other method may be called.
    virtual ss::future<> stop() { return ss::make_ready_future<>(); }
};

/// \brief Creates one `source_reader` per link.
class source_reader_factory {
public:
    source_reader_factory() = default;
    source_reader_factory(const source_reader_factory&) = delete;
    source_reader_factory& operator=(const source_reader_factory&) = delete;
    source_reader_factory(source_reader_factory&&) = delete;
    source_reader_factory& operator=(source_reader_factory&&) = delete;
    virtual ~source_reader_factory() = default;

    /// \param api_cfg the link's Schema-Registry-API shadowing config, or
    ///        nullptr when the link is not in SR-API mode. The HTTP-backed
    ///        reader builds its transport (source URL, auth, TLS) from it;
    ///        readers that do not talk to a remote source ignore it.
    virtual std::unique_ptr<source_reader> create(
      const model::schema_registry_sync_config::shadow_schema_registry_api*
        api_cfg) = 0;
};

} // namespace cluster_link::schema_registry_sync

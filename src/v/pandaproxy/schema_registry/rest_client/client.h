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
#include "bytes/iobuf.h"
#include "container/chunked_vector.h"
#include "http/client.h"
#include "pandaproxy/schema_registry/rest_client/config.h"
#include "pandaproxy/schema_registry/rest_client/credentials.h"
#include "pandaproxy/schema_registry/rest_client/error.h"
#include "pandaproxy/schema_registry/rest_client/mode.h"
#include "pandaproxy/schema_registry/rest_client/parse.h"
#include "pandaproxy/schema_registry/rest_client/retry_policy.h"
#include "pandaproxy/schema_registry/types.h"
#include "utils/retry_chain_node.h"

#include <seastar/core/future.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/sstring.hh>

#include <expected>
#include <memory>
#include <optional>

namespace http {
class request_builder;
} // namespace http

namespace pandaproxy::schema_registry::rest_client {

/// A reusable client for the Schema Registry REST API, usable against both
/// Redpanda's own Schema Registry and third-party Confluent-compatible
/// registries. The HTTP transport is injected (http::abstract_client) so it can
/// be mocked in tests. A client is lightweight; create one per registry
/// endpoint. shutdown() must be called before destruction.
class client {
public:
    /// \param http_client the HTTP transport to issue requests on
    /// \param endpoint the registry's scheme+host (e.g. "https://sr:8081")
    /// \param credentials optional HTTP Basic auth credentials; absent means no
    ///        Authorization header is sent
    /// \param qualified policy for interpreting context-qualified subject
    ///        strings in responses (see context_subject::from_string); supplied
    ///        by the caller so parsing does not depend on this node's config
    /// \param retry_policy classifies failures as retriable/permanent; defaults
    ///        to default_retry_policy when null. The retry budget/backoff comes
    ///        from the per-call retry_chain_node, not this policy.
    client(
      std::unique_ptr<http::abstract_client> http_client,
      ss::sstring endpoint,
      std::optional<basic_auth_credentials> credentials = std::nullopt,
      qualified_subjects_enabled qualified = qualified_subjects_enabled::no,
      std::unique_ptr<retry_policy> retry_policy = nullptr);

    client(const client&) = delete;
    client& operator=(const client&) = delete;
    client(client&&) = delete;
    client& operator=(client&&) = delete;
    ~client() = default;

    /// GET /subjects — list subjects in the registry. With \p deleted set to
    /// yes, soft-deleted subjects are included in the listing.
    ///
    /// When \p ctx is set, only subjects in that context are returned. The
    /// context is both sent as a `subjectPrefix=":<ctx>:"` query parameter (so
    /// a server that supports it filters at the source — e.g. ":.dev:", or
    /// ":.:" for the default context) AND applied client-side by exact context
    /// match, so the result is correctly scoped even against a server that
    /// ignores the parameter (Redpanda's own server honors it). nullopt (the
    /// default) lists subjects across all contexts.
    ss::future<std::expected<chunked_vector<context_subject>, domain_error>>
    list_subjects(
      retry_chain_node& rtc,
      include_deleted deleted = include_deleted::no,
      std::optional<context> ctx = std::nullopt);

    /// GET /contexts — list the contexts that currently exist in the registry.
    /// The default context is always present, returned as the one-character
    /// context ".". Each element is the bare, dot-prefixed context name (e.g.
    /// ".dev"); to reuse one as a context-qualified subject prefix elsewhere,
    /// wrap it in colons (".dev" -> ":.dev:"). There is no operation-specific
    /// not-found: an empty registry still lists ["."].
    ///
    /// When \p context_prefix is set, only contexts whose (dot-prefixed) name
    /// begins with it are returned — e.g. ".prod" matches ".prod" and
    /// ".prod-eu", and "." matches every context. The prefix is both sent as
    /// the `contextPrefix` query parameter (so a server that supports it
    /// filters at the source) AND applied client-side, so the result is
    /// correctly filtered even against a server that ignores the parameter
    /// (Redpanda's own server does). nullopt (the default) applies no filter.
    ss::future<std::expected<chunked_vector<context>, domain_error>>
    list_contexts(
      retry_chain_node& rtc,
      std::optional<ss::sstring> context_prefix = std::nullopt);

    /// GET /mode — read the registry-wide (global / default-context) operating
    /// mode. This endpoint always resolves to a value: a registry that never
    /// had a global mode set reports READWRITE. There is no operation-specific
    /// not-found condition (only a transient 500 / error_code 50001 storage
    /// failure, handled by the retry policy).
    ///
    /// The mode is an open enum (see mode.h): the values the Schema Registry
    /// REST API defines are mapped to registry_mode, and any value a newer
    /// server reports is surfaced as registry_mode::unknown with the verbatim
    /// wire string preserved in mode_info::raw, rather than rejected. The
    /// `defaultToGlobal` query parameter is omitted: on the subject-less global
    /// endpoint it has no observable effect.
    ss::future<std::expected<mode_info, domain_error>>
    get_mode(retry_chain_node& rtc);

    /// GET /mode/{subject} — read the mode of a single subject (or a context,
    /// when \p subject names one). \p fallback selects which of two questions
    /// is asked:
    ///   - no (default): the subject's OWN explicitly-set mode. A subject with
    ///     no override yields subject_mode_not_found (HTTP 404 / error_code
    ///     40409; a 40401 from some server versions is treated the same). Use
    ///     this to read back an override or detect its absence.
    ///   - yes: the EFFECTIVE mode, resolving subject -> context -> global ->
    ///     built-in default. This always resolves, so subject_mode_not_found
    ///     cannot occur. Use this for the mode that actually governs the
    ///     subject.
    ///
    /// The result is the same open enum as get_mode (see mode.h). Targeting: a
    /// plain/qualified subject reads that subject; a context string (":.ctx:")
    /// reads that context's mode; the explicit default context (":.:") behaves
    /// like the global get_mode.
    ss::future<std::expected<mode_info, domain_error>> get_subject_mode(
      const context_subject& subject,
      retry_chain_node& rtc,
      default_to_global fallback = default_to_global::no);

    /// GET /config — read the registry-wide (global / default) configuration.
    /// Like get_mode this always resolves to a value: a registry with no global
    /// config set reports the built-in default compatibility level (BACKWARD),
    /// so there is no operation-specific not-found (only a transient 500 /
    /// error_code 50001 storage failure, handled by the retry policy).
    ///
    /// Only the compatibility level is modeled (see config.h): it is an open
    /// enum, so a value a newer server reports is surfaced as
    /// registry_compatibility_level::unknown with the verbatim string in
    /// config_info::raw. Any other top-level config fields present (validation
    /// flags, metadata, rule sets — which Redpanda's own server does not emit)
    /// are surfaced in config_info::unsupported rather than modeled. The
    /// `defaultToGlobal` query parameter is omitted: on the subject-less global
    /// endpoint it has no observable effect.
    ss::future<std::expected<config_info, domain_error>>
    get_config(retry_chain_node& rtc);

    /// GET /config/{subject} — read the configuration of a single subject (or a
    /// context, when \p subject names one). \p fallback selects which of two
    /// questions is asked:
    ///   - no (default): the subject's OWN config. A subject with no
    ///     override yields subject_config_not_found (HTTP 404 / error_code
    ///     40408; a 40401 from some server versions is treated the same). Use
    ///     this to read back an override or detect its absence.
    ///   - yes: the EFFECTIVE config, resolving subject -> context -> global ->
    ///     built-in default. This always resolves, so subject_config_not_found
    ///     cannot occur. Use this for the config that actually governs the
    ///     subject.
    ///
    /// The result is the same config_info as get_config (see config.h).
    /// Targeting: a plain/qualified subject reads that subject; a context
    /// string
    /// (":.ctx:") reads that context's config; the explicit default context
    /// (":.:") behaves like the global get_config.
    ss::future<std::expected<config_info, domain_error>> get_subject_config(
      const context_subject& subject,
      retry_chain_node& rtc,
      default_to_global fallback = default_to_global::no);

    /// GET /subjects/{subject}/versions — list the version numbers registered
    /// under \p subject. With \p deleted set to yes, soft-deleted versions are
    /// included. A missing subject yields subject_not_found.
    ss::future<std::expected<chunked_vector<schema_version>, domain_error>>
    list_subject_versions(
      const context_subject& subject,
      retry_chain_node& rtc,
      include_deleted deleted = include_deleted::no);

    /// GET /subjects/{subject}/versions/{version} — fetch one version of a
    /// subject's schema. With \p deleted set to yes, a soft-deleted version can
    /// be fetched. A missing subject yields subject_not_found; a missing
    /// version (of an existing subject) yields version_not_found.
    ///
    /// The result wraps the schema together with any unsupported response
    /// fields the client could not store (source_schema_read::unsupported); a
    /// caller that needs fidelity can inspect them and apply its own strictness
    /// policy.
    ss::future<std::expected<source_schema_read, domain_error>>
    get_schema_by_version(
      const context_subject& subject,
      schema_version version,
      retry_chain_node& rtc,
      include_deleted deleted = include_deleted::no);

    /// GET /schemas/ids/{id}/versions — list every (subject, version) pair
    /// backed by the schema with numeric \p id. One schema's content can be
    /// registered under many subjects; this enumerates all of them within one
    /// context. An id that does not resolve yields schema_id_not_found (HTTP
    /// 404 / error_code 40403 — the schema-not-found code, not the 40401/40402
    /// used elsewhere).
    ///
    /// Schema ids are namespaced PER CONTEXT, so \p subject selects the context
    /// to resolve \p id in; it is sent verbatim as the `subject` query
    /// parameter. It only LOCATES the schema — it never filters the output: the
    /// result always lists every subject in the resolved context that shares
    /// the content, regardless of \p subject. The four forms:
    ///   - std::nullopt (default): resolve \p id in the DEFAULT context.
    ///   - a bare context, e.g. context_subject{context{".ctx"}, subject{""}}
    ///     (wire ":.ctx:"): resolve \p id in ".ctx" whichever subject holds it.
    ///     The recommended way to target a named context.
    ///   - a context-qualified subject, e.g. {context{".ctx"}, subject{"s"}}
    ///     (wire ":.ctx:s"): resolves \p id in ".ctx" ONLY IF "s" carries it,
    ///     else schema_id_not_found even when \p id exists in ".ctx" under
    ///     other subjects. Rarely useful.
    ///   - a plain subject, e.g. context_subject::unqualified("s"): tries the
    ///     default context, then falls back to searching other contexts for a
    ///     subject of that name holding \p id. Surprising; avoid.
    /// Prefer std::nullopt or the bare-context form.
    ///
    /// Returned subjects are context-qualified for a non-default context. The
    /// pairs are UNORDERED; sort client-side if you need determinism.
    /// include_deleted::yes sends `deleted=true`: a source honoring it
    /// includes soft-deleted pairs and treats a fully soft-deleted id as a
    /// hit, while Redpanda ignores the parameter and returns only the live
    /// pairs. `offset` and `limit` stay unimplemented because the result is
    /// unordered and Redpanda answers unpaginated.
    ss::future<std::expected<chunked_vector<subject_version>, domain_error>>
    get_schema_id_subject_versions(
      schema_id id,
      retry_chain_node& rtc,
      std::optional<context_subject> subject = std::nullopt,
      include_deleted inc = include_deleted::no);

    /// Stops the transport and drains in-flight requests. Must be called before
    /// destroying the client.
    ss::future<> shutdown();

private:
    std::expected<ss::gate::holder, domain_error> maybe_gate();
    void maybe_add_basic_auth(http::request_builder& request);

    // Builds and issues the request against _endpoint, retrying according to
    // the retry policy and the supplied retry_chain_node, and returns the
    // collected response body. A permanent http_status_error is enriched with
    // the parsed error_code before being returned.
    ss::future<std::expected<iobuf, domain_error>> perform_request(
      retry_chain_node& parent_rtc,
      http::request_builder builder,
      std::optional<iobuf> payload = std::nullopt);

    ss::gate _gate;
    std::unique_ptr<http::abstract_client> _http_client;
    ss::sstring _endpoint;
    std::optional<basic_auth_credentials> _credentials;
    qualified_subjects_enabled _qualified;
    std::unique_ptr<retry_policy> _retry_policy;
};

} // namespace pandaproxy::schema_registry::rest_client

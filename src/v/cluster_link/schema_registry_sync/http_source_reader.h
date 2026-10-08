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

#include "cluster_link/schema_registry_sync/source_reader.h"
#include "net/tls.h"
#include "pandaproxy/schema_registry/rest_client/client.h"
#include "pandaproxy/schema_registry/rest_client/credentials.h"
#include "ssx/semaphore.h"
#include "utils/unresolved_address.h"

#include <memory>
#include <optional>

namespace cluster_link::schema_registry_sync {

namespace rc = ppsr::rest_client;

/// Connection parameters for an HTTP(S) source Schema Registry, resolved
/// synchronously from the link's API-mode config. The TLS certificate material
/// is converted to net types up front, but the
/// ss::tls::certificate_credentials are built lazily (the build is async, the
/// factory is not), so the parsed inputs are held here until first connect.
struct http_source_connection {
    net::unresolved_address address;
    /// Full source URL (scheme + host[:port]); the rest_client uses it as the
    /// endpoint for the Host header and request paths.
    ss::sstring endpoint;
    bool tls_enabled{false};
    std::optional<net::certificate> truststore;
    std::optional<net::key_store> client_key;
    bool provide_sni{true};
    std::optional<rc::basic_auth_credentials> auth;
    /// Cap on requests/sec to the source, resolved from the link's
    /// max_source_requests_per_second (always > 0: conversion rejects
    /// negatives and maps zero/unset to the default). Like the other
    /// connection settings, a link-config change applies when the task next
    /// (re)starts.
    size_t max_requests_per_sec{
      model::schema_registry_sync_config::shadow_schema_registry_api::
        default_max_source_requests_per_second};
};

/// Resolves a source Schema Registry URL to the host:port the transport should
/// connect to. When the URL omits a port (including a standard port ada
/// normalizes away, e.g. :443 on https), the scheme's default port is used;
/// an explicit non-default port is honored. Returns nullopt for a URL that
/// cannot be resolved to a host:port (no host, unparseable/zero port, or a
/// scheme with no default port), so the caller can report a config error.
std::optional<net::unresolved_address> parse_source_address(std::string_view);

/// \brief A `source_reader` that reads a remote (source) Schema Registry over
/// its HTTP REST API, via `pandaproxy::schema_registry::rest_client::client`.
///
/// Enumerates every source context (via GET /contexts); basic auth and TLS
/// are honored from the link config. The reconcile engine drives discovery
/// (list contexts/subjects/versions) and schema-body fetches through this
/// reader, concurrently over a small pool of connections sized from
/// schema_registry_sync_parallelism.
class http_source_reader final : public source_reader {
public:
    /// Production: the HTTP transports (and their TLS credentials) are built
    /// lazily from `conn` on the first request, since credential building is
    /// async but the factory that creates the reader is not.
    explicit http_source_reader(http_source_connection conn);
    /// Test seam: takes a ready rest_client, bypassing the lazy transport
    /// build.
    explicit http_source_reader(std::unique_ptr<rc::client> client);

    ss::future<source_result<chunked_vector<ppsr::context>>>
    list_contexts(ss::abort_source&) override;

    ss::future<source_result<chunked_vector<ppsr::context_subject>>>
    list_subjects(ppsr::context, ss::abort_source&) override;

    ss::future<source_result<chunked_vector<ppsr::schema_version>>>
    list_subject_versions(
      ppsr::context_subject, ppsr::include_deleted, ss::abort_source&) override;

    ss::future<source_result<chunked_vector<ppsr::subject_version>>>
    list_schema_id_subject_versions(
      ppsr::schema_id,
      ppsr::context,
      ppsr::include_deleted,
      ss::abort_source&) override;

    ss::future<source_result<ppsr::source_schema_read>> read_subject_version(
      ppsr::context_subject, ppsr::schema_version, ss::abort_source&) override;

    ss::future<source_result<std::optional<ppsr::mode>>>
    read_mode(ppsr::context_subject, ss::abort_source&) override;

    ss::future<source_result<source_config_read>>
    read_config(ppsr::context_subject, ss::abort_source&) override;

    ss::future<> stop() override;

private:
    /// Returns the rest_client, building it (and its TLS credentials) on first
    /// call. The build is serialized on `_build` so at most one runs; a build
    /// failure surfaces as source_unavailable so the link parks and retries.
    /// Returns a borrowed pointer owned by `_client`.
    ss::future<source_result<rc::client*>> ensure_client(ss::abort_source&);

    // Connection inputs for the lazy build; nullopt once a client is injected.
    std::optional<http_source_connection> _conn;
    std::unique_ptr<rc::client> _client;
    // Set by stop(): the reader is stopped while the reconcile engine's
    // fibers may still be unwinding, and a late call must fail rather than
    // lazily rebuild the client (which nothing would ever stop).
    bool _stopped{false};
    // Serializes the lazy client build only. Requests run concurrently: the
    // reconcile engine drives this reader from several fibers, and the
    // rest_client's pooled transport bounds them to one in-flight request per
    // pooled connection.
    ssx::semaphore _build{1, "cluster_link/sr_source/build"};
};

class http_source_reader_factory final : public source_reader_factory {
public:
    /// Builds a reader from the link's API-mode config (source URL, auth,
    /// TLS). A null config or an unparseable URL yields an unavailable
    /// reader, so the link parks rather than faulting.
    std::unique_ptr<source_reader> create(
      const model::schema_registry_sync_config::shadow_schema_registry_api*
        api_cfg) override;
};

} // namespace cluster_link::schema_registry_sync

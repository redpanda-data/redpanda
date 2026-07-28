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

#include "cluster_link/schema_registry_sync/http_source_reader.h"

#include "cluster_link/schema_registry_sync/unavailable_source_reader.h"
#include "cluster_link/utils.h"
#include "config/configuration.h"
#include "config/tls_config.h"
#include "http/client.h"
#include "net/tls.h"
#include "net/tls_certificate_probe.h"
#include "net/transport.h"
#include "pandaproxy/schema_registry/rest_client/error.h"
#include "pandaproxy/schema_registry/rest_client/pooled_client.h"
#include "pandaproxy/schema_registry/rest_client/rate_limited_client.h"
#include "utils/retry_chain_node.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/semaphore.hh>

#include <ada.h>
#include <charconv>
#include <chrono>
#include <system_error>
#include <variant>

using namespace std::chrono_literals;

namespace cluster_link::schema_registry_sync {

namespace {

// Per-call retry budget for source requests. The rest_client retries
// network/timeout/5xx within this window; an exhausted budget surfaces as
// retries_exhausted, which the reader maps to source_unavailable.
constexpr auto request_timeout = 30s;
constexpr auto request_backoff = 100ms;

// Upper bound on pooled connections to the source registry. The pool is sized
// to schema_registry_sync_parallelism (the reconciler's fan-out), capped here
// so a large parallelism setting does not hold as many sockets open against
// the source.
constexpr size_t max_source_connections = 8;

// Map a rest_client failure onto a source_error. A source that is unreachable
// or rejecting every request is source_unavailable and parks the link; a 404 is
// subject_not_found, or schema_id_not_found when it names an unresolved schema
// id; any other terminal status is a per-item operation_failed.
source_error to_source_error(rc::domain_error err) {
    using enum boost::beast::http::status;
    auto kind = ss::visit(
      err,
      [](const rc::http_call_error& call) {
          return ss::visit(
            call,
            [](const rc::http_status_error& s) {
                // Auth failures are link-wide and deterministic, so back off
                // rather than re-fail every subject in turn.
                return s.status == unauthorized || s.status == forbidden
                         ? source_error_kind::source_unavailable
                         : source_error_kind::operation_failed;
            },
            // A permanent exception with no HTTP response: source unreachable.
            [](const ss::sstring&) {
                return source_error_kind::source_unavailable;
            });
      },
      [](const rc::retries_exhausted&) {
          return source_error_kind::source_unavailable;
      },
      [](const rc::aborted_error&) {
          return source_error_kind::source_unavailable;
      },
      [](const rc::subject_not_found&) {
          return source_error_kind::subject_not_found;
      },
      [](const rc::schema_id_not_found&) {
          return source_error_kind::schema_id_not_found;
      },
      [](const auto&) { return source_error_kind::operation_failed; });
    return source_error{.kind = kind, .message = fmt::format("{}", err)};
}

// Narrow the open-enum source mode to Redpanda's mode. READONLY_OVERRIDE is
// preserved as read_only; FORWARD and unknown have no representation and yield
// nullopt, which the caller counts as a per-item error.
std::optional<ppsr::mode> narrow_mode(rc::registry_mode m) {
    switch (m) {
    case rc::registry_mode::read_write:
        return ppsr::mode::read_write;
    case rc::registry_mode::read_only:
    case rc::registry_mode::read_only_override:
        return ppsr::mode::read_only;
    case rc::registry_mode::import:
        return ppsr::mode::import;
    case rc::registry_mode::forward:
    case rc::registry_mode::unknown:
        return std::nullopt;
    }
    return std::nullopt;
}

// Narrow the open-enum compatibility level to Redpanda's closed enum; only
// unknown has no representation and yields nullopt.
std::optional<ppsr::compatibility_level>
narrow_compat(rc::registry_compatibility_level c) {
    using enum rc::registry_compatibility_level;
    switch (c) {
    case none:
        return ppsr::compatibility_level::none;
    case backward:
        return ppsr::compatibility_level::backward;
    case backward_transitive:
        return ppsr::compatibility_level::backward_transitive;
    case forward:
        return ppsr::compatibility_level::forward;
    case forward_transitive:
        return ppsr::compatibility_level::forward_transitive;
    case full:
        return ppsr::compatibility_level::full;
    case full_transitive:
        return ppsr::compatibility_level::full_transitive;
    case unknown:
        return std::nullopt;
    }
    return std::nullopt;
}

} // namespace

std::optional<net::unresolved_address>
parse_source_address(std::string_view url) {
    auto parsed = ada::parse<ada::url_aggregator>(url);
    if (!parsed || parsed->get_hostname().empty()) {
        return std::nullopt;
    }
    // Only host:port is carried into the transport; a path prefix, query, or
    // fragment would be silently dropped (requests always hit the SR API at the
    // root). Reject rather than connect to the wrong place. ada normalizes an
    // empty path to "/" for special schemes, so "/" means "no prefix".
    if (auto path = parsed->get_pathname(); path != "/" && !path.empty()) {
        return std::nullopt;
    }
    if (!parsed->get_search().empty() || !parsed->get_hash().empty()) {
        return std::nullopt;
    }
    // ada normalizes away a port equal to the scheme default (per the WHATWG
    // URL spec), so get_port() is empty for e.g. https://host:443. Fall back to
    // the scheme's default port (443 for https, 80 for http) rather than a
    // fixed one, so a source behind a standard port is reachable; an explicit
    // non-default port (e.g. :8081) is still honored.
    uint16_t port = parsed->scheme_default_port();
    if (auto port_sv = parsed->get_port(); !port_sv.empty()) {
        auto [ptr, ec] = std::from_chars(
          port_sv.data(), port_sv.data() + port_sv.size(), port);
        if (ec != std::errc{} || ptr != port_sv.data() + port_sv.size()) {
            return std::nullopt;
        }
    }
    // A non-special scheme has no default port; reject rather than connect to
    // 0.
    if (port == 0) {
        return std::nullopt;
    }
    return net::unresolved_address{ss::sstring{parsed->get_hostname()}, port};
}

http_source_reader::http_source_reader(http_source_connection conn)
  : _conn(std::move(conn)) {}

http_source_reader::http_source_reader(std::unique_ptr<rc::client> client)
  : _client(std::move(client)) {}

ss::future<source_result<rc::client*>>
http_source_reader::ensure_client(ss::abort_source& as) {
    // Checked before the fast path: stop() nulls _client, and a call arriving
    // after stop (fibers may still be unwinding when the reader is stopped)
    // must fail rather than rebuild a client nothing would ever stop.
    if (_stopped) {
        co_return std::unexpected(
          source_error{
            .kind = source_error_kind::source_unavailable,
            .message = "source reader is stopped"});
    }
    if (_client) {
        co_return _client.get();
    }
    auto build = co_await ss::get_units(_build, 1, as);
    if (_stopped) {
        co_return std::unexpected(
          source_error{
            .kind = source_error_kind::source_unavailable,
            .message = "source reader is stopped"});
    }
    if (_client) {
        co_return _client.get();
    }
    try {
        net::base_transport::configuration cfg{.server_addr = _conn->address};
        if (_conn->tls_enabled) {
            auto builder = co_await net::get_credentials_builder({
              .truststore = _conn->truststore,
              .k_store = _conn->client_key,
              .min_tls_version = config::from_config(
                config::shard_local_cfg().tls_min_version()),
              .enable_renegotiation
              = config::shard_local_cfg().tls_enable_renegotiation(),
              .require_client_auth = false,
            });
            cfg.credentials
              = co_await net::build_reloadable_credentials_with_probe<
                ss::tls::certificate_credentials>(
                std::move(builder), "cluster_link/sr_source", "source");
            if (_conn->provide_sni) {
                cfg.tls_sni_hostname = _conn->address.host();
            }
        }
        // Enough connections for the reconciler's fan-out; each http::client
        // connects lazily on first use, so idle pool slots cost no sockets.
        auto pool_size = std::min(
          config::shard_local_cfg().schema_registry_sync_parallelism(),
          max_source_connections);
        std::vector<std::unique_ptr<http::abstract_client>> transports;
        transports.reserve(pool_size);
        for (size_t i = 0; i < pool_size; ++i) {
            transports.push_back(std::make_unique<http::client>(cfg));
        }
        // Limiter over pool: a request pays for dispatch (rate cap and any
        // server-imposed Retry-After pause) before competing for a
        // connection.
        _client = std::make_unique<rc::client>(
          std::make_unique<rc::rate_limited_client>(
            std::make_unique<rc::pooled_client>(std::move(transports)),
            _conn->max_requests_per_sec),
          _conn->endpoint,
          _conn->auth,
          ppsr::qualified_subjects_enabled::yes);
        co_return _client.get();
    } catch (...) {
        co_return std::unexpected(
          source_error{
            .kind = source_error_kind::source_unavailable,
            .message = fmt::format(
              "failed to build source Schema Registry client: {}",
              std::current_exception())});
    }
}

ss::future<source_result<chunked_vector<ppsr::context>>>
http_source_reader::list_contexts(ss::abort_source& as) {
    auto client = co_await ensure_client(as);
    if (!client.has_value()) {
        co_return std::unexpected(std::move(client.error()));
    }
    retry_chain_node rtc(as, request_timeout, request_backoff);
    // No prefix filter: enumerate every context so subjects in non-default
    // contexts are discovered. The default context is always present (".").
    auto res = co_await client.value()->list_contexts(rtc);
    if (!res.has_value()) {
        co_return std::unexpected(to_source_error(std::move(res.error())));
    }
    co_return std::move(res.value());
}

ss::future<source_result<chunked_vector<ppsr::context_subject>>>
http_source_reader::list_subjects(ppsr::context ctx, ss::abort_source& as) {
    auto client = co_await ensure_client(as);
    if (!client.has_value()) {
        co_return std::unexpected(std::move(client.error()));
    }
    retry_chain_node rtc(as, request_timeout, request_backoff);
    // Scope discovery to the requested context: the rest_client sends a
    // subjectPrefix hint and filters the response to that context. Include
    // soft-deleted subjects so a subject whose every version is deleted is
    // still discovered; the reconcile classifies per-version deleted state (via
    // two list_subject_versions calls) and propagates the soft-deletes.
    auto res = co_await client.value()->list_subjects(
      rtc, ppsr::include_deleted::yes, ctx);
    if (!res.has_value()) {
        co_return std::unexpected(to_source_error(std::move(res.error())));
    }
    co_return std::move(res.value());
}

ss::future<source_result<chunked_vector<ppsr::schema_version>>>
http_source_reader::list_subject_versions(
  ppsr::context_subject sub,
  ppsr::include_deleted include_deleted,
  ss::abort_source& as) {
    auto client = co_await ensure_client(as);
    if (!client.has_value()) {
        co_return std::unexpected(std::move(client.error()));
    }
    retry_chain_node rtc(as, request_timeout, request_backoff);
    auto res = co_await client.value()->list_subject_versions(
      sub, rtc, include_deleted);
    if (!res.has_value()) {
        co_return std::unexpected(to_source_error(std::move(res.error())));
    }
    co_return std::move(res.value());
}

ss::future<source_result<chunked_vector<ppsr::subject_version>>>
http_source_reader::list_schema_id_subject_versions(
  ppsr::schema_id id, ppsr::context ctx, ss::abort_source& as) {
    auto client = co_await ensure_client(as);
    if (!client.has_value()) {
        co_return std::unexpected(std::move(client.error()));
    }
    retry_chain_node rtc(as, request_timeout, request_backoff);
    // Ids are namespaced per context. The bare-context form (empty subject,
    // wire ":.dev:") names the context without constraining which subject
    // carries the id -- that is what the probe is discovering. As with
    // mode/config, the default context omits the parameter so a source
    // without context support is still served.
    auto subject = ctx == ppsr::default_context
                     ? std::nullopt
                     : std::optional{
                         ppsr::context_subject{ctx, ppsr::subject{""}}};
    auto res = co_await client.value()->get_schema_id_subject_versions(
      id, rtc, std::move(subject));
    if (!res.has_value()) {
        co_return std::unexpected(to_source_error(std::move(res.error())));
    }
    co_return std::move(res.value());
}

ss::future<source_result<ppsr::source_schema_read>>
http_source_reader::read_subject_version(
  ppsr::context_subject sub,
  ppsr::schema_version version,
  ss::abort_source& as) {
    auto client = co_await ensure_client(as);
    if (!client.has_value()) {
        co_return std::unexpected(std::move(client.error()));
    }
    retry_chain_node rtc(as, request_timeout, request_backoff);
    // Include deleted: the reconcile fetches soft-deleted source versions to
    // propagate the soft-delete, so a deleted version must still resolve here
    // (an active version is returned just the same).
    auto res = co_await client.value()->get_schema_by_version(
      sub, version, rtc, ppsr::include_deleted::yes);
    if (!res.has_value()) {
        co_return std::unexpected(to_source_error(std::move(res.error())));
    }
    // Carry the read through unchanged: the schema, any unsupported fields the
    // source served but Redpanda cannot store, and whether the source reported
    // the `deleted` flag. The reconciler applies the configured
    // unsupported-feature policy to `source_schema_read::unsupported` and
    // prefers the reported `deleted` over its listing-derived fallback.
    co_return std::move(res.value());
}

ss::future<source_result<std::optional<ppsr::mode>>>
http_source_reader::read_mode(ppsr::context_subject sub, ss::abort_source& as) {
    auto client = co_await ensure_client(as);
    if (!client.has_value()) {
        co_return std::unexpected(std::move(client.error()));
    }
    retry_chain_node rtc(as, request_timeout, request_backoff);
    // Prefer GET /mode over GET /mode/. for the default context to be
    // backwards-compatible with a source that does not implement contexts.
    auto res = sub.is_default_context()
                 ? co_await client.value()->get_mode(rtc)
                 : co_await client.value()->get_subject_mode(
                     sub, rtc, ppsr::default_to_global::no);
    if (!res.has_value()) {
        if (std::holds_alternative<rc::subject_mode_not_found>(res.error())) {
            co_return std::optional<ppsr::mode>{std::nullopt};
        }
        co_return std::unexpected(to_source_error(std::move(res.error())));
    }
    auto narrowed = narrow_mode(res.value().mode);
    if (!narrowed.has_value()) {
        co_return std::unexpected(
          source_error{
            .kind = source_error_kind::operation_failed,
            .message = fmt::format(
              "source mode '{}' of {} is not supported by the destination",
              res.value().raw,
              sub)});
    }
    co_return narrowed;
}

ss::future<source_result<source_config_read>> http_source_reader::read_config(
  ppsr::context_subject sub, ss::abort_source& as) {
    auto client = co_await ensure_client(as);
    if (!client.has_value()) {
        co_return std::unexpected(std::move(client.error()));
    }
    retry_chain_node rtc(as, request_timeout, request_backoff);
    // Prefer GET /config over GET /config/. for the default context to be
    // backwards-compatible with a source that does not implement contexts.
    auto res = sub.is_default_context()
                 ? co_await client.value()->get_config(rtc)
                 : co_await client.value()->get_subject_config(
                     sub, rtc, ppsr::default_to_global::no);
    if (!res.has_value()) {
        if (std::holds_alternative<rc::subject_config_not_found>(res.error())) {
            co_return source_config_read{.compatibility = std::nullopt};
        }
        co_return std::unexpected(to_source_error(std::move(res.error())));
    }
    // A governance-only config (no compatibility level) is "no override"; its
    // unsupported fields still reach the policy. The Config API documents the
    // subject-level compatibility as optional ("the compatibility, if any"):
    // https://docs.confluent.io/platform/current/schema-registry/develop/api.html#config
    std::optional<ppsr::compatibility_level> narrowed;
    if (res.value().level.has_value()) {
        narrowed = narrow_compat(*res.value().level);
        if (!narrowed.has_value()) {
            co_return std::unexpected(
              source_error{
                .kind = source_error_kind::operation_failed,
                .message = fmt::format(
                  "source compatibility level '{}' of {} is not supported by "
                  "the destination",
                  res.value().raw,
                  sub)});
        }
    }
    // Carry the unsupported config fields through for the sync's policy.
    co_return source_config_read{
      .compatibility = narrowed,
      .unsupported = std::move(res.value().unsupported)};
}

ss::future<> http_source_reader::stop() {
    // Idempotent: the reader can be stopped more than once (e.g. an in-flight
    // reconciler stopping the task before link teardown stops it again). The
    // rest_client's gate must be closed exactly once, so release the client
    // after shutting it down and skip it on a repeat call. The sticky
    // _stopped flag rejects post-stop rebuilds, and waiting out the build
    // mutex means a build that raced this stop is shut down here rather than
    // leaked.
    _stopped = true;
    auto build = co_await ss::get_units(_build, 1);
    if (auto client = std::move(_client); client) {
        co_await client->shutdown();
    }
}

std::unique_ptr<source_reader> http_source_reader_factory::create(
  const model::schema_registry_sync_config::shadow_schema_registry_api*
    api_cfg) {
    if (api_cfg == nullptr) {
        return std::make_unique<unavailable_source_reader>();
    }
    auto address = parse_source_address(api_cfg->source_url);
    if (!address.has_value()) {
        return std::make_unique<unavailable_source_reader>(fmt::format(
          "invalid source Schema Registry URL: '{}'", api_cfg->source_url));
    }

    http_source_connection conn{
      .address = std::move(*address),
      .endpoint = api_cfg->source_url,
      .tls_enabled = bool(api_cfg->tls_enabled),
      .provide_sni = bool(api_cfg->tls_provide_sni),
      .max_requests_per_sec = static_cast<size_t>(
        api_cfg->get_max_source_requests_per_second())};

    if (api_cfg->ca.has_value()) {
        conn.truststore = to_certificate(api_cfg->ca.value());
    }
    if (api_cfg->cert.has_value() && api_cfg->key.has_value()) {
        conn.client_key = to_key_store(
          api_cfg->key.value(), api_cfg->cert.value());
    }
    if (api_cfg->auth_config.has_value()) {
        conn.auth = ss::visit(
          api_cfg->auth_config.value(),
          [](const model::schema_registry_sync_config::basic_auth& basic) {
              return rc::basic_auth_credentials{
                .username = basic.username, .password = basic.password};
          });
    }

    return std::make_unique<http_source_reader>(std::move(conn));
}

} // namespace cluster_link::schema_registry_sync

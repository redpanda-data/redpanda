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
#include "pandaproxy/schema_registry/rest_client/client.h"

#include "bytes/iobuf.h"
#include "http/request_builder.h"
#include "http/utils.h"
#include "pandaproxy/schema_registry/rest_client/logger.h"
#include "pandaproxy/schema_registry/rest_client/parse.h"
#include "ssx/future-util.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/sleep.hh>
#include <seastar/coroutine/as_future.hh>

#include <boost/beast/http/status.hpp>
#include <boost/beast/http/verb.hpp>

#include <chrono>
#include <optional>
#include <utility>
#include <variant>

namespace pandaproxy::schema_registry::rest_client {

namespace {

constexpr std::string_view accept_json = "application/json";

// Confluent returns extended schema fields (guid, ts, deleted) on a schema
// response only when this request header is present. The migration client opts
// in so the tolerant parser sees the full response and the ignorable-field list
// explicitly drops the server-assigned ones (guid, ts) rather than relying on
// the server to omit them.
constexpr std::string_view accept_unknown_properties_header
  = "Confluent-Accept-Unknown-Properties";

// Schema Registry error_codes for the not-found conditions.
constexpr int32_t error_code_subject_not_found = 40401;
constexpr int32_t error_code_version_not_found = 40402;
constexpr int32_t error_code_schema_id_not_found = 40403;
constexpr int32_t error_code_subject_config_not_found = 40408;
constexpr int32_t error_code_subject_mode_not_found = 40409;

// Percent-encode a subject for use as a single path segment. The qualified wire
// form ":.ctx:sub" contains ':' which must be encoded ("%3A"); uri_encode also
// encodes an interior '/' ("%2F") so the subject stays within one segment.
// request_builder/ada leave the resulting "%XX" intact (verified), so the
// subject is encoded exactly once.
ss::sstring encode_subject(const context_subject& subject) {
    return http::uri_encode(subject.to_string(), http::uri_encode_slash::yes);
}

// Append ?deleted=true when soft-deleted entries should be included. Omitted
// otherwise, so the default request shape and behavior are unchanged. The
// Schema Registry parses the value as true when it equals "true" or "1".
void add_deleted_param(
  http::request_builder& request, include_deleted deleted) {
    if (deleted) {
        request.query_param_kv("deleted", "true");
    }
}

// Append ?defaultToGlobal=true to request the effective (hierarchy-resolved)
// mode instead of the subject's own override. Omitted otherwise, so the default
// request asks for the subject's own mode (which yields 40409 when unset).
void add_default_to_global_param(
  http::request_builder& request, default_to_global fallback) {
    if (fallback) {
        request.query_param_kv("defaultToGlobal", "true");
    }
}

// Translate a terminal 404 into a typed not-found error using the error_code
// that perform_request attached. Anything else (other statuses, unrecognized
// codes) passes through unchanged. version is set only for
// get_schema_by_version (40402 is meaningless for the subject listing).
domain_error translate_not_found(
  domain_error err,
  const context_subject& subject,
  std::optional<schema_version> version) {
    auto* call = std::get_if<http_call_error>(&err);
    if (call == nullptr) {
        return err;
    }
    auto* status = std::get_if<http_status_error>(call);
    if (status == nullptr) {
        return err;
    }
    if (status->status != boost::beast::http::status::not_found) {
        return err;
    }
    if (status->error_code == error_code_subject_not_found) {
        return domain_error{subject_not_found{subject}};
    }
    if (
      version.has_value()
      && status->error_code == error_code_version_not_found) {
        return domain_error{version_not_found{subject, *version}};
    }
    return err;
}

// Translate a terminal 404 from GET /mode/{subject} into the typed
// subject_mode_not_found: error_code 40409 is the documented "no subject-level
// mode" signal, and some server versions use 40401 for the same case, so both
// map. Anything else (other statuses, a bare 404 for an unknown path, non-http
// errors like retries_exhausted) passes through unchanged. Only reachable with
// default_to_global::no; with yes the effective mode always resolves.
domain_error translate_subject_mode_not_found(
  domain_error err, const context_subject& subject) {
    auto* call = std::get_if<http_call_error>(&err);
    if (call == nullptr) {
        return err;
    }
    auto* status = std::get_if<http_status_error>(call);
    if (status == nullptr) {
        return err;
    }
    if (status->status != boost::beast::http::status::not_found) {
        return err;
    }
    if (
      status->error_code == error_code_subject_mode_not_found
      || status->error_code == error_code_subject_not_found) {
        return domain_error{subject_mode_not_found{subject}};
    }
    return err;
}

// Translate a terminal 404 from GET /config/{subject} into the typed
// subject_config_not_found: error_code 40408 is the documented "no
// subject-level compatibility" signal, and some server versions use 40401 for
// the same case, so both map. Anything else (other statuses, a bare 404 for an
// unknown path, non-http errors like retries_exhausted) passes through
// unchanged. Only reachable with default_to_global::no; with yes the effective
// config always resolves.
domain_error translate_subject_config_not_found(
  domain_error err, const context_subject& subject) {
    auto* call = std::get_if<http_call_error>(&err);
    if (call == nullptr) {
        return err;
    }
    auto* status = std::get_if<http_status_error>(call);
    if (status == nullptr) {
        return err;
    }
    if (status->status != boost::beast::http::status::not_found) {
        return err;
    }
    if (
      status->error_code == error_code_subject_config_not_found
      || status->error_code == error_code_subject_not_found) {
        return domain_error{subject_config_not_found{subject}};
    }
    return err;
}

// Translate a terminal 404 from GET /schemas/ids/{id}/versions into the typed
// schema_id_not_found: error_code 40403 (the schema-not-found code) means the
// id does not resolve in the searched context. Unlike the subject/config
// endpoints this uses only 40403 — a bare 404 (unknown path), other statuses,
// and non-http errors all pass through unchanged.
domain_error translate_schema_id_not_found(domain_error err, schema_id id) {
    auto* call = std::get_if<http_call_error>(&err);
    if (call == nullptr) {
        return err;
    }
    auto* status = std::get_if<http_status_error>(call);
    if (status == nullptr) {
        return err;
    }
    if (status->status != boost::beast::http::status::not_found) {
        return err;
    }
    if (status->error_code == error_code_schema_id_not_found) {
        return domain_error{schema_id_not_found{id}};
    }
    return err;
}

// If the terminal error carries an http_status_error, parse its (already
// collected) response body for the Schema Registry error_code and attach it.
// Tolerant: leaves the error unchanged when no integer error_code is present.
ss::future<domain_error> attach_error_code(domain_error err) {
    auto* call = std::get_if<http_call_error>(&err);
    if (call == nullptr) {
        co_return std::move(err);
    }
    auto* status = std::get_if<http_status_error>(call);
    if (status == nullptr) {
        co_return std::move(err);
    }
    auto parsed = co_await parse_error_body(iobuf::from(status->body));
    if (parsed.has_value()) {
        status->error_code = parsed->error_code;
        if (!parsed->message.empty()) {
            status->message = std::move(parsed->message);
        }
    }
    co_return std::move(err);
}

} // namespace

client::client(
  std::unique_ptr<http::abstract_client> http_client,
  ss::sstring endpoint,
  std::optional<basic_auth_credentials> credentials,
  qualified_subjects_enabled qualified,
  std::unique_ptr<retry_policy> retry_policy)
  : _http_client(std::move(http_client))
  , _endpoint(std::move(endpoint))
  , _credentials(std::move(credentials))
  , _qualified(qualified)
  , _retry_policy(
      retry_policy ? std::move(retry_policy)
                   : std::make_unique<default_retry_policy>()) {}

ss::future<> client::shutdown() {
    auto gate_closed = _gate.close();
    co_await _http_client->shutdown_and_stop();
    co_await std::move(gate_closed);
}

std::expected<ss::gate::holder, domain_error> client::maybe_gate() {
    if (_gate.is_closed()) {
        return std::unexpected(
          domain_error{aborted_error{"client gate is closed"}});
    }
    return _gate.hold();
}

void client::maybe_add_basic_auth(http::request_builder& request) {
    if (_credentials.has_value()) {
        request.with_basic_auth(_credentials->username, _credentials->password);
    }
}

ss::future<std::expected<iobuf, domain_error>> client::perform_request(
  retry_chain_node& parent_rtc,
  http::request_builder builder,
  std::optional<iobuf> payload) {
    if (payload.has_value()) {
        builder.with_content_length(payload.value().size_bytes());
    }
    retry_chain_node rtc(&parent_rtc);
    std::vector<error_kind> retriable_errors;
    std::optional<http_call_error> last_error;
    // Target (path + query) of the most recent attempt, for the failure logs
    // below. Empty until the first request is built.
    ss::sstring request_target;

    while (true) {
        retry_permit permit{};
        try {
            permit = rtc.retry();
        } catch (...) {
            auto ex = std::current_exception();
            auto msg = fmt::format("{}", ex);
            if (ssx::is_shutdown_exception(ex)) {
                vlog(srclog.debug, "shutting down during request: {}", msg);
                co_return std::unexpected(domain_error{aborted_error{msg}});
            }
            // We only expect shutdown exceptions here; treat anything else
            // conservatively as exhausted rather than aborted.
            vlog(
              srclog.warn,
              "schema registry request gave up [{}]: {}",
              request_target,
              msg);
            co_return std::unexpected(
              domain_error{retries_exhausted{
                .reasons = std::move(retriable_errors),
                .last_error = std::move(last_error)}});
        }
        if (!permit.is_allowed) {
            co_return std::unexpected(
              domain_error{retries_exhausted{
                .reasons = std::move(retriable_errors),
                .last_error = std::move(last_error)}});
        }

        auto request = builder.host(_endpoint).build();
        if (!request.has_value()) {
            co_return std::unexpected(domain_error{request.error()});
        }
        request_target = ss::sstring{
          request->target().begin(), request->target().end()};

        auto response_f = co_await ss::coroutine::as_future(
          _http_client->request_and_collect_response(
            std::move(request.value()),
            payload.has_value() ? std::make_optional(payload->copy())
                                : std::nullopt));

        auto call_res = _retry_policy->should_retry(std::move(response_f));
        if (call_res.has_value()) {
            co_return std::move(call_res->body);
        }

        auto& error = call_res.error();
        if (error.kind == error_kind::aborted) {
            co_return std::unexpected(
              domain_error{
                aborted_error{"shutting down while evaluating retry"}});
        }
        if (!is_retriable(error.kind)) {
            vlog(
              srclog.warn,
              "schema registry request failed [{}]: {}",
              request_target,
              error.err);
            co_return std::unexpected(
              co_await attach_error_code(domain_error{std::move(error.err)}));
        }

        vlog(
          srclog.trace,
          "schema registry request failed [{}], retrying in {}ms: {}",
          request_target,
          std::chrono::duration_cast<std::chrono::milliseconds>(permit.delay)
            .count(),
          error.err);
        retriable_errors.push_back(error.kind);
        last_error.emplace(std::move(error.err));
        auto sleep_fut = co_await ss::coroutine::as_future(
          ss::sleep_abortable(permit.delay, rtc.root_abort_source()));
        if (sleep_fut.failed()) {
            auto msg = fmt::format(
              "exception during retry sleep: {}", sleep_fut.get_exception());
            vlog(srclog.debug, "{}", msg);
            co_return std::unexpected(domain_error{aborted_error{msg}});
        }
    }
}

ss::future<std::expected<chunked_vector<context_subject>, domain_error>>
client::list_subjects(
  retry_chain_node& rtc, include_deleted deleted, std::optional<context> ctx) {
    auto gate = maybe_gate();
    if (!gate.has_value()) {
        co_return std::unexpected(std::move(gate.error()));
    }
    // TODO: offset/limit pagination and deletedOnly are unimplemented (Redpanda
    // SR doesn't support them).
    auto request = http::request_builder{}
                     .method(boost::beast::http::verb::get)
                     .path("/subjects")
                     .header("accept", accept_json);
    add_deleted_param(request, deleted);
    if (ctx.has_value()) {
        // A source-filtering hint for servers that honor it: ":<ctx>:" is the
        // context-qualified subject prefix that scopes to a single context
        // (e.g. ":.dev:", or ":.:" for the default context). The result is also
        // filtered client-side below, so it is correct against a server that
        // ignores the hint (Redpanda's own server honors it).
        request.query_param_kv("subjectPrefix", fmt::format(":{}:", (*ctx)()));
    }
    maybe_add_basic_auth(request);

    auto response = co_await perform_request(rtc, std::move(request));
    if (!response.has_value()) {
        co_return std::unexpected(std::move(response.error()));
    }
    auto parsed = co_await parse_subjects(
      std::move(response.value()), _qualified);
    if (!parsed.has_value()) {
        co_return std::unexpected(domain_error{std::move(parsed.error())});
    }
    if (ctx.has_value()) {
        // Keep only the requested context so the listing is correctly scoped
        // even if the server ignored the subjectPrefix hint.
        chunked_vector<context_subject> filtered;
        for (auto& cs : parsed.value()) {
            if (cs.ctx == *ctx) {
                filtered.push_back(std::move(cs));
            }
        }
        co_return std::move(filtered);
    }
    co_return std::move(parsed.value());
}

ss::future<std::expected<chunked_vector<context>, domain_error>>
client::list_contexts(
  retry_chain_node& rtc, std::optional<ss::sstring> context_prefix) {
    auto gate = maybe_gate();
    if (!gate.has_value()) {
        co_return std::unexpected(std::move(gate.error()));
    }
    // TODO: offset/limit pagination is unimplemented (Redpanda SR ignores it
    // and returns every materialized context in one unpaginated call).
    auto request = http::request_builder{}
                     .method(boost::beast::http::verb::get)
                     .path("/contexts")
                     .header("accept", accept_json);
    if (context_prefix.has_value()) {
        // A source-filtering hint for servers that honor it; the result is also
        // filtered client-side below, so it is correct against a server that
        // ignores it (Redpanda's own server does).
        request.query_param_kv("contextPrefix", *context_prefix);
    }
    maybe_add_basic_auth(request);

    auto response = co_await perform_request(rtc, std::move(request));
    if (!response.has_value()) {
        co_return std::unexpected(std::move(response.error()));
    }
    auto parsed = co_await parse_contexts(std::move(response.value()));
    if (!parsed.has_value()) {
        co_return std::unexpected(domain_error{std::move(parsed.error())});
    }
    if (context_prefix.has_value()) {
        // Filter by the returned dot-prefixed name form (e.g. ".prod" matches
        // ".prod" and ".prod-eu"), matching the server's contextPrefix
        // semantics, so the result is correct even if the server did not
        // filter.
        chunked_vector<context> filtered;
        for (auto& ctx : parsed.value()) {
            if (
              std::string_view{ctx()}.starts_with(
                std::string_view{*context_prefix})) {
                filtered.push_back(std::move(ctx));
            }
        }
        co_return std::move(filtered);
    }
    co_return std::move(parsed.value());
}

ss::future<std::expected<mode_info, domain_error>>
client::get_mode(retry_chain_node& rtc) {
    auto gate = maybe_gate();
    if (!gate.has_value()) {
        co_return std::unexpected(std::move(gate.error()));
    }
    // defaultToGlobal is omitted: on the subject-less GET /mode it has no
    // observable effect (the global mode is returned either way).
    auto request = http::request_builder{}
                     .method(boost::beast::http::verb::get)
                     .path("/mode")
                     .header("accept", accept_json);
    maybe_add_basic_auth(request);

    auto response = co_await perform_request(rtc, std::move(request));
    if (!response.has_value()) {
        co_return std::unexpected(std::move(response.error()));
    }
    auto parsed = co_await parse_mode(std::move(response.value()));
    if (!parsed.has_value()) {
        co_return std::unexpected(domain_error{std::move(parsed.error())});
    }
    co_return std::move(parsed.value());
}

ss::future<std::expected<mode_info, domain_error>> client::get_subject_mode(
  const context_subject& subject,
  retry_chain_node& rtc,
  default_to_global fallback) {
    auto gate = maybe_gate();
    if (!gate.has_value()) {
        co_return std::unexpected(std::move(gate.error()));
    }
    auto request = http::request_builder{}
                     .method(boost::beast::http::verb::get)
                     .path(fmt::format("/mode/{}", encode_subject(subject)))
                     .header("accept", accept_json);
    add_default_to_global_param(request, fallback);
    maybe_add_basic_auth(request);

    auto response = co_await perform_request(rtc, std::move(request));
    if (!response.has_value()) {
        co_return std::unexpected(translate_subject_mode_not_found(
          std::move(response.error()), subject));
    }
    auto parsed = co_await parse_mode(std::move(response.value()));
    if (!parsed.has_value()) {
        co_return std::unexpected(domain_error{std::move(parsed.error())});
    }
    co_return std::move(parsed.value());
}

ss::future<std::expected<config_info, domain_error>>
client::get_config(retry_chain_node& rtc) {
    auto gate = maybe_gate();
    if (!gate.has_value()) {
        co_return std::unexpected(std::move(gate.error()));
    }
    // defaultToGlobal is omitted: on the subject-less GET /config it has no
    // observable effect (the global config is returned either way).
    auto request = http::request_builder{}
                     .method(boost::beast::http::verb::get)
                     .path("/config")
                     .header("accept", accept_json);
    maybe_add_basic_auth(request);

    auto response = co_await perform_request(rtc, std::move(request));
    if (!response.has_value()) {
        co_return std::unexpected(std::move(response.error()));
    }
    auto parsed = co_await parse_config(std::move(response.value()));
    if (!parsed.has_value()) {
        co_return std::unexpected(domain_error{std::move(parsed.error())});
    }
    co_return std::move(parsed.value());
}

ss::future<std::expected<config_info, domain_error>> client::get_subject_config(
  const context_subject& subject,
  retry_chain_node& rtc,
  default_to_global fallback) {
    auto gate = maybe_gate();
    if (!gate.has_value()) {
        co_return std::unexpected(std::move(gate.error()));
    }
    auto request = http::request_builder{}
                     .method(boost::beast::http::verb::get)
                     .path(fmt::format("/config/{}", encode_subject(subject)))
                     .header("accept", accept_json);
    add_default_to_global_param(request, fallback);
    maybe_add_basic_auth(request);

    auto response = co_await perform_request(rtc, std::move(request));
    if (!response.has_value()) {
        co_return std::unexpected(translate_subject_config_not_found(
          std::move(response.error()), subject));
    }
    auto parsed = co_await parse_config(std::move(response.value()));
    if (!parsed.has_value()) {
        co_return std::unexpected(domain_error{std::move(parsed.error())});
    }
    co_return std::move(parsed.value());
}

ss::future<std::expected<chunked_vector<schema_version>, domain_error>>
client::list_subject_versions(
  const context_subject& subject,
  retry_chain_node& rtc,
  include_deleted deleted) {
    auto gate = maybe_gate();
    if (!gate.has_value()) {
        co_return std::unexpected(std::move(gate.error()));
    }
    // TODO: offset/limit pagination, deletedOnly, and deletedAsNegative are
    // unimplemented (Redpanda SR doesn't support them).
    auto request
      = http::request_builder{}
          .method(boost::beast::http::verb::get)
          .path(fmt::format("/subjects/{}/versions", encode_subject(subject)))
          .header("accept", accept_json);
    add_deleted_param(request, deleted);
    maybe_add_basic_auth(request);

    auto response = co_await perform_request(rtc, std::move(request));
    if (!response.has_value()) {
        co_return std::unexpected(translate_not_found(
          std::move(response.error()), subject, std::nullopt));
    }
    auto parsed = co_await parse_subject_versions(std::move(response.value()));
    if (!parsed.has_value()) {
        co_return std::unexpected(domain_error{std::move(parsed.error())});
    }
    co_return std::move(parsed.value());
}

ss::future<std::expected<source_schema_read, domain_error>>
client::get_schema_by_version(
  const context_subject& subject,
  schema_version version,
  retry_chain_node& rtc,
  include_deleted deleted) {
    auto gate = maybe_gate();
    if (!gate.has_value()) {
        co_return std::unexpected(std::move(gate.error()));
    }
    auto request
      = http::request_builder{}
          .method(boost::beast::http::verb::get)
          .path(
            fmt::format(
              "/subjects/{}/versions/{}", encode_subject(subject), version()))
          .header("accept", accept_json)
          .header(accept_unknown_properties_header, "true");
    add_deleted_param(request, deleted);
    maybe_add_basic_auth(request);

    auto response = co_await perform_request(rtc, std::move(request));
    if (!response.has_value()) {
        co_return std::unexpected(
          translate_not_found(std::move(response.error()), subject, version));
    }
    auto parsed = co_await parse_subject_version(
      std::move(response.value()), _qualified);
    if (!parsed.has_value()) {
        co_return std::unexpected(domain_error{std::move(parsed.error())});
    }
    co_return std::move(parsed.value());
}

ss::future<std::expected<chunked_vector<subject_version>, domain_error>>
client::get_schema_id_subject_versions(
  schema_id id,
  retry_chain_node& rtc,
  std::optional<context_subject> subject,
  include_deleted inc) {
    auto gate = maybe_gate();
    if (!gate.has_value()) {
        co_return std::unexpected(std::move(gate.error()));
    }
    auto request = http::request_builder{}
                     .method(boost::beast::http::verb::get)
                     .path(fmt::format("/schemas/ids/{}/versions", id()))
                     .header("accept", accept_json);
    if (subject.has_value()) {
        // Selects the context to resolve the id in. query_param_kv
        // percent-encodes the value (":" -> "%3A").
        request.query_param_kv("subject", subject->to_string());
    }
    if (inc == include_deleted::yes) {
        request.query_param_kv("deleted", "true");
    }
    maybe_add_basic_auth(request);

    auto response = co_await perform_request(rtc, std::move(request));
    if (!response.has_value()) {
        co_return std::unexpected(
          translate_schema_id_not_found(std::move(response.error()), id));
    }
    auto parsed = co_await parse_schema_id_subject_versions(
      std::move(response.value()), _qualified);
    if (!parsed.has_value()) {
        co_return std::unexpected(domain_error{std::move(parsed.error())});
    }
    co_return std::move(parsed.value());
}

} // namespace pandaproxy::schema_registry::rest_client

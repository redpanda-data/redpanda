// Copyright 2021 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "handlers.h"

#include "bytes/iobuf_parser.h"
#include "cluster/controller.h"
#include "cluster/security_frontend.h"
#include "container/json.h"
#include "pandaproxy/json/rjson_util.h"
#include "pandaproxy/json/types.h"
#include "pandaproxy/logger.h"
#include "pandaproxy/parsing/httpd.h"
#include "pandaproxy/schema_registry/authorization.h"
#include "pandaproxy/schema_registry/error.h"
#include "pandaproxy/schema_registry/errors.h"
#include "pandaproxy/schema_registry/requests/acls.h"
#include "pandaproxy/schema_registry/requests/compatibility.h"
#include "pandaproxy/schema_registry/requests/config.h"
#include "pandaproxy/schema_registry/requests/get_schemas_ids_id.h"
#include "pandaproxy/schema_registry/requests/get_schemas_ids_id_versions.h"
#include "pandaproxy/schema_registry/requests/get_subject_versions_version.h"
#include "pandaproxy/schema_registry/requests/mode.h"
#include "pandaproxy/schema_registry/requests/post_subject_versions.h"
#include "pandaproxy/schema_registry/types.h"
#include "pandaproxy/server.h"
#include "security/acl.h"
#include "security/acl_store.h"
#include "security/authorizer.h"
#include "security/fwd.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/future.hh>
#include <seastar/core/sstring.hh>

#include <algorithm>
#include <iterator>
#include <limits>

namespace ppj = pandaproxy::json;

namespace pandaproxy::schema_registry {

using server = ctx_server<service>;

void parse_accept_header(const server::request_t& rq, server::reply_t& rp) {
    static const std::vector<ppj::serialization_format> headers{
      ppj::serialization_format::schema_registry_v1_json,
      ppj::serialization_format::schema_registry_json,
      ppj::serialization_format::application_json,
      ppj::serialization_format::none};
    rp.mime_type = parse::accept_header(*rq.req, headers);
}

void parse_content_type_header(const server::request_t& rq) {
    static const std::vector<ppj::serialization_format> headers{
      ppj::serialization_format::schema_registry_v1_json,
      ppj::serialization_format::schema_registry_json,
      ppj::serialization_format::application_json,
      ppj::serialization_format::application_octet};
    parse::content_type_header(*rq.req, headers);
}

result<schema_version> parse_numerical_schema_version(const ss::sstring& ver) {
    auto res = parse::from_chars<int64_t>{}(ver);
    if (
      res.has_error() || res.assume_value() < 1
      || res.assume_value() > std::numeric_limits<int32_t>::max()) {
        return schema_version_invalid(ver);
    }

    return schema_version{static_cast<int32_t>(res.assume_value())};
}

result<std::optional<schema_version>>
parse_schema_version(const ss::sstring& ver) {
    return (ver == "latest" || ver == "-1")
             ? std::optional<schema_version>{}
             : parse_numerical_schema_version(ver).value();
}

output_format parse_output_format(const ss::http::request& req) {
    return parse::query_param<std::optional<ss::sstring>>(req, "format")
      .and_then(&from_string_view<output_format>)
      .value_or(output_format::none);
}

template<ppj::impl::RjsonParseHandler Handler>
typename ss::future<typename Handler::rjson_parse_result>
rjson_parse(ss::http::request& req, Handler handler) {
    co_return co_await ppj::rjson_parse(req, std::move(handler), srreqs);
}

void log_response(const ss::http::request& req, const iobuf& resp) {
    if (srreqs.is_enabled(ss::log_level::trace)) {
        iobuf_const_parser parser{resp};
        vlog(
          srreqs.trace,
          "[{}:{}] sending response {} {}: {:?}",
          req.get_client_address().addr(),
          req.get_client_address().port(),
          req._method,
          req._url,
          parser.read_string(
            std::min(parser.bytes_left(), max_log_line_bytes)));
    }
}

ss::future<server::reply_t>
get_config(server::request_t rq, server::reply_t rp) {
    parse_accept_header(rq, rp);

    // Ensure we see latest writes
    co_await rq.service().writer().read_sync();

    auto res = co_await rq.service().schema_store().get_compatibility();

    auto resp = ppj::rjson_serialize_iobuf(get_config_req_rep{.compat = res});
    log_response(*rq.req, resp);
    rp.rep->write_body("json", ppj::as_body_writer(std::move(resp)));
    co_return rp;
}

ss::future<server::reply_t>
put_config(server::request_t rq, server::reply_t rp) {
    parse_content_type_header(rq);
    parse_accept_header(rq, rp);
    auto config = co_await rjson_parse(*rq.req, put_config_handler<>{});

    co_await rq.service().writer().write_config(std::nullopt, config.compat);

    auto resp = ppj::rjson_serialize_iobuf(config);
    log_response(*rq.req, resp);
    rp.rep->write_body("json", ppj::as_body_writer(std::move(resp)));
    co_return rp;
}

ss::future<server::reply_t>
get_config_subject(server::request_t rq, server::reply_t rp) {
    parse_accept_header(rq, rp);
    auto sub = parse::request_param<subject>(*rq.req, "subject");
    auto fallback = parse::query_param<std::optional<default_to_global>>(
                      *rq.req, "defaultToGlobal")
                      .value_or(default_to_global::no);

    // Ensure we see latest writes
    co_await rq.service().writer().read_sync();

    auto res = co_await rq.service().schema_store().get_compatibility(
      sub, fallback);

    auto resp = ppj::rjson_serialize_iobuf(get_config_req_rep{.compat = res});
    log_response(*rq.req, resp);
    rp.rep->write_body("json", ppj::as_body_writer(std::move(resp)));
    co_return rp;
}

/// For GETs that load a specific version, we usually find it in memory,
/// but if it's missing, trigger a re-read of the topic before responding
/// definitively as to whether it is present or not.
///
/// This is still only eventually consistent for deletes: if we have a
/// requested ID in cache it might have been deleted else where and
/// we won't notice.
template<typename F>
std::invoke_result_t<F> get_or_load(server::request_t& rq, F f) {
    try {
        co_return co_await f();
    } catch (pandaproxy::schema_registry::exception& ex) {
        if (
          ex.code() == error_code::schema_id_not_found
          || ex.code() == error_code::subject_not_found
          || ex.code() == error_code::subject_version_not_found) {
            // A missing object, we will proceed to reload to see if we can
            // find it.

        } else {
            // Not a missing object, something else went wrong
            throw;
        }
    }

    // Load latest writes and retry
    vlog(srlog.debug, "get_or_load: refreshing schema store on missing item");
    co_await rq.service().writer().read_sync();
    co_return co_await f();
}

ss::future<server::reply_t>
put_config_subject(server::request_t rq, server::reply_t rp) {
    parse_content_type_header(rq);
    parse_accept_header(rq, rp);
    auto sub = parse::request_param<subject>(*rq.req, "subject");
    auto config = co_await rjson_parse(*rq.req, put_config_handler<>{});

    // Ensure we see latest writes
    co_await rq.service().writer().read_sync();
    co_await rq.service().writer().write_config(sub, config.compat);

    auto resp = ppj::rjson_serialize_iobuf(std::move(config));
    log_response(*rq.req, resp);
    rp.rep->write_body("json", ppj::as_body_writer(std::move(resp)));
    co_return rp;
}

ss::future<server::reply_t>
delete_config_subject(server::request_t rq, server::reply_t rp) {
    parse_accept_header(rq, rp);
    auto sub = parse::request_param<subject>(*rq.req, "subject");

    // ensure we see latest writes
    co_await rq.service().writer().read_sync();
    co_await rq.service().writer().check_mutable(sub);

    compatibility_level lvl{};
    try {
        lvl = co_await rq.service().schema_store().get_compatibility(
          sub, default_to_global::no);
    } catch (const exception& e) {
        if (e.code() == error_code::compatibility_not_found) {
            throw as_exception(not_found(sub));
        } else {
            throw;
        }
    }

    co_await rq.service().writer().delete_config(sub);

    auto resp = ppj::rjson_serialize_iobuf(get_config_req_rep{.compat = lvl});
    log_response(*rq.req, resp);
    rp.rep->write_body("json", ppj::as_body_writer(std::move(resp)));
    co_return rp;
}

ss::future<server::reply_t> get_mode(server::request_t rq, server::reply_t rp) {
    parse_accept_header(rq, rp);

    // Ensure we see latest writes
    co_await rq.service().writer().read_sync();

    auto res = co_await rq.service().schema_store().get_mode();

    auto resp = ppj::rjson_serialize_iobuf(mode_req_rep{.mode = res});
    log_response(*rq.req, resp);
    rp.rep->write_body("json", ppj::as_body_writer(std::move(resp)));
    co_return rp;
}

ss::future<server::reply_t> put_mode(server::request_t rq, server::reply_t rp) {
    parse_content_type_header(rq);
    parse_accept_header(rq, rp);
    auto frc = parse::query_param<std::optional<force>>(*rq.req, "force")
                 .value_or(force::no);
    auto res = co_await rjson_parse(*rq.req, mode_handler<>{});

    // Ensure we are up to date (eg. see all existing subjects for import mode)
    co_await rq.service().writer().read_sync();
    co_await rq.service().writer().write_mode(std::nullopt, res.mode, frc);

    auto resp = ppj::rjson_serialize_iobuf(res);
    log_response(*rq.req, resp);
    rp.rep->write_body("json", ppj::as_body_writer(std::move(resp)));
    co_return rp;
}

ss::future<server::reply_t>
get_mode_subject(server::request_t rq, server::reply_t rp) {
    parse_accept_header(rq, rp);
    auto sub = parse::request_param<subject>(*rq.req, "subject");
    auto fallback = parse::query_param<std::optional<default_to_global>>(
                      *rq.req, "defaultToGlobal")
                      .value_or(default_to_global::no);

    // Ensure we see latest writes
    co_await rq.service().writer().read_sync();

    auto res = co_await rq.service().schema_store().get_mode(sub, fallback);

    auto resp = ppj::rjson_serialize_iobuf(mode_req_rep{.mode = res});
    log_response(*rq.req, resp);
    rp.rep->write_body("json", ppj::as_body_writer(std::move(resp)));
    co_return rp;
}

ss::future<server::reply_t>
put_mode_subject(server::request_t rq, server::reply_t rp) {
    parse_content_type_header(rq);
    parse_accept_header(rq, rp);
    auto frc = parse::query_param<std::optional<force>>(*rq.req, "force")
                 .value_or(force::no);
    auto sub = parse::request_param<subject>(*rq.req, "subject");
    auto res = co_await rjson_parse(*rq.req, mode_handler<>{});

    // Ensure we see latest writes
    co_await rq.service().writer().read_sync();
    co_await rq.service().writer().write_mode(sub, res.mode, frc);

    auto resp = ppj::rjson_serialize_iobuf(res);
    log_response(*rq.req, resp);
    rp.rep->write_body("json", ppj::as_body_writer(std::move(resp)));
    co_return rp;
}

ss::future<server::reply_t>
delete_mode_subject(server::request_t rq, server::reply_t rp) {
    parse_accept_header(rq, rp);
    auto sub = parse::request_param<subject>(*rq.req, "subject");

    // ensure we see latest writes
    co_await rq.service().writer().read_sync();

    mode m{};
    try {
        m = co_await rq.service().schema_store().get_mode(
          sub, default_to_global::no);
    } catch (const exception& e) {
        if (e.code() == error_code::mode_not_found) {
            // Upstream compatibility: return 40401 instead of 40409
            throw as_exception(not_found(sub));
        }
        throw;
    }

    co_await rq.service().writer().delete_mode(sub);

    auto resp = ppj::rjson_serialize_iobuf(mode_req_rep{.mode = m});
    log_response(*rq.req, resp);
    rp.rep->write_body("json", ppj::as_body_writer(std::move(resp)));
    co_return rp;
}

ss::future<server::reply_t>
get_schemas_types(server::request_t rq, server::reply_t rp) {
    parse_accept_header(rq, rp);

    static const iobuf schemas_types{ppj::rjson_serialize_iobuf(
      std::vector<std::string_view>{"JSON", "PROTOBUF", "AVRO"})};
    log_response(*rq.req, schemas_types);
    rp.rep->write_body("json", ppj::as_body_writer(schemas_types.copy()));
    return ss::make_ready_future<server::reply_t>(std::move(rp));
}

ss::future<server::reply_t> get_schemas_ids_id(
  server::request_t rq,
  server::reply_t rp,
  std::optional<request_auth_result> auth_result) {
    parse_accept_header(rq, rp);
    auto id = parse::request_param<schema_id>(*rq.req, "id");
    const auto format = parse_output_format(*rq.req);

    co_await rq.service().writer().read_sync();
    auto subjects = co_await rq.service().schema_store().get_schema_subjects(
      id, include_deleted::yes);

    enterprise::handle_get_schemas_ids_id_authz(rq, auth_result, subjects);

    // With deferred schema validation, there might be a schema that
    // had invalid references. These might have already been posted, so
    // we need to sync
    co_await rq.service().writer().read_sync();

    auto def = co_await get_or_load(rq, [&rq, id, format]() {
        return rq.service().schema_store().get_schema_definition(id, format);
    });

    auto resp = ppj::rjson_serialize_iobuf(
      get_schemas_ids_id_response{.definition{std::move(def)}});
    log_response(*rq.req, resp);
    rp.rep->write_body("json", ppj::as_body_writer(std::move(resp)));
    co_return rp;
}

ss::future<server::reply_t>
get_schemas_ids_id_versions(server::request_t rq, server::reply_t rp) {
    parse_accept_header(rq, rp);
    auto id = parse::request_param<schema_id>(*rq.req, "id");

    // List-type request: must ensure we see latest writes
    co_await rq.service().writer().read_sync();

    // Force early 40403 if the schema id isn't found
    co_await rq.service().schema_store().get_schema_definition(id);

    auto svs = co_await rq.service().schema_store().get_schema_subject_versions(
      id);

    auto resp = ppj::rjson_serialize_iobuf(
      get_schemas_ids_id_versions_response{.subject_versions{std::move(svs)}});
    log_response(*rq.req, resp);
    rp.rep->write_body("json", ppj::as_body_writer(std::move(resp)));
    co_return rp;
}

ss::future<ctx_server<service>::reply_t> get_schemas_ids_id_subjects(
  ctx_server<service>::request_t rq, ctx_server<service>::reply_t rp) {
    parse_accept_header(rq, rp);
    auto id = parse::request_param<schema_id>(*rq.req, "id");
    auto incl_del{
      parse::query_param<std::optional<include_deleted>>(*rq.req, "deleted")
        .value_or(include_deleted::no)};

    // List-type request: must ensure we see latest writes
    co_await rq.service().writer().read_sync();

    // Force early 40403 if the schema id isn't found
    co_await rq.service().schema_store().get_schema_definition(id);

    auto resp = ppj::rjson_serialize_iobuf(
      co_await rq.service().schema_store().get_schema_subjects(id, incl_del));
    log_response(*rq.req, resp);
    rp.rep->write_body("json", ppj::as_body_writer(std::move(resp)));
    co_return rp;
}

ss::future<server::reply_t> get_subjects(
  server::request_t rq,
  server::reply_t rp,
  std::optional<request_auth_result> auth_result) {
    parse_accept_header(rq, rp);
    auto inc_del{
      parse::query_param<std::optional<include_deleted>>(*rq.req, "deleted")
        .value_or(include_deleted::no)};
    auto subject_prefix{
      parse::query_param<std::optional<ss::sstring>>(*rq.req, "subjectPrefix")};

    // List-type request: must ensure we see latest writes
    co_await rq.service().writer().read_sync();

    auto res = co_await rq.service().schema_store().get_subjects(
      inc_del, subject_prefix);

    // Handle AuthZ - Filters res for the subjects the user is allowed to see
    enterprise::handle_get_subjects_authz(rq, auth_result, res);

    auto resp = ppj::rjson_serialize_iobuf(std::move(res));
    log_response(*rq.req, resp);
    rp.rep->write_body("json", ppj::as_body_writer(std::move(resp)));
    co_return rp;
}

ss::future<server::reply_t>
get_subject_versions(server::request_t rq, server::reply_t rp) {
    parse_accept_header(rq, rp);
    auto sub = parse::request_param<subject>(*rq.req, "subject");
    auto inc_del{
      parse::query_param<std::optional<include_deleted>>(*rq.req, "deleted")
        .value_or(include_deleted::no)};

    // List-type request: must ensure we see latest writes
    co_await rq.service().writer().read_sync();

    auto versions = ppj::rjson_serialize_iobuf(
      co_await rq.service().schema_store().get_versions(sub, inc_del));

    log_response(*rq.req, versions);
    rp.rep->write_body("json", ppj::as_body_writer(std::move(versions)));
    co_return rp;
}

ss::future<server::reply_t>
post_subject(server::request_t rq, server::reply_t rp) {
    parse_content_type_header(rq);
    parse_accept_header(rq, rp);
    auto sub = parse::request_param<subject>(*rq.req, "subject");
    auto inc_del{
      parse::query_param<std::optional<include_deleted>>(*rq.req, "deleted")
        .value_or(include_deleted::no)};
    auto norm{parse::query_param<std::optional<normalize>>(*rq.req, "normalize")
                .value_or(normalize::no)};
    const auto format = parse_output_format(*rq.req);
    vlog(
      srlog.debug,
      "post_subject subject='{}', normalize='{}', deleted='{}', format='{}'",
      sub,
      norm,
      inc_del,
      format);
    // We must sync
    co_await rq.service().writer().read_sync();

    // Force 40401 if no subject
    co_await rq.service().schema_store().get_versions(sub, inc_del);

    subject_schema schema;
    try {
        auto unparsed = co_await rjson_parse(
          *rq.req, post_subject_versions_request_handler<>{sub});
        schema = co_await rq.service().schema_store().make_canonical_schema(
          std::move(unparsed.def), norm);
    } catch (const exception& e) {
        if (e.code() == error_code::schema_empty) {
            throw as_exception(invalid_subject_schema(sub));
        }
        throw;
    } catch (const ppj::parse_error&) {
        throw as_exception(invalid_subject_schema(sub));
    }

    auto sub_schema = co_await rq.service().schema_store().has_schema(
      std::move(schema), inc_del);

    auto [subject, def] = std::move(sub_schema.schema).destructure();
    auto formatted_schema = co_await rq.service().schema_store().format_schema(
      std::move(def), format);

    auto resp = ppj::rjson_serialize_iobuf(
      post_subject_versions_version_response{
        .schema{std::move(subject), std::move(formatted_schema)},
        .id{sub_schema.id},
        .version{sub_schema.version}});
    log_response(*rq.req, resp);
    rp.rep->write_body("json", ppj::as_body_writer(std::move(resp)));
    co_return rp;
}

ss::future<server::reply_t>
post_subject_versions(server::request_t rq, server::reply_t rp) {
    parse_content_type_header(rq);
    parse_accept_header(rq, rp);
    const auto sub = parse::request_param<subject>(*rq.req, "subject");
    const auto norm{
      parse::query_param<std::optional<normalize>>(*rq.req, "normalize")
        .value_or(normalize::no)};
    vlog(
      srlog.debug,
      "post_subject_versions subject='{}', normalize='{}'",
      sub,
      norm);

    auto& wr = rq.service().writer();
    auto& st = rq.service().schema_store();

    co_await wr.read_sync();

    auto unparsed = co_await rjson_parse(
      *rq.req, post_subject_versions_request_handler<>{sub});

    // If presented with a non-positive integer for version, set it to
    // invalid_schema_version so that the version number can be projected
    if (unparsed.version.has_value() && unparsed.version.value() < 1) {
        unparsed.version = invalid_schema_version;
    }

    // Upstream permits IDs of 0 on 'import'
    if (unparsed.id.has_value() && unparsed.id.value() < 0) {
        unparsed.id = invalid_schema_id;
    }

    stored_schema schema{
      co_await st.make_canonical_schema(std::move(unparsed.def), norm),
      unparsed.version.value_or(invalid_schema_version),
      unparsed.id.value_or(invalid_schema_id),
      is_deleted::no};

    // Validate the schema (may throw)
    co_await st.validate_schema(schema.schema.share());

    // Determine if the definition already exists
    auto s_id = co_await st.get_schema_id(schema.schema.def().share());

    vlog(
      srlog.debug, "post_subject_versions: ID for schema definition: {}", s_id);

    // Determine if the subject already has a version that references this
    // schema, deleted versions are not seen.
    const auto undeleted_versions = co_await st.get_subject_versions(
      sub, include_deleted::no);

    std::optional<schema_version> v_id;
    if (s_id.has_value()) {
        auto v_it = std::ranges::find(
          undeleted_versions, *s_id, &subject_version_entry::id);
        if (v_it != undeleted_versions.end()) {
            v_id.emplace(v_it->version);
        }
    }

    // Check if a match was found for the given request
    // Return the id if a match was found, register the schema if not
    const auto any_id_allowed = schema.id == invalid_schema_id;
    const auto id_matches = (any_id_allowed && s_id.has_value())
                            || schema.id == s_id;

    const auto any_version_allowed = schema.version == invalid_schema_version;
    const auto version_matches = (any_version_allowed && v_id.has_value())
                                 || schema.version == v_id;

    const auto matched = id_matches && version_matches;

    schema_id schema_id{s_id.value_or(invalid_schema_id)};
    if (!matched) {
        // Check if the request is appropriate for the mode
        const auto mode = co_await st.get_mode(sub, default_to_global::yes);
        if (mode == mode::read_only) {
            throw as_exception(mode_is_readonly(sub));
        }
        if (schema.id >= 0 && mode != mode::import) {
            throw as_exception(mode_not_import(schema.schema.sub()));
        }
        if (schema.id < 0 && mode != mode::read_write) {
            throw as_exception(mode_not_readwrite(sub));
        }

        // Determine if a provided schema id is appropriate
        if (
          schema.id != invalid_schema_id && s_id != schema.id
          && co_await st.has_schema(schema.id)) {
            // The supplied id already exists, but the schema is different
            co_return ss::coroutine::return_exception(
              as_exception(overwrite_schema_with_id_not_permitted(schema.id)));
        }

        // Check compatibility of the schema
        if (!undeleted_versions.empty() && mode != mode::import) {
            auto compat = co_await st.is_compatible(
              undeleted_versions.back().version,
              schema.schema.share(),
              verbose::yes);
            if (!compat.is_compat) {
                throw exception(
                  error_code::schema_incompatible,
                  fmt::format(
                    "Schema being registered is incompatible with an earlier "
                    "schema for subject \"{}\", details: [{}]",
                    sub,
                    fmt::join(compat.messages, ", ")));
            }
        }

        schema.id = (schema.id == invalid_schema_id)
                      ? s_id.value_or(invalid_schema_id)
                      : schema.id;

        schema_id = co_await wr.write_subject_version(std::move(schema));
    }

    auto resp = ppj::rjson_serialize_iobuf(
      post_subject_versions_response{.id{schema_id}});
    log_response(*rq.req, resp);
    rp.rep->write_body("json", ppj::as_body_writer(std::move(resp)));
    co_return rp;
}

ss::future<ctx_server<service>::reply_t> get_subject_versions_version(
  ctx_server<service>::request_t rq, ctx_server<service>::reply_t rp) {
    parse_accept_header(rq, rp);
    auto sub = parse::request_param<subject>(*rq.req, "subject");
    auto ver = parse::request_param<ss::sstring>(*rq.req, "version");
    auto inc_del{
      parse::query_param<std::optional<include_deleted>>(*rq.req, "deleted")
        .value_or(include_deleted::no)};
    const auto format = parse_output_format(*rq.req);

    co_await rq.service().writer().read_sync();

    auto version = parse_schema_version(ver).value();

    auto get_res = co_await get_or_load(rq, [&rq, sub, version, inc_del]() {
        return rq.service().schema_store().get_subject_schema(
          sub, version, inc_del);
    });

    auto [subject, def] = std::move(get_res.schema).destructure();
    auto formatted_schema = co_await rq.service().schema_store().format_schema(
      std::move(def), format);

    auto resp = ppj::rjson_serialize_iobuf(
      post_subject_versions_version_response{
        .schema = {std::move(subject), std::move(formatted_schema)},
        .id = get_res.id,
        .version = get_res.version});
    log_response(*rq.req, resp);
    rp.rep->write_body("json", ppj::as_body_writer(std::move(resp)));
    co_return rp;
}

ss::future<ctx_server<service>::reply_t> get_subject_versions_version_schema(
  ctx_server<service>::request_t rq, ctx_server<service>::reply_t rp) {
    parse_accept_header(rq, rp);
    auto sub = parse::request_param<subject>(*rq.req, "subject");
    auto ver = parse::request_param<ss::sstring>(*rq.req, "version");
    auto inc_del{
      parse::query_param<std::optional<include_deleted>>(*rq.req, "deleted")
        .value_or(include_deleted::no)};
    const auto format = parse_output_format(*rq.req);

    co_await rq.service().writer().read_sync();

    auto version = parse_schema_version(ver).value();

    auto get_res = co_await rq.service().schema_store().get_subject_schema(
      sub, version, inc_del);

    auto [_, def] = std::move(get_res.schema).destructure();
    auto formatted_schema = co_await rq.service().schema_store().format_schema(
      std::move(def), format);

    auto resp = std::move(formatted_schema).raw();
    log_response(*rq.req, resp);
    rp.rep->write_body("json", ppj::as_body_writer(std::move(resp)()));
    co_return rp;
}

ss::future<ctx_server<service>::reply_t>
get_subject_versions_version_referenced_by(
  ctx_server<service>::request_t rq, ctx_server<service>::reply_t rp) {
    parse_accept_header(rq, rp);
    auto sub = parse::request_param<subject>(*rq.req, "subject");
    auto ver = parse::request_param<ss::sstring>(*rq.req, "version");

    co_await rq.service().writer().read_sync();

    auto version = parse_schema_version(ver).value();

    auto references = ppj::rjson_serialize_iobuf(
      co_await rq.service().schema_store().referenced_by(sub, version));

    log_response(*rq.req, references);
    rp.rep->write_body("json", ppj::as_body_writer(std::move(references)));
    co_return rp;
}

ss::future<server::reply_t>
delete_subject(server::request_t rq, server::reply_t rp) {
    parse_accept_header(rq, rp);
    auto sub{parse::request_param<subject>(*rq.req, "subject")};
    auto permanent{
      parse::query_param<std::optional<permanent_delete>>(*rq.req, "permanent")
        .value_or(permanent_delete::no)};

    // Must see latest data to do a valid check of whether the
    // subject is already soft-deleted
    co_await rq.service().writer().read_sync();

    auto versions
      = permanent
          ? co_await rq.service().writer().delete_subject_permanent(
              sub, std::nullopt)
          : co_await rq.service().writer().delete_subject_impermanent(sub);

    auto resp = ppj::rjson_serialize_iobuf(std::move(versions));
    log_response(*rq.req, resp);
    rp.rep->write_body("json", ppj::as_body_writer(std::move(resp)));
    co_return rp;
}

ss::future<server::reply_t>
delete_subject_version(server::request_t rq, server::reply_t rp) {
    parse_accept_header(rq, rp);
    auto sub{parse::request_param<subject>(*rq.req, "subject")};
    auto ver = parse::request_param<ss::sstring>(*rq.req, "version");
    auto permanent{
      parse::query_param<std::optional<permanent_delete>>(*rq.req, "permanent")
        .value_or(permanent_delete::no)};

    // Must see latest data to know whether what we're deleting is the last
    // version
    co_await rq.service().writer().read_sync();

    auto version = invalid_schema_version;
    if (ver == "latest") {
        // Requests for 'latest' mean the latest which is not marked deleted
        // (Clearly this will never succeed for permanent=true -- calling
        //  with latest+permanent is a bad request per API docs)
        auto versions = co_await rq.service().schema_store().get_versions(
          sub, include_deleted::no);
        if (versions.empty()) {
            throw as_exception(not_found(sub, version));
        }
        version = versions.back();
    } else {
        version = parse_numerical_schema_version(ver).value();
    }

    // A permanent deletion emits tombstones for prior schema_key messages
    if (permanent) {
        co_await rq.service().writer().delete_subject_permanent(sub, version);
    } else {
        // Refuse to soft-delete the same thing twice
        if (co_await rq.service().schema_store().is_subject_version_deleted(
              sub, version)) {
            throw as_exception(soft_deleted(sub, version));
        }

        // Upsert the version with is_deleted=1
        co_await rq.service().writer().delete_subject_version(sub, version);
    }

    auto resp = ppj::rjson_serialize_iobuf(version);
    log_response(*rq.req, resp);
    rp.rep->write_body("json", ppj::as_body_writer(std::move(resp)));
    co_return rp;
}

ss::future<server::reply_t>
compatibility_subject_version(server::request_t rq, server::reply_t rp) {
    parse_content_type_header(rq);
    parse_accept_header(rq, rp);
    auto ver = parse::request_param<ss::sstring>(*rq.req, "version");
    auto sub = parse::request_param<subject>(*rq.req, "subject");
    auto is_verbose{
      parse::query_param<std::optional<verbose>>(*rq.req, "verbose")
        .value_or(verbose::no)};
    auto unparsed = co_await rjson_parse(
      *rq.req, post_subject_versions_request_handler<>{sub});

    // Must read, in case we have the subject in cache with an outdated config
    co_await rq.service().writer().read_sync();

    vlog(
      srlog.info,
      "compatibility_subject_version: subject: {}, version: {}",
      unparsed.def.sub(),
      ver);
    auto version = invalid_schema_version;
    if (ver == "latest") {
        auto versions = co_await rq.service().schema_store().get_versions(
          unparsed.def.sub(), include_deleted::no);
        if (versions.empty()) {
            throw as_exception(not_found(unparsed.def.sub(), version));
        }
        version = versions.back();
    } else {
        version = parse_numerical_schema_version(ver).value();
    }

    subject_schema schema;
    try {
        schema = co_await rq.service().schema_store().make_canonical_schema(
          std::move(unparsed.def));
    } catch (exception& e) {
        constexpr auto reportable = [](std::error_code ec) {
            constexpr std::array errors{
              error_code::schema_invalid,
              error_code::schema_empty,
              error_code::schema_missing_reference};
            return std::ranges::any_of(
              errors, [ec](error_code e) { return ec == e; });
        };
        if (is_verbose && reportable(e.code())) {
            auto resp = ppj::rjson_serialize_iobuf(post_compatibility_res{
              .is_compat = false,
              .messages = {e.message()},
              .is_verbose = is_verbose,
            });
            log_response(*rq.req, resp);
            rp.rep->write_body("json", json::as_body_writer(std::move(resp)));
            co_return rp;
        }
        throw;
    }

    auto get_res = co_await get_or_load(
      rq, [&rq, schema{std::move(schema)}, version, is_verbose]() {
          return rq.service().schema_store().is_compatible(
            version, schema.share(), is_verbose);
      });

    auto resp = ppj::rjson_serialize_iobuf(post_compatibility_res{
      .is_compat = get_res.is_compat,
      .messages = std::move(get_res.messages),
      .is_verbose = is_verbose,
    });
    log_response(*rq.req, resp);
    rp.rep->write_body("json", json::as_body_writer(std::move(resp)));
    co_return rp;
}

ss::future<server::reply_t>
status_ready(server::request_t rq, server::reply_t rp) {
    co_await rq.service().writer().read_sync();
    rp.rep->set_status(ss::http::reply::status_type::ok);
    co_return rp;
}

namespace {
void check_feature_ready(const server::request_t& rq) {
    constexpr auto feature = features::feature::schema_registry_authz;
    const auto& ft = rq.service().controller()->get_feature_table().local();
    if (!ft.is_active(feature)) {
        throw exception(
          error_code::internal_server_error,
          fmt::format("Feature '{}' is not yet available", feature));
    }
}

void check_licence(const server::request_t& rq) {
    const auto& ft = rq.service().controller()->get_feature_table().local();
    if (ft.should_sanction()) {
        const auto& license = ft.get_license();
        auto status = [&license]() {
            return !license.has_value()    ? "not present"
                   : license->is_expired() ? "expired"
                                           : "unknown error";
        };
        throw ss::httpd::base_exception(
          fmt::format("Invalid license: {}", status()),
          ss::http::reply::status_type::forbidden);
    }
}

} // namespace

ss::future<server::reply_t>
get_security_acls(server::request_t rq, server::reply_t rp) {
    auto& acl_store
      = rq.service().controller()->get_authorizer().local().store();

    auto parse_and_convert = [&](
                               const std::string& param_name, auto converter) {
        auto str_value = parse::query_param<std::optional<ss::sstring>>(
          *rq.req, param_name);
        return str_value ? std::make_optional(converter(*str_value))
                         : std::nullopt;
    };

    auto resource = parse::query_param<std::optional<ss::sstring>>(
      *rq.req, "resource");

    auto principal = parse_and_convert("principal", to_acl_principal);
    auto resource_type = parse_and_convert("resource_type", to_resource_type);
    auto pattern_type = parse_and_convert("pattern_type", to_pattern_type);
    auto operation = parse_and_convert("operation", to_acl_operation);
    auto permission = parse_and_convert("permission", to_acl_permission);
    auto host = parse_and_convert("host", to_acl_host);

    auto filter = security::acl_binding_filter{
      security::resource_pattern_filter{
        resource_type,
        resource,
        pattern_type,
        security::resource_pattern_filter::resource_subsystem::schema_registry},
      security::acl_entry_filter{principal, host, operation, permission}};

    auto sr_acls = std::ranges::to<chunked_vector<acl>>(
      acl_store.acls(filter)
      | std::views::transform(
        [](const security::acl_binding& binding) { return acl(binding); }));

    auto resp = ppj::rjson_serialize_iobuf(std::move(sr_acls));

    rp.rep->write_body("json", json::as_body_writer(std::move(resp)));
    co_return rp;
}

ss::future<server::reply_t>
post_security_acls(server::request_t rq, server::reply_t rp) {
    check_licence(rq);

    auto& security_frontend
      = rq.service().controller()->get_security_frontend().local();

    auto raw_acls = co_await rjson_parse(
      *rq.req, acl_handler<>{acl_handler<>::require_fields::yes});

    std::vector<security::acl_binding> bindings;
    bindings.reserve(raw_acls.size());

    for (const auto& acl : raw_acls) {
        if (
          acl.pattern_type == security::pattern_type::prefixed
          && acl.resource_type != security::resource_type::sr_subject) {
            throw exception(
              error_code::acl_invalid,
              "Pattern type 'prefixed' can only be used with resource type "
              "'subject'");
        }

        bindings.emplace_back(
          security::resource_pattern{
            *acl.resource_type, *acl.resource, *acl.pattern_type},
          security::acl_entry{
            *acl.principal, *acl.host, *acl.operation, *acl.permission});
    }

    check_feature_ready(rq);

    auto err_vec = co_await security_frontend.create_acls(bindings, 5s);

    auto it = std::find_if(err_vec.begin(), err_vec.end(), [](const auto& err) {
        return err != cluster::errc::success;
    });

    if (it != err_vec.end()) {
        throw exception(
          error_code::internal_server_error,
          fmt::format(
            "Failed to create ACLs: {}",
            cluster::make_error_code(*it).message()));
    }

    rp.rep->set_status(ss::http::reply::status_type::created);
    co_return rp;
}

ss::future<server::reply_t>
delete_security_acls(server::request_t rq, server::reply_t rp) {
    check_licence(rq);

    auto& security_frontend
      = rq.service().controller()->get_security_frontend().local();

    auto raw_acls = co_await rjson_parse(
      *rq.req, acl_handler<>{acl_handler<>::require_fields::no});

    std::vector<security::acl_binding_filter> filters;
    filters.reserve(raw_acls.size());

    for (const auto& acl : raw_acls) {
        filters.emplace_back(
          security::resource_pattern_filter{
            acl.resource_type,
            acl.resource,
            acl.pattern_type,
            security::resource_pattern_filter::resource_subsystem::
              schema_registry},
          security::acl_entry_filter{
            acl.principal, acl.host, acl.operation, acl.permission});
    }

    check_feature_ready(rq);

    auto deleted = co_await security_frontend.delete_acls(
      std::move(filters), 5s);

    auto res = chunked_vector<acl>{};
    std::ranges::for_each(deleted, [&res](cluster::delete_acls_result r) {
        if (r.error != cluster::errc::success) {
            throw exception(
              error_code::internal_server_error,
              fmt::format(
                "Failed to delete ACLs: {}",
                cluster::make_error_code(r.error).message()));
        }
        std::ranges::transform(
          r.bindings, std::back_inserter(res), [](const auto& b) {
              return acl(b);
          });
    });

    auto resp = ppj::rjson_serialize_iobuf(std::move(res));

    rp.rep->write_body("json", json::as_body_writer(std::move(resp)));
    co_return rp;
}

} // namespace pandaproxy::schema_registry

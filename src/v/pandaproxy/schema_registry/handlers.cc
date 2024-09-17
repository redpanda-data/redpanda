// Copyright 2021 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "handlers.h"

#include "container/json.h"
#include "pandaproxy/json/rjson_util.h"
#include "pandaproxy/json/types.h"
#include "pandaproxy/parsing/httpd.h"
#include "pandaproxy/schema_registry/error.h"
#include "pandaproxy/schema_registry/errors.h"
#include "pandaproxy/schema_registry/requests/compatibility.h"
#include "pandaproxy/schema_registry/requests/config.h"
#include "pandaproxy/schema_registry/requests/get_schemas_ids_id.h"
#include "pandaproxy/schema_registry/requests/get_schemas_ids_id_versions.h"
#include "pandaproxy/schema_registry/requests/get_subject_versions_version.h"
#include "pandaproxy/schema_registry/requests/mode.h"
#include "pandaproxy/schema_registry/requests/post_subject_versions.h"
#include "pandaproxy/schema_registry/types.h"
#include "pandaproxy/server.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/future.hh>
#include <seastar/core/sstring.hh>

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

ss::future<server::reply_t>
get_config(server::request_t rq, server::reply_t rp) {
    parse_accept_header(rq, rp);
    rq.req.reset();

    // Ensure we see latest writes
    co_await rq.service().writer().read_sync();

    auto res = co_await rq.service().schema_store().get_compatibility();

    rp.rep->write_body(
      "json", ppj::rjson_serialize(get_config_req_rep{.compat = res}));
    co_return rp;
}

ss::future<server::reply_t>
put_config(server::request_t rq, server::reply_t rp) {
    parse_content_type_header(rq);
    parse_accept_header(rq, rp);
    auto config = co_await ppj::rjson_parse(
      std::move(rq.req), put_config_handler<>{});

    co_await rq.service().writer().write_config(std::nullopt, config.compat);

    rp.rep->write_body("json", ppj::rjson_serialize(config));
    co_return rp;
}

ss::future<server::reply_t>
get_config_subject(server::request_t rq, server::reply_t rp) {
    parse_accept_header(rq, rp);
    auto sub = parse::request_param<subject>(*rq.req, "subject");
    auto fallback = parse::query_param<std::optional<default_to_global>>(
                      *rq.req, "defaultToGlobal")
                      .value_or(default_to_global::no);
    rq.req.reset();

    // Ensure we see latest writes
    co_await rq.service().writer().read_sync();

    auto res = co_await rq.service().schema_store().get_compatibility(
      sub, fallback);

    rp.rep->write_body(
      "json", ppj::rjson_serialize(get_config_req_rep{.compat = res}));
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
    vlog(plog.debug, "get_or_load: refreshing schema store on missing item");
    co_await rq.service().writer().read_sync();
    co_return co_await f();
}

ss::future<server::reply_t>
put_config_subject(server::request_t rq, server::reply_t rp) {
    parse_content_type_header(rq);
    parse_accept_header(rq, rp);
    auto sub = parse::request_param<subject>(*rq.req, "subject");
    auto config = co_await ppj::rjson_parse(
      std::move(rq.req), put_config_handler<>{});

    // Ensure we see latest writes
    co_await rq.service().writer().read_sync();
    co_await rq.service().writer().write_config(sub, config.compat);

    rp.rep->write_body("json", ppj::rjson_serialize(config));
    co_return rp;
}

ss::future<server::reply_t>
delete_config_subject(server::request_t rq, server::reply_t rp) {
    parse_accept_header(rq, rp);
    auto sub = parse::request_param<subject>(*rq.req, "subject");

    rq.req.reset();

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

    rp.rep->write_body(
      "json", ppj::rjson_serialize(get_config_req_rep{.compat = lvl}));
    co_return rp;
}

ss::future<server::reply_t> get_mode(server::request_t rq, server::reply_t rp) {
    parse_accept_header(rq, rp);
    rq.req.reset();

    // Ensure we see latest writes
    co_await rq.service().writer().read_sync();

    auto res = co_await rq.service().schema_store().get_mode();

    rp.rep->write_body("json", ppj::rjson_serialize(mode_req_rep{.mode = res}));
    co_return rp;
}

ss::future<server::reply_t> put_mode(server::request_t rq, server::reply_t rp) {
    parse_content_type_header(rq);
    parse_accept_header(rq, rp);
    auto frc = parse::query_param<std::optional<force>>(*rq.req, "force")
                 .value_or(force::no);
    auto res = co_await ppj::rjson_parse(std::move(rq.req), mode_handler<>{});

    co_await rq.service().writer().write_mode(std::nullopt, res.mode, frc);

    rp.rep->write_body("json", ppj::rjson_serialize(res));
    co_return rp;
}

ss::future<server::reply_t>
get_mode_subject(server::request_t rq, server::reply_t rp) {
    parse_accept_header(rq, rp);
    auto sub = parse::request_param<subject>(*rq.req, "subject");
    auto fallback = parse::query_param<std::optional<default_to_global>>(
                      *rq.req, "defaultToGlobal")
                      .value_or(default_to_global::no);
    rq.req.reset();

    // Ensure we see latest writes
    co_await rq.service().writer().read_sync();

    auto res = co_await rq.service().schema_store().get_mode(sub, fallback);

    rp.rep->write_body("json", ppj::rjson_serialize(mode_req_rep{.mode = res}));
    co_return rp;
}

ss::future<server::reply_t>
put_mode_subject(server::request_t rq, server::reply_t rp) {
    parse_content_type_header(rq);
    parse_accept_header(rq, rp);
    auto frc = parse::query_param<std::optional<force>>(*rq.req, "force")
                 .value_or(force::no);
    auto sub = parse::request_param<subject>(*rq.req, "subject");
    auto res = co_await ppj::rjson_parse(std::move(rq.req), mode_handler<>{});

    // Ensure we see latest writes
    co_await rq.service().writer().read_sync();
    co_await rq.service().writer().write_mode(sub, res.mode, frc);

    rp.rep->write_body("json", ppj::rjson_serialize(res));
    co_return rp;
}

ss::future<server::reply_t>
delete_mode_subject(server::request_t rq, server::reply_t rp) {
    parse_accept_header(rq, rp);
    auto sub = parse::request_param<subject>(*rq.req, "subject");

    rq.req.reset();

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

    rp.rep->write_body("json", ppj::rjson_serialize(mode_req_rep{.mode = m}));
    co_return rp;
}

ss::future<server::reply_t>
get_schemas_types(server::request_t rq, server::reply_t rp) {
    parse_accept_header(rq, rp);
    rq.req.reset();

    static const std::vector<std::string_view> schemas_types{
      "JSON", "PROTOBUF", "AVRO"};
    rp.rep->write_body("json", ppj::rjson_serialize(schemas_types));
    return ss::make_ready_future<server::reply_t>(std::move(rp));
}

ss::future<server::reply_t>
get_schemas_ids_id(server::request_t rq, server::reply_t rp) {
    parse_accept_header(rq, rp);
    auto id = parse::request_param<schema_id>(*rq.req, "id");
    rq.req.reset();

    auto def = co_await get_or_load(rq, [&rq, id]() {
        return rq.service().schema_store().get_schema_definition(id);
    });

    rp.rep->write_body(
      "json",
      ppj::rjson_serialize(
        get_schemas_ids_id_response{.definition{std::move(def)}}));
    co_return rp;
}

ss::future<server::reply_t>
get_schemas_ids_id_versions(server::request_t rq, server::reply_t rp) {
    parse_accept_header(rq, rp);
    auto id = parse::request_param<schema_id>(*rq.req, "id");
    rq.req.reset();

    // List-type request: must ensure we see latest writes
    co_await rq.service().writer().read_sync();

    // Force early 40403 if the schema id isn't found
    co_await rq.service().schema_store().get_schema_definition(id);

    auto svs = co_await rq.service().schema_store().get_schema_subject_versions(
      id);

    rp.rep->write_body(
      "json",
      ppj::rjson_serialize(get_schemas_ids_id_versions_response{
        .subject_versions{std::move(svs)}}));
    co_return rp;
}

ss::future<ctx_server<service>::reply_t> get_schemas_ids_id_subjects(
  ctx_server<service>::request_t rq, ctx_server<service>::reply_t rp) {
    parse_accept_header(rq, rp);
    auto id = parse::request_param<schema_id>(*rq.req, "id");
    auto incl_del{
      parse::query_param<std::optional<include_deleted>>(*rq.req, "deleted")
        .value_or(include_deleted::no)};
    rq.req.reset();

    // List-type request: must ensure we see latest writes
    co_await rq.service().writer().read_sync();

    // Force early 40403 if the schema id isn't found
    co_await rq.service().schema_store().get_schema_definition(id);

    rp.rep->write_body(
      "json",
      ppj::rjson_serialize(
        co_await rq.service().schema_store().get_schema_subjects(
          id, incl_del)));
    co_return rp;
}

ss::future<server::reply_t>
get_subjects(server::request_t rq, server::reply_t rp) {
    parse_accept_header(rq, rp);
    auto inc_del{
      parse::query_param<std::optional<include_deleted>>(*rq.req, "deleted")
        .value_or(include_deleted::no)};
    auto subject_prefix{
      parse::query_param<std::optional<ss::sstring>>(*rq.req, "subjectPrefix")};
    rq.req.reset();

    // List-type request: must ensure we see latest writes
    co_await rq.service().writer().read_sync();

    rp.rep->write_body(
      "json",
      json::rjson_serialize(co_await rq.service().schema_store().get_subjects(
        inc_del, subject_prefix)));
    co_return rp;
}

ss::future<server::reply_t>
get_subject_versions(server::request_t rq, server::reply_t rp) {
    parse_accept_header(rq, rp);
    auto sub = parse::request_param<subject>(*rq.req, "subject");
    auto inc_del{
      parse::query_param<std::optional<include_deleted>>(*rq.req, "deleted")
        .value_or(include_deleted::no)};
    rq.req.reset();

    // List-type request: must ensure we see latest writes
    co_await rq.service().writer().read_sync();

    auto versions = co_await rq.service().schema_store().get_versions(
      sub, inc_del);

    rp.rep->write_body("json", ppj::rjson_serialize(versions));
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
    vlog(
      plog.debug,
      "post_subject subject='{}', normalize='{}', deleted='{}'",
      sub,
      norm,
      inc_del);
    // We must sync
    co_await rq.service().writer().read_sync();

    // Force 40401 if no subject
    co_await rq.service().schema_store().get_versions(sub, inc_del);

    canonical_schema schema;
    try {
        auto unparsed = co_await ppj::rjson_parse(
          std::move(rq.req), post_subject_versions_request_handler<>{sub});
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

    rp.rep->write_body(
      "json",
      ppj::rjson_serialize(post_subject_versions_version_response{
        .schema{std::move(sub_schema.schema)},
        .id{sub_schema.id},
        .version{sub_schema.version}}));
    co_return rp;
}

ss::future<server::reply_t>
post_subject_versions(server::request_t rq, server::reply_t rp) {
    parse_content_type_header(rq);
    parse_accept_header(rq, rp);
    auto sub = parse::request_param<subject>(*rq.req, "subject");
    auto norm{parse::query_param<std::optional<normalize>>(*rq.req, "normalize")
                .value_or(normalize::no)};
    vlog(
      plog.debug,
      "post_subject_versions subject='{}', normalize='{}'",
      sub,
      norm);

    co_await rq.service().writer().read_sync();

    auto unparsed = co_await ppj::rjson_parse(
      std::move(rq.req), post_subject_versions_request_handler<>{sub});

    subject_schema schema{
      co_await rq.service().schema_store().make_canonical_schema(
        std::move(unparsed.def), norm),
      unparsed.version.value_or(invalid_schema_version),
      unparsed.id.value_or(invalid_schema_id),
      is_deleted::no};

    auto ids = co_await rq.service().schema_store().get_schema_version(
      schema.share());

    schema_id schema_id{ids.id.value_or(invalid_schema_id)};
    if (!ids.version.has_value()) {
        schema.id = ids.id.value_or(invalid_schema_id);
        schema.version = schema.version == invalid_schema_version
                           ? ids.version.value_or(invalid_schema_version)
                           : schema.version;
        schema_id = co_await rq.service().writer().write_subject_version(
          std::move(schema));
    }

    rp.rep->write_body(
      "json",
      ppj::rjson_serialize(post_subject_versions_response{.id{schema_id}}));
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
    rq.req.reset();

    co_await rq.service().writer().read_sync();

    auto version = parse_schema_version(ver).value();

    auto get_res = co_await get_or_load(rq, [&rq, sub, version, inc_del]() {
        return rq.service().schema_store().get_subject_schema(
          sub, version, inc_del);
    });

    rp.rep->write_body(
      "json",
      ppj::rjson_serialize(post_subject_versions_version_response{
        .schema = std::move(get_res.schema),
        .id = get_res.id,
        .version = get_res.version}));
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
    rq.req.reset();

    co_await rq.service().writer().read_sync();

    auto version = parse_schema_version(ver).value();

    auto get_res = co_await rq.service().schema_store().get_subject_schema(
      sub, version, inc_del);

    rp.rep->write_body(
      "json", ppj::as_body_writer(std::move(get_res.schema).def().raw()()));
    co_return rp;
}

ss::future<ctx_server<service>::reply_t>
get_subject_versions_version_referenced_by(
  ctx_server<service>::request_t rq, ctx_server<service>::reply_t rp) {
    parse_accept_header(rq, rp);
    auto sub = parse::request_param<subject>(*rq.req, "subject");
    auto ver = parse::request_param<ss::sstring>(*rq.req, "version");
    rq.req.reset();

    co_await rq.service().writer().read_sync();

    auto version = parse_schema_version(ver).value();

    auto references = co_await rq.service().schema_store().referenced_by(
      sub, version);

    rp.rep->write_body("json", ppj::rjson_serialize(std::move(references)));
    co_return rp;
}

ss::future<server::reply_t>
delete_subject(server::request_t rq, server::reply_t rp) {
    parse_accept_header(rq, rp);
    auto sub{parse::request_param<subject>(*rq.req, "subject")};
    auto permanent{
      parse::query_param<std::optional<permanent_delete>>(*rq.req, "permanent")
        .value_or(permanent_delete::no)};
    rq.req.reset();

    // Must see latest data to do a valid check of whether the
    // subject is already soft-deleted
    co_await rq.service().writer().read_sync();

    auto versions
      = permanent
          ? co_await rq.service().writer().delete_subject_permanent(
              sub, std::nullopt)
          : co_await rq.service().writer().delete_subject_impermanent(sub);

    rp.rep->write_body("json", ppj::rjson_serialize(versions));
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
    rq.req.reset();

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

    rp.rep->write_body("json", ppj::rjson_serialize(version));
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
    auto unparsed = co_await ppj::rjson_parse(
      std::move(rq.req), post_subject_versions_request_handler<>{sub});

    // Must read, in case we have the subject in cache with an outdated config
    co_await rq.service().writer().read_sync();

    vlog(
      plog.info,
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

    canonical_schema schema;
    try {
        schema = co_await rq.service().schema_store().make_canonical_schema(
          std::move(unparsed.def));
    } catch (exception& e) {
        constexpr auto reportable = [](std::error_code ec) {
            constexpr std::array errors{
              error_code::schema_invalid, error_code::schema_empty};
            return absl::c_any_of(
              errors, [ec](error_code e) { return ec == e; });
        };
        if (is_verbose && reportable(e.code())) {
            rp.rep->write_body(
              "json",
              json::rjson_serialize(post_compatibility_res{
                .is_compat = false,
                .messages = {e.message()},
                .is_verbose = is_verbose,
              }));
            co_return rp;
        }
        throw;
    }

    auto get_res = co_await get_or_load(
      rq, [&rq, schema{std::move(schema)}, version, is_verbose]() {
          return rq.service().schema_store().is_compatible(
            version, schema.share(), is_verbose);
      });

    rp.rep->write_body(
      "json",
      json::rjson_serialize(post_compatibility_res{
        .is_compat = get_res.is_compat,
        .messages = std::move(get_res.messages),
        .is_verbose = is_verbose,
      }));
    co_return rp;
}

ss::future<server::reply_t>
status_ready(server::request_t rq, server::reply_t rp) {
    co_await rq.service().writer().read_sync();
    rp.rep->set_status(ss::http::reply::status_type::ok);
    co_return rp;
}

} // namespace pandaproxy::schema_registry

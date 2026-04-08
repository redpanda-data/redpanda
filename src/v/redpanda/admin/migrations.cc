/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */
#include "base/vlog.h"
#include "cluster/controller.h"
#include "cluster/data_migration_frontend.h"
#include "cluster/data_migration_irpc_frontend.h"
#include "cluster/data_migration_types.h"
#include "cluster/offsets_snapshot.h"
#include "json/document.h"
#include "json/validator.h"
#include "json/writer.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "model/namespace.h"
#include "model/timestamp.h"
#include "redpanda/admin/api-doc/migration.json.hh"
#include "redpanda/admin/data_migration_utils.h"
#include "redpanda/admin/server.h"
#include "redpanda/admin/util.h"
#include "ssx/async_algorithm.h"

#include <seastar/core/sstring.hh>
#include <seastar/http/exception.hh>
#include <seastar/http/reply.hh>
#include <seastar/http/request.hh>
#include <seastar/json/json_elements.hh>
#include <seastar/util/variant_utils.hh>

#include <fmt/core.h>

using admin::apply_validator;

namespace {

ss::httpd::migration_json::namespaced_topic
to_admin_type(const model::topic_namespace& tp_ns) {
    ss::httpd::migration_json::namespaced_topic ret;
    ret.ns = tp_ns.ns;
    ret.topic = tp_ns.tp;
    return ret;
}

ss::httpd::migration_json::namespaced_topic to_admin_type(
  const model::topic_namespace& tp_ns,
  const std::optional<cluster::data_migrations::cloud_storage_location>&
    cloud_storage_location) {
    ss::httpd::migration_json::namespaced_topic ret;
    ret.ns = tp_ns.ns;
    ret.topic = cloud_storage_location
                  ? ss::sstring(
                      fmt::format(
                        "{}/{}", tp_ns.tp, cloud_storage_location->hint))
                  : tp_ns.tp;
    return ret;
}

ss::httpd::migration_json::outbound_topic to_admin_type(
  const model::topic_namespace& tp_ns,
  const cluster::data_migrations::topic_location* tp_loc) {
    ss::httpd::migration_json::outbound_topic ret;
    ret.ns = tp_ns.ns;
    ret.topic = tp_ns.tp;
    if (tp_loc && tp_loc->location) {
        ret.remote_location = ssx::sformat(
          "{}/{}", tp_loc->remote_topic.tp, tp_loc->location->hint);
    }
    return ret;
}

ss::httpd::migration_json::inbound_migration
to_admin_type(const cluster::data_migrations::inbound_migration& idm) {
    ss::httpd::migration_json::inbound_migration migration;
    using migration_type_enum = ss::httpd::migration_json::inbound_migration::
      inbound_migration_migration_type;
    migration.migration_type = migration_type_enum::inbound;
    for (auto& inbound_t : idm.topics) {
        ss::httpd::migration_json::inbound_topic inbound_tp;
        inbound_tp.source_topic_reference = to_admin_type(
          inbound_t.source_topic_name, inbound_t.cloud_storage_location);
        if (inbound_t.alias) {
            inbound_tp.alias = to_admin_type(*inbound_t.alias);
        }
        migration.topics.push(inbound_tp);
    }
    for (auto& cg : idm.groups) {
        migration.consumer_groups.push(cg);
    }
    migration.auto_advance = idm.auto_advance;
    return migration;
}

ss::httpd::migration_json::outbound_migration
to_admin_type(const cluster::data_migrations::outbound_migration& odm) {
    ss::httpd::migration_json::outbound_migration migration;
    using migration_type_enum = ss::httpd::migration_json::outbound_migration::
      outbound_migration_migration_type;
    migration.migration_type = migration_type_enum::outbound;

    if (!(odm.topic_locations.empty()
          || odm.topic_locations.size() == odm.topics.size())) {
        // topic_locations should be either empty (written by pre-v25.2
        // redpanda) or have the same size as topics
        throw std::runtime_error(
          fmt::format(
            "unexpected migration topic locations size: {}, expected: {}",
            odm.topic_locations.size(),
            odm.topics.size()));
    }

    for (size_t i = 0; i < odm.topics.size(); ++i) {
        const auto* loc = !odm.topic_locations.empty() ? &odm.topic_locations[i]
                                                       : nullptr;
        migration.topics.push(to_admin_type(odm.topics[i], loc));
    }
    for (auto& cg : odm.groups) {
        migration.consumer_groups.push(cg);
    }
    migration.auto_advance = odm.auto_advance;
    return migration;
}

template<class Migration>
struct StateAdmin;

template<>
struct StateAdmin<cluster::data_migrations::inbound_migration> {
    using type = ss::httpd::migration_json::inbound_migration_state;
};

template<>
struct StateAdmin<cluster::data_migrations::outbound_migration> {
    using type = ss::httpd::migration_json::outbound_migration_state;
};

template<class Migration>
auto to_admin_type(
  const Migration& migration,
  const cluster::data_migrations::migration_metadata& meta) {
    typename StateAdmin<Migration>::type ret;
    ret.id = meta.id;
    ret.state = fmt::to_string(meta.state);
    ret.migration = to_admin_type(migration);
    ret.created_timestamp = meta.created_timestamp.value();
    if (meta.completed_timestamp != model::timestamp::missing()) {
        ret.completed_timestamp = meta.completed_timestamp.value();
    }
    return ret;
}
void write_migration_as_json(
  const cluster::data_migrations::migration_metadata& meta,
  json::Writer<json::StringBuffer>& writer) {
    ss::visit(meta.migration, [&writer, &meta](auto& migration) {
        auto json_str = to_admin_type(migration, meta).to_json();
        writer.RawValue(
          json_str.c_str(), json_str.size(), rapidjson::Type::kObjectType);
    });
}

json::validator make_migration_validator() {
    const std::string_view schema = R"(
{
    "$schema": "http://json-schema.org/draft-04/schema#",
    "type": "object",
    "anyOf": [
        {
            "properties": {
                "migration_type": {
                    "description": "Migration type",
                    "type": "string",
                    "enum": ["inbound"]
                },
                "topics": {
                    "description": "Topics to migrate",
                    "type": "array",
                    "items": {
                        "$ref": "#/definitions/inbound_topic"
                    }
                },
                "consumer_groups": {
                    "description": "Groups to migrate",
                    "type": "array",
                    "items": {
                      "type":"string"
                    }
                }
            },
            "required": [
                "migration_type",
                "topics",
                "consumer_groups"
            ],
            "additionalProperties": false
        },
        {
            "properties": {
                "migration_type": {
                    "description": "Migration type",
                    "type": "string",
                    "enum": ["outbound"]
                },
                "topics": {
                    "description": "Topics to migrate",
                    "type": "array",
                    "items": {
                        "$ref": "#/definitions/namespaced_topic"
                    }
                },
                "consumer_groups": {
                    "description": "Groups to migrate",
                    "type": "array",
                    "items": {
                      "type":"string"
                    }
                }
            },
            "additionalProperties": false,
            "required": [
                "topics",
                "consumer_groups",
                "migration_type"
            ]
        }
    ],
    "definitions": {
        "namespaced_topic": {
            "type": "object",
            "required": [
                "topic"
            ],
            "properties": {
                "topic": {
                    "type": "string"
                },
                "ns": {
                    "type": "string"
                }
            },
            "additionalProperties": false
        },
        "inbound_topic": {
            "type": "object",
            "required": [
                "source_topic_reference"
            ],
            "properties": {
                "source_topic_reference": {
                    "$ref": "#/definitions/namespaced_topic"
                },
                "alias": {
                    "$ref": "#/definitions/namespaced_topic"
                }
            },
            "additionalProperties": false
        }
    }
}
  )";
    return json::validator(schema);
}

cluster::data_migrations::inbound_migration
parse_inbound_data_migration(json::Value& json) {
    cluster::data_migrations::inbound_migration ret;
    ret.topics = parse_inbound_topics(json);
    auto consumer_groups_array = json["consumer_groups"].GetArray();
    ret.groups.reserve(consumer_groups_array.Size());
    for (auto& group : consumer_groups_array) {
        ret.groups.emplace_back(group.GetString());
    }
    return ret;
}

cluster::data_migrations::outbound_migration
parse_outbound_data_migration(json::Value& json) {
    cluster::data_migrations::outbound_migration ret;
    ret.topics = parse_topics(json);

    auto consumer_groups_array = json["consumer_groups"].GetArray();
    ret.groups.reserve(consumer_groups_array.Size());
    for (auto& group : consumer_groups_array) {
        ret.groups.emplace_back(group.GetString());
    }
    return ret;
}

cluster::data_migrations::data_migration
parse_add_migration_request(json::Document& json_doc) {
    static thread_local json::validator validator = make_migration_validator();

    static constexpr std::string_view inbound = "inbound";
    static constexpr std::string_view outbound = "outbound";

    apply_validator(validator, json_doc);
    auto type = json_doc["migration_type"].GetString();
    if (type == inbound) {
        return parse_inbound_data_migration(json_doc);
    } else if (type == outbound) {
        return parse_outbound_data_migration(json_doc);
    } else {
        throw ss::httpd::bad_request_exception(
          ssx::sformat("unknown migration type: {}", type));
    }
}

cluster::data_migrations::id parse_data_migration_id(ss::http::request& req) {
    auto id_str = req.get_path_param("id");
    try {
        return cluster::data_migrations::id(
          boost::lexical_cast<cluster::data_migrations::id::type>(id_str));
    } catch (const boost::bad_lexical_cast& e) {
        throw ss::httpd::bad_param_exception(e.what());
    }
}
cluster::data_migrations::state parse_migration_action(ss::http::request& req) {
    auto action_str = req.get_query_param("action");
    if (action_str == "prepare") {
        return cluster::data_migrations::state::preparing;
    } else if (action_str == "execute") {
        return cluster::data_migrations::state::executing;
    } else if (action_str == "cancel") {
        return cluster::data_migrations::state::canceling;
    } else if (action_str == "finish") {
        return cluster::data_migrations::state::cut_over;
    }
    throw ss::httpd::bad_param_exception(
      fmt::format("unknown data migration action: {}", action_str));
}

} // namespace

void admin_server::register_data_migration_routes() {
    register_route_raw_async<superuser>(
      ss::httpd::migration_json::list_migrations,
      [this](
        std::unique_ptr<ss::http::request> req,
        std::unique_ptr<ss::http::reply> reply) {
          return list_data_migrations(std::move(req), std::move(reply));
      });
    register_route_raw_async<superuser>(
      ss::httpd::migration_json::get_migration,
      [this](
        std::unique_ptr<ss::http::request> req,
        std::unique_ptr<ss::http::reply> reply) {
          return get_data_migration(std::move(req), std::move(reply));
      });
    register_route_raw_async<superuser>(
      ss::httpd::migration_json::add_migration,
      [this](
        std::unique_ptr<ss::http::request> req,
        std::unique_ptr<ss::http::reply> reply) {
          return add_data_migration(std::move(req), std::move(reply));
      });
    register_route<superuser>(
      ss::httpd::migration_json::execute_migration_action,
      [this](std::unique_ptr<ss::http::request> req) {
          return execute_migration_action(std::move(req));
      });
    register_route<superuser>(
      ss::httpd::migration_json::delete_migration,
      [this](std::unique_ptr<ss::http::request> req) {
          return delete_migration(std::move(req));
      });
    register_route_raw_async<superuser>(
      ss::httpd::migration_json::get_migrated_entities_status,
      [this](
        std::unique_ptr<ss::http::request> req,
        std::unique_ptr<ss::http::reply> reply) {
          return get_migrated_entities_status(std::move(req), std::move(reply));
      });
    register_route_raw_async<superuser>(
      ss::httpd::migration_json::set_migrated_entities_status,
      [this](
        std::unique_ptr<ss::http::request> req,
        std::unique_ptr<ss::http::reply> reply) {
          return set_migrated_entities_status(std::move(req), std::move(reply));
      });
}

ss::future<std::unique_ptr<ss::http::reply>> admin_server::list_data_migrations(
  std::unique_ptr<ss::http::request> req,
  std::unique_ptr<ss::http::reply> reply) {
    auto& frontend = _controller->get_data_migration_frontend();
    auto maybe_migrations = co_await frontend.local().list_migrations();
    if (maybe_migrations.has_error()) {
        co_await admin_server::throw_on_error(
          *req, maybe_migrations.assume_error(), model::controller_ntp);
    }
    auto& migrations = maybe_migrations.assume_value();
    json::StringBuffer buf;
    json::Writer<json::StringBuffer> writer(buf);
    writer.StartArray();
    co_await ssx::async_for_each(
      migrations.begin(),
      migrations.end(),
      [&writer](const cluster::data_migrations::migration_metadata& meta) {
          write_migration_as_json(meta, writer);
      });
    writer.EndArray();
    reply->set_status(ss::http::reply::status_type::ok, buf.GetString());

    co_return std::move(reply);
}

ss::future<std::unique_ptr<ss::http::reply>> admin_server::get_data_migration(
  std::unique_ptr<ss::http::request> req,
  std::unique_ptr<ss::http::reply> reply) {
    auto id = parse_data_migration_id(*req);
    auto& frontend = _controller->get_data_migration_frontend();
    auto maybe_migration = co_await frontend.local().get_migration(id);
    if (maybe_migration.has_value()) [[likely]] {
        json::StringBuffer buf;
        json::Writer<json::StringBuffer> writer(buf);
        write_migration_as_json(maybe_migration.assume_value(), writer);
        reply->set_status(ss::http::reply::status_type::ok, buf.GetString());
    } else {
        co_await throw_on_error(
          *req, maybe_migration.error(), model::controller_ntp);
    }
    co_return std::move(reply);
}

ss::future<std::unique_ptr<ss::http::reply>> admin_server::add_data_migration(
  std::unique_ptr<ss::http::request> req,
  std::unique_ptr<ss::http::reply> reply) {
    auto& frontend = _controller->get_data_migration_frontend();
    auto json_doc = co_await parse_json_body(req.get());
    auto r = co_await frontend.local().create_migration(
      parse_add_migration_request(json_doc));
    if (!r) {
        vlog(
          adminlog.warn,
          "unable to create data migration - error: {}",
          r.error());
        co_await throw_on_error(*req, r.error(), model::controller_ntp);
        co_return std::move(reply);
    }
    json::StringBuffer buf;
    json::Writer<json::StringBuffer> writer(buf);
    writer.StartObject();
    writer.Key("id");
    writer.Int64(r.value());
    writer.EndObject();
    reply->set_status(ss::http::reply::status_type::ok, buf.GetString());

    co_return std::move(reply);
}

ss::future<ss::json::json_return_type>
admin_server::execute_migration_action(std::unique_ptr<ss::http::request> req) {
    auto id = parse_data_migration_id(*req);
    auto& frontend = _controller->get_data_migration_frontend();

    auto ec = co_await frontend.local().update_migration_state(
      id, parse_migration_action(*req));
    if (ec) {
        co_await throw_on_error(*req, ec, model::controller_ntp);
    }
    co_return ss::json::json_void();
}

ss::future<ss::json::json_return_type>
admin_server::delete_migration(std::unique_ptr<ss::http::request> req) {
    auto id = parse_data_migration_id(*req);
    auto& frontend = _controller->get_data_migration_frontend();
    auto ec = co_await frontend.local().remove_migration(id);
    if (ec) {
        co_await throw_on_error(*req, ec, model::controller_ntp);
    }
    co_return ss::json::json_void();
}
static constexpr auto consumer_groups_data_key = "consumer_groups_data";
static constexpr auto group_id_key = "group_id";
static constexpr auto topics_key = "topics";
static constexpr auto topic_key = "topic";
static constexpr auto partitions_key = "partitions";
static constexpr auto partition_key = "partition";
static constexpr auto offset_key = "offset";

ss::future<std::unique_ptr<ss::http::reply>>
admin_server::get_migrated_entities_status(
  std::unique_ptr<ss::http::request> req,
  std::unique_ptr<ss::http::reply> reply) {
    auto id = parse_data_migration_id(*req);

    auto& ifrontend = _controller->get_data_migration_irpc_frontend();

    auto entities_status = co_await ifrontend.local().get_entities_status(id);

    if (!entities_status.has_value()) [[unlikely]] {
        co_await throw_on_error(
          *req, entities_status.error(), model::controller_ntp);
        vassert(false, "should have thrown");
    }

    json::StringBuffer buf;
    json::Writer<json::StringBuffer> writer(buf);
    writer.StartObject();
    ssx::async_counter counter;
    const auto& groups = entities_status.assume_value().groups;
    writer.Key(consumer_groups_data_key);
    writer.StartArray();
    for (const auto& group : groups) {
        writer.StartObject();
        writer.Key(group_id_key);
        writer.String(group.group_id);
        writer.Key(topics_key);
        writer.StartArray();
        for (const auto& topic : group.offsets) {
            writer.StartObject();
            writer.Key(topic_key);
            writer.String(topic.topic());
            writer.Key(partitions_key);
            writer.StartArray();
            co_await ssx::async_for_each_counter(
              counter, topic.partitions, [&writer](const auto& partition) {
                  writer.StartObject();
                  writer.Key(partition_key);
                  writer.Int(partition.partition);
                  writer.Key(offset_key);
                  writer.Int64(partition.offset);
                  writer.EndObject();
              });
            writer.EndArray();
            writer.EndObject();
        }
        writer.EndArray();
        writer.EndObject();
    }
    writer.EndArray();
    writer.EndObject();
    reply->set_status(ss::http::reply::status_type::ok, buf.GetString());
    co_return std::move(reply);
}

cluster::data_migrations::entities_status
parse_migrated_entities_status(json::Value& json) {
    cluster::data_migrations::entities_status ret;

    if (
      auto it = json.FindMember(consumer_groups_data_key);
      it != json.MemberEnd()) {
        auto consumer_groups_array = it->value.GetArray();
        ret.groups.reserve(consumer_groups_array.Size());
        for (auto& group : consumer_groups_array) {
            cluster::group_offsets group_res{
              .group_id = group[group_id_key].GetString()};
            for (auto& topic : group[topics_key].GetArray()) {
                cluster::group_offsets::topic_partitions topic_res;
                topic_res.topic = model::topic(topic[topic_key].GetString());
                for (auto& partition : topic[partitions_key].GetArray()) {
                    topic_res.partitions.push_back(
                      {model::partition_id{partition[partition_key].GetInt()},
                       kafka::offset{partition[offset_key].GetInt64()}});
                }
                group_res.offsets.push_back(std::move(topic_res));
            }
            ret.groups.emplace_back(std::move(group_res));
        }
    }
    return ret;
}

ss::future<std::unique_ptr<ss::http::reply>>
admin_server::set_migrated_entities_status(
  std::unique_ptr<ss::http::request> req,
  std::unique_ptr<ss::http::reply> reply) {
    auto id = parse_data_migration_id(*req);

    auto& ifrontend = _controller->get_data_migration_irpc_frontend();
    auto json_doc = co_await parse_json_body(req.get());
    auto status_data = parse_migrated_entities_status(json_doc);

    auto res = co_await ifrontend.local().set_entities_status(
      id, std::move(status_data));

    if (res != cluster::errc::success) {
        vlog(adminlog.warn, "unable to set migration entities status: {}", res);
        co_await throw_on_error(*req, res, model::controller_ntp);
    }
    reply->set_status(ss::http::reply::status_type::ok);
    co_return std::move(reply);
}

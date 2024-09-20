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
#include "cluster/data_migration_types.h"
#include "container/fragmented_vector.h"
#include "json/validator.h"
#include "redpanda/admin/api-doc/migration.json.hh"
#include "redpanda/admin/data_migration_utils.h"
#include "redpanda/admin/server.h"
#include "redpanda/admin/util.h"

using admin::apply_validator;

namespace {

json::validator make_mount_configuration_validator() {
    const std::string schema = R"(

{
    "$schema": "http://json-schema.org/draft-04/schema#",
    "type": "object",
    "additionalProperties": false,
    "required": [
        "topics"
    ],
    "properties": {
        "topics": {
            "type": "array",
            "items": {
                "$ref": "#/definitions/inbound_topic"
            },
            "description": "List of topics to mount"
        }
    },
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
                "source_topic"
            ],
            "properties": {
                "source_topic": {
                    "$ref": "#/definitions/namespaced_topic"
                },
                "alias": {
                    "$ref": "#/definitions/namespaced_topic"
                },
                "location": {
                    "type": "string"
                }
            },
            "additionalProperties": false
        }
    }
})";
    return json::validator(schema);
}

json::validator make_unmount_array_validator() {
    const std::string schema = R"(
{
    "$schema": "http://json-schema.org/draft-04/schema#",
    "type": "object",
    "additionalProperties": false,
    "required": [
        "topics"
    ],
    "properties": {
        "topics": {
            "type": "array",
            "items": {
                "$ref": "#/definitions/namespaced_topic"
            },
            "description": "List of topics to unmount"
        }
    },
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
        }
    }
})";
    return json::validator(schema);
}

} // namespace

void admin_server::register_topic_routes() {
    register_route<superuser>(
      ss::httpd::migration_json::mount_topics,
      [this](std::unique_ptr<ss::http::request> req) {
          return mount_topics(std::move(req));
      });
    register_route<superuser>(
      ss::httpd::migration_json::unmount_topics,
      [this](std::unique_ptr<ss::http::request> req) {
          return unmount_topics(std::move(req));
      });
}

ss::future<ss::json::json_return_type>
admin_server::mount_topics(std::unique_ptr<ss::http::request> req) {
    static thread_local json::validator validator
      = make_mount_configuration_validator();
    auto json_doc = co_await parse_json_body(req.get());
    apply_validator(validator, json_doc);
    cluster::data_migrations::inbound_migration migration;

    migration.auto_advance = true;
    migration.topics = parse_inbound_topics(json_doc);
    auto result = co_await _controller->get_data_migration_frontend()
                    .local()
                    .create_migration(std::move(migration));
    if (!result) {
        vlog(
          adminlog.warn,
          "unable to create data migration for topic mount - error: {}",
          result.error());
        co_await throw_on_error(*req, result.error(), model::controller_ntp);
        throw ss::httpd::server_error_exception(
          "unknown error when creating data migration for mounting topics");
    }

    ss::httpd::migration_json::migration_info reply;
    reply.id = result.value();
    co_return std::move(reply);
}

ss::future<ss::json::json_return_type>
admin_server::unmount_topics(std::unique_ptr<ss::http::request> req) {
    static thread_local json::validator validator
      = make_unmount_array_validator();
    auto json_doc = co_await parse_json_body(req.get());
    apply_validator(validator, json_doc);
    cluster::data_migrations::outbound_migration migration;

    migration.auto_advance = true;
    migration.topics = parse_topics(json_doc);
    auto result = co_await _controller->get_data_migration_frontend()
                    .local()
                    .create_migration(std::move(migration));
    if (!result) {
        vlog(
          adminlog.warn,
          "unable to create data migration for topic unmount - error: {}",
          result.error());
        co_await throw_on_error(*req, result.error(), model::controller_ntp);
        throw ss::httpd::server_error_exception(
          "unknown error when creating data migration for unmounting topics");
    }

    ss::httpd::migration_json::migration_info reply;
    reply.id = result.value();
    co_return std::move(reply);
}

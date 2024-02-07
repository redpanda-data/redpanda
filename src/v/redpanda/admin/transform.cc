/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "model/transform.h"

#include "bytes/streambuf.h"
#include "json/document.h"
#include "json/istreamwrapper.h"
#include "json/validator.h"
#include "redpanda/admin/api-doc/transform.json.hh"
#include "redpanda/admin/server.h"
#include "redpanda/admin/util.h"
#include "transform/api.h"

namespace {

ss::httpd::bad_request_exception transforms_not_enabled() {
    return {"data transforms disabled - use `rpk cluster config set "
            "data_transforms_enabled true` to enable"};
}

} // namespace

void admin_server::register_wasm_transform_routes() {
    register_route<superuser>(
      ss::httpd::transform_json::deploy_transform,
      [this](auto req) { return deploy_transform(std::move(req)); });
    register_route<user>(
      ss::httpd::transform_json::list_transforms,
      [this](auto req) { return list_transforms(std::move(req)); });
    register_route<superuser>(
      ss::httpd::transform_json::delete_transform,
      [this](auto req) { return delete_transform(std::move(req)); });
    register_route<superuser>(
      ss::httpd::transform_json::list_committed_offsets,
      [this](auto req) { return list_committed_offsets(std::move(req)); });
    register_route<superuser>(
      ss::httpd::transform_json::garbage_collect_committed_offsets,
      [this](auto req) {
          return garbage_collect_committed_offsets(std::move(req));
      });
}

ss::future<ss::json::json_return_type>
admin_server::delete_transform(std::unique_ptr<ss::http::request> req) {
    if (!_transform_service->local_is_initialized()) {
        throw transforms_not_enabled();
    }
    auto name = model::transform_name(req->param["name"]);
    auto ec = co_await _transform_service->local().delete_transform(
      std::move(name));
    co_await throw_on_error(*req, ec, model::controller_ntp);
    co_return ss::json::json_void();
}

namespace {
ss::httpd::transform_json::partition_transform_status::
  partition_transform_status_status
  convert_transform_status(
    model::transform_report::processor::state model_state) {
    using model_status = model::transform_report::processor::state;
    using json_status = ss::httpd::transform_json::partition_transform_status::
      partition_transform_status_status;
    switch (model_state) {
    case model_status::inactive:
        return json_status::inactive;
    case model_status::running:
        return json_status::running;
    case model_status::errored:
        return json_status::errored;
    case model_status::unknown:
        return json_status::unknown;
    }
    vlog(adminlog.error, "unknown transform status: {}", uint8_t(model_state));
    return json_status::unknown;
}
} // namespace

ss::future<ss::json::json_return_type>
admin_server::list_transforms(std::unique_ptr<ss::http::request>) {
    if (!_transform_service->local_is_initialized()) {
        throw transforms_not_enabled();
    }
    auto report = co_await _transform_service->local().list_transforms();

    co_return ss::json::json_return_type(ss::json::stream_range_as_array(
      std::move(report.transforms), [](const auto& entry) {
          const model::transform_report& t = entry.second;
          ss::httpd::transform_json::transform_metadata meta;
          meta.name = t.metadata.name();
          meta.input_topic = t.metadata.input_topic.tp();
          for (const auto& output_topic : t.metadata.output_topics) {
              meta.output_topics.push(output_topic.tp());
          }
          for (const auto& [k, v] : t.metadata.environment) {
              ss::httpd::transform_json::environment_variable var;
              var.key = k;
              var.value = v;
              meta.environment.push(var);
          }
          for (const auto& [_, processor] : t.processors) {
              ss::httpd::transform_json::partition_transform_status s;
              s.partition = processor.id();
              s.node_id = processor.node();
              s.status = convert_transform_status(processor.status);
              s.lag = processor.lag;
              meta.status.push(s);
          }
          return meta;
      }));
}

namespace {
void validate_transform_deploy_document(const json::Document& doc) {
    const std::string schema = R"(
{
    "type": "object",
    "properties": {
        "name": {
            "type": "string"
        },
        "input_topic": {
            "type": "string"
        },
        "output_topics": {
            "type": "array",
            "items": {
              "type": "string"
            }
        },
        "environment": {
            "type": "array",
            "items": {
              "type": "object",
              "properties": {
                  "key": {
                      "type": "string"
                  },
                  "value": {
                      "type": "string"
                  }
              },
              "required": [
                  "key",
                  "value"
              ],
              "additionalProperties": false
            }
        }
    },
    "required": ["name", "input_topic", "output_topics"],
    "additionalProperties": false
}
)";
    auto validator = json::validator(schema);
    try {
        json::validate(validator, doc);
    } catch (json::json_validation_error& err) {
        throw ss::httpd::bad_request_exception(
          fmt::format("invalid JSON request body: {}", err.what()));
    }
}
} // namespace

ss::future<ss::json::json_return_type>
admin_server::deploy_transform(std::unique_ptr<ss::http::request> req) {
    if (!_transform_service->local_is_initialized()) {
        throw transforms_not_enabled();
    }
    // The request body could be large here, so stream it into an iobuf.
    iobuf body;
    auto out_stream = make_iobuf_ref_output_stream(body);
    co_await ss::copy(*req->content_stream, out_stream);
    // Now wrap the iobuf in a stream and parse the header JSON document out.
    iobuf_istreambuf ibuf(body);
    std::istream stream(&ibuf);
    json::IStreamWrapper s(stream);
    // Make sure to stop when the JSON object is over, as after is the Wasm
    // binary.
    json::Document doc;
    doc.ParseStream<
      rapidjson::kParseDefaultFlags | rapidjson::kParseStopWhenDoneFlag>(s);
    if (doc.HasParseError()) {
        throw ss::httpd::bad_request_exception(
          fmt::format("JSON parse error: {}", doc.GetParseError()));
    }
    // Drop the JSON object from the iobuf, so this is just our Wasm binary.
    body.trim_front(s.Tell());
    validate_transform_deploy_document(doc);

    // Convert JSON into our metadata object.
    auto name = model::transform_name(doc["name"].GetString());
    auto input_nt = model::topic_namespace(
      model::kafka_namespace, model::topic(doc["input_topic"].GetString()));
    std::vector<model::topic_namespace> output_topics;
    for (const auto& topic : doc["output_topics"].GetArray()) {
        auto output_nt = model::topic_namespace(
          model::kafka_namespace, model::topic(topic.GetString()));
        output_topics.push_back(output_nt);
    }
    absl::flat_hash_map<ss::sstring, ss::sstring> env;
    if (doc.HasMember("environment")) {
        for (const auto& e : doc["environment"].GetArray()) {
            auto v = e.GetObject();
            env.insert_or_assign(v["key"].GetString(), v["value"].GetString());
        }
    }

    // Now do the deploy!
    cluster::errc ec = co_await _transform_service->local().deploy_transform(
      {.name = name,
       .input_topic = input_nt,
       .output_topics = output_topics,
       .environment = std::move(env)},
      std::move(body));

    co_await throw_on_error(*req, ec, model::controller_ntp);
    co_return ss::json::json_void();
}

ss::future<ss::json::json_return_type>
admin_server::list_committed_offsets(std::unique_ptr<ss::http::request> req) {
    if (!_transform_service->local_is_initialized()) {
        throw transforms_not_enabled();
    }
    auto result = co_await _transform_service->local().list_committed_offsets(
      {.show_unknown = admin::get_boolean_query_param(*req, "show_unknown")});
    if (result.has_error()) {
        co_await throw_on_error(*req, result.error(), model::controller_ntp);
        co_return ss::json::json_void();
    }

    co_return ss::json::json_return_type(ss::json::stream_range_as_array(
      admin::lw_shared_container(std::move(result).value()),
      [](const model::transform_committed_offset& committed) {
          ss::httpd::transform_json::committed_offset response;
          response.transform_name = committed.name();
          response.offset = committed.offset();
          response.partition = committed.partition();
          return response;
      }));
}

ss::future<ss::json::json_return_type>
admin_server::garbage_collect_committed_offsets(
  std::unique_ptr<ss::http::request> req) {
    if (!_transform_service->local_is_initialized()) {
        throw transforms_not_enabled();
    }
    auto ec = co_await _transform_service->local()
                .garbage_collect_committed_offsets();
    co_await throw_on_error(*req, ec, model::controller_ntp);
    co_return ss::json::json_void();
}

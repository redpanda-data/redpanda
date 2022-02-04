#include "cluster/controller.h"
#include "config/node_config.h"
#include "redpanda/admin/api-doc/cluster_config.json.h"
#include "redpanda/admin/server.h"
#include "vlog.h"

namespace admin {

namespace {
json_validator make_cluster_config_validator() {
    const std::string schema = R"(
{
    "type": "object",
    "properties": {
        "upsert": {
            "type": "object"
        },
        "remove": {
            "type": "array",
            "items": "string"
        }
    },
    "additionalProperties": false,
    "required": ["upsert", "remove"]
}
)";
    return json_validator(schema);
}
} // namespace

void admin_server::register_cluster_config_routes() {
    static thread_local auto cluster_config_validator(
      make_cluster_config_validator());

    ss::httpd::cluster_config_json::get_cluster_config_status.set(
      _server._routes,
      [this](std::unique_ptr<ss::httpd::request> req)
        -> ss::future<ss::json::json_return_type> {
          std::vector<ss::httpd::cluster_config_json::cluster_config_status>
            res;

          auto& cfg = _controller->get_config_manager();
          auto statuses = co_await cfg.invoke_on(
            cluster::controller_stm_shard,
            [](cluster::config_manager& manager) {
                // Intentional copy, do not want to pass reference to mutable
                // status map back to originating core.
                return cluster::config_manager::status_map(
                  manager.get_status());
            });

          for (const auto& s : statuses) {
              vlog(logger.trace, "status: {}", s.second);
              auto& rs = res.emplace_back();
              rs.node_id = s.first;
              rs.restart = s.second.restart;
              rs.config_version = s.second.version;

              // Workaround: seastar json_list hides empty lists by
              // default.  This complicates API clients, so always push
              // in a dummy element to get _set=true on json_list (this
              // is then cleared in the subsequent operator=).
              rs.invalid.push(ss::sstring("hack"));
              rs.unknown.push(ss::sstring("hack"));

              rs.invalid = s.second.invalid;
              rs.unknown = s.second.unknown;
          }

          co_return ss::json::json_return_type(std::move(res));
      });

    ss::httpd::cluster_config_json::get_cluster_config_schema.set(
      _server._routes,
      [](std::unique_ptr<ss::httpd::request> req)
        -> ss::future<ss::json::json_return_type> {
          using property_map = std::map<
            ss::sstring,
            ss::httpd::cluster_config_json::cluster_config_property_metadata>;

          property_map properties;

          config::shard_local_cfg().for_each(
            [&properties](const config::base_property& p) {
                if (p.get_visibility() == config::visibility::deprecated) {
                    // Do not mention deprecated settings in schema: they
                    // only exist internally to avoid making existing stored
                    // configs invalid.
                    return;
                }

                auto [pm_i, inserted] = properties.emplace(
                  ss::sstring(p.name()),
                  ss::httpd::cluster_config_json::
                    cluster_config_property_metadata());
                vassert(inserted, "Emplace failed, duplicate property name?");

                auto& pm = pm_i->second;
                pm.description = ss::sstring(p.desc());
                pm.needs_restart = p.needs_restart();
                pm.visibility = ss::sstring(
                  config::to_string_view(p.get_visibility()));
                pm.nullable = p.is_nullable();
                pm.is_secret = p.is_secret();

                if (p.is_array()) {
                    pm.type = "array";

                    auto items = ss::httpd::cluster_config_json::
                      cluster_config_property_metadata_items();
                    items.type = ss::sstring(p.type_name());
                    pm.items = items;
                } else {
                    pm.type = ss::sstring(p.type_name());
                }

                const auto& example = p.example();
                if (example.has_value()) {
                    pm.example = ss::sstring(example.value());
                }

                const auto& units = p.units_name();
                if (units.has_value()) {
                    pm.units = ss::sstring(units.value());
                }
            });

          std::map<ss::sstring, property_map> response = {
            {ss::sstring("properties"), std::move(properties)}};

          co_return ss::json::json_return_type(std::move(response));
      });

    ss::httpd::cluster_config_json::patch_cluster_config.set(
      _server._routes,
      [this](std::unique_ptr<ss::httpd::request> req)
        -> ss::future<ss::json::json_return_type> {
          if (!config::node().enable_central_config) {
              throw ss::httpd::bad_request_exception(
                "Requires enable_central_config=True in node configuration");
          }

          auto doc = parse_json_body(*req);
          apply_validator(cluster_config_validator, doc);

          cluster::config_update_request update;

          // Deserialize removes
          const auto& json_remove = doc["remove"];
          for (const auto& v : json_remove.GetArray()) {
              update.remove.push_back(v.GetString());
          }

          // Deserialize upserts
          const auto& json_upsert = doc["upsert"];
          for (const auto& i : json_upsert.GetObject()) {
              // Re-serialize the individual value.  Our on-disk format
              // for property values is a YAML value (JSON is a subset
              // of YAML, so encoding with JSON is fine)
              rapidjson::StringBuffer val_buf;
              rapidjson::Writer<rapidjson::StringBuffer> w{val_buf};
              i.value.Accept(w);
              auto s = ss::sstring{val_buf.GetString(), val_buf.GetSize()};
              update.upsert.push_back({i.name.GetString(), s});
          }

          // Config property validation happens further down the line
          // at the point that properties are set on each node in
          // response to the deltas that we write to the controller log,
          // but we also do an early validation pass here to avoid writing
          // clearly wrong things into the log & give better feedback
          // to the API consumer.
          if (!get_boolean_query_param(*req, "force")) {
              // A scratch copy of configuration: we must not touch
              // the real live configuration object, that will be updated
              // by config_manager much after config is written to controller
              // log.
              config::configuration cfg;

              std::map<ss::sstring, ss::sstring> errors;
              for (const auto& i : update.upsert) {
                  // Decode to a YAML object because that's what the property
                  // interface expects.
                  // Don't both catching ParserException: this was encoded
                  // just a few lines above.
                  const auto& yaml_value = i.second;
                  auto val = YAML::Load(yaml_value);

                  if (!cfg.contains(i.first)) {
                      errors[i.first] = "Unknown property";
                      continue;
                  }
                  auto& property = cfg.get(i.first);

                  try {
                      auto validation_err = property.validate(val);
                      if (validation_err.has_value()) {
                          errors[i.first]
                            = validation_err.value().error_message();
                          vlog(
                            logger.warn,
                            "Invalid {}: '{}' ({})",
                            i.first,
                            yaml_value,
                            validation_err.value().error_message());
                      } else {
                          // In case any property subclass might throw
                          // from it's value setter even after a non-throwing
                          // call to validate (if this happens validate() was
                          // implemented wrongly, but let's be safe)
                          property.set_value(val);
                      }
                  } catch (...) {
                      auto message = fmt::format(
                        "{}", std::current_exception());
                      errors[i.first] = message;
                      vlog(
                        logger.warn,
                        "Invalid {}: '{}' ({})",
                        i.first,
                        yaml_value,
                        message);
                  }
              }

              if (!errors.empty()) {
                  // TODO structured response
                  throw ss::httpd::bad_request_exception(
                    fmt::format("Invalid properties"));
              }
          }

          vlog(
            logger.trace,
            "patch_cluster_config: {} upserts, {} removes",
            update.upsert.size(),
            update.remove.size());

          auto patch_result
            = co_await _controller->get_config_frontend().invoke_on(
              cluster::config_frontend::version_shard,
              [update = std::move(update)](cluster::config_frontend& fe) mutable
              -> ss::future<cluster::config_frontend::patch_result> {
                  return fe.patch(
                    std::move(update), model::timeout_clock::now() + 5s);
              });

          co_await throw_on_error(
            *req, patch_result.errc, model::controller_ntp);

          ss::httpd::cluster_config_json::cluster_config_write_result result;
          result.config_version = patch_result.version;
          co_return ss::json::json_return_type(std::move(result));
      });
}

} // namespace admin

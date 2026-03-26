/*
 * Copyright 2020 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "base/vlog.h"
#include "cloud_io/cache_service.h"
#include "cluster/config_frontend.h"
#include "cluster/controller.h"
#include "cluster/controller_stm.h"
#include "cluster/errc.h"
#include "cluster/feature_manager.h"
#include "cluster/fwd.h"
#include "cluster/metadata_cache.h"
#include "cluster/partition_manager.h"
#include "cluster/shard_table.h"
#include "config/base_property.h"
#include "config/configuration.h"
#include "config/validators.h"
#include "features/enterprise_features.h"
#include "features/feature_table.h"
#include "json/document.h"
#include "json/stringbuffer.h"
#include "json/validator.h"
#include "json/writer.h"
#include "pandaproxy/schema_registry/api.h"
#include "pandaproxy/schema_registry/schema_id_validation.h"
#include "raft/types.h"
#include "redpanda/admin/api-doc/cluster_config.json.hh"
#include "redpanda/admin/api-doc/config.json.hh"
#include "redpanda/admin/api-doc/features.json.hh"
#include "redpanda/admin/api-doc/raft.json.hh"
#include "redpanda/admin/api-doc/status.json.hh"
#include "redpanda/admin/cluster_config_schema_util.h"
#include "redpanda/admin/server.h"
#include "redpanda/admin/util.h"
#include "ssx/sformat.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/smp.hh>
#include <seastar/core/sstring.hh>
#include <seastar/http/exception.hh>
#include <seastar/http/json_path.hh>
#include <seastar/util/log.hh>
#include <seastar/util/short_streams.hh>

#include <boost/algorithm/string/classification.hpp>
#include <boost/algorithm/string/predicate.hpp>
#include <boost/algorithm/string/trim.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/lexical_cast/bad_lexical_cast.hpp>
#include <fmt/core.h>

#include <chrono>
#include <unordered_map>
#include <unordered_set>

using namespace std::chrono_literals;

using admin::apply_validator;
using admin::get_boolean_query_param;

void admin_server::register_config_routes() {
    register_route_raw_sync<superuser>(
      ss::httpd::config_json::get_config,
      [](ss::httpd::const_req, ss::http::reply& reply) {
          json::StringBuffer buf;
          json::Writer<json::StringBuffer> writer(buf);
          config::shard_local_cfg().to_json(
            writer, config::redact_secrets::yes);

          reply.set_status(ss::http::reply::status_type::ok, buf.GetString());
          return "";
      });

    register_route_raw_sync<superuser>(
      ss::httpd::cluster_config_json::get_cluster_config,
      [](ss::httpd::const_req req, ss::http::reply& reply) {
          json::StringBuffer buf;
          json::Writer<json::StringBuffer> writer(buf);

          bool include_defaults = true;
          auto include_defaults_str = req.get_query_param("include_defaults");
          if (!include_defaults_str.empty()) {
              include_defaults = str_to_bool(include_defaults_str);
          }

          auto key_str = req.get_query_param("key");
          if (!key_str.empty()) {
              // Write a single key to json.
              try {
                  config::shard_local_cfg().to_json_single_key(
                    writer, config::redact_secrets::yes, key_str);
              } catch (const std::out_of_range&) {
                  throw ss::httpd::bad_param_exception(
                    fmt::format("Unknown property {{{}}}", key_str));
              }
          } else {
              // Write the entire config to json.
              config::shard_local_cfg().to_json(
                writer,
                config::redact_secrets::yes,
                [include_defaults](config::base_property& p) {
                    return include_defaults || !p.is_default();
                });
          }

          reply.set_status(ss::http::reply::status_type::ok, buf.GetString());
          return "";
      });

    register_route_raw_sync<superuser>(
      ss::httpd::config_json::get_node_config,
      [](ss::httpd::const_req, ss::http::reply& reply) {
          json::StringBuffer buf;
          json::Writer<json::StringBuffer> writer(buf);
          config::node().to_json(writer, config::redact_secrets::yes);

          reply.set_status(ss::http::reply::status_type::ok, buf.GetString());
          return "";
      });

    register_route_raw_sync<superuser>(
      ss::httpd::config_json::get_loggers,
      [](ss::httpd::const_req req, ss::http::reply& reply) {
          json::StringBuffer buf;
          json::Writer<json::StringBuffer> writer(buf);
          bool include_levels = false;
          auto include_levels_str = req.get_query_param("include-levels");
          if (!include_levels_str.empty()) {
              include_levels = str_to_bool(include_levels_str);
          }
          writer.StartArray();
          for (const auto& name :
               ss::global_logger_registry().get_all_logger_names()) {
              writer.StartObject();
              writer.Key("name");
              writer.String(name);
              if (include_levels) {
                  writer.Key("level");
                  writer.String(
                    fmt::format(
                      "{}",
                      ss::global_logger_registry().get_logger_level(name)));
              }
              writer.EndObject();
          }
          writer.EndArray();
          reply.set_status(ss::http::reply::status_type::ok, buf.GetString());
          return "";
      });

    register_route<superuser>(
      ss::httpd::config_json::get_log_level,
      [this](std::unique_ptr<ss::http::request> req) {
          return container().invoke_on(
            ss::shard_id{0}, [req = std::move(req)](const admin_server& as) {
                ss::httpd::config_json::get_log_level_response rsp{};
                ss::sstring name = req->get_path_param("name");
                if (name == "") {
                    throw ss::httpd::bad_param_exception(
                      fmt::format(
                        "Invalid parameter 'name' got {{{}}}",
                        req->get_path_param("name")));
                }
                validate_no_control(name, string_conversion_exception{"name"});

                ss::log_level cur_level;
                try {
                    cur_level = ss::global_logger_registry().get_logger_level(
                      name);
                } catch (const std::out_of_range&) {
                    throw ss::httpd::bad_param_exception(
                      fmt::format(
                        "Cannot set log level: unknown logger {{{}}}", name));
                }

                rsp.name = name;
                rsp.level = ss::to_sstring(cur_level);

                auto find_iter = as._log_level_resets.find(name);
                if (
                  find_iter == as._log_level_resets.end()
                  || !find_iter->second.expires.has_value()) {
                    rsp.expiration = 0;
                } else {
                    auto remaining_dur = find_iter->second.expires.value()
                                         - ss::timer<>::clock::now();
                    rsp.expiration
                      = std::chrono::duration_cast<std::chrono::seconds>(
                          remaining_dur)
                          .count();
                }

                return ss::make_ready_future<ss::json::json_return_type>(rsp);
            });
      });

    register_route<superuser>(
      ss::httpd::config_json::set_log_level,
      [this](std::unique_ptr<ss::http::request> req) {
          return container().invoke_on(
            ss::shard_id{0}, [req = std::move(req)](admin_server& as) {
                using namespace std::chrono_literals;
                ss::httpd::config_json::set_log_level_response rsp{};
                ss::sstring name = req->get_path_param("name");
                if (name == "") {
                    throw ss::httpd::bad_param_exception(
                      fmt::format(
                        "Invalid parameter 'name' got {{{}}}",
                        req->get_path_param("name")));
                }
                validate_no_control(name, string_conversion_exception{"name"});

                // current level: will be used revert after a timeout (optional)
                ss::log_level cur_level;
                try {
                    cur_level = ss::global_logger_registry().get_logger_level(
                      name);
                } catch (const std::out_of_range&) {
                    throw ss::httpd::bad_param_exception(
                      fmt::format(
                        "Cannot set log level: unknown logger {{{}}}", name));
                }

                rsp.name = name;
                rsp.previous_level = ss::to_sstring(cur_level);

                // decode new level
                ss::log_level new_level;
                try {
                    new_level = boost::lexical_cast<ss::log_level>(
                      req->get_query_param("level"));
                } catch (const boost::bad_lexical_cast& e) {
                    throw ss::httpd::bad_param_exception(
                      fmt::format(
                        "Cannot set log level for {{{}}}: unknown level {{{}}}",
                        name,
                        req->get_query_param("level")));
                }

                rsp.new_level = ss::to_sstring(new_level);

                // how long should the new log level be active
                std::optional<std::chrono::seconds> expires;
                if (auto e = req->get_query_param("expires"); !e.empty()) {
                    try {
                        expires = std::chrono::seconds(
                          boost::lexical_cast<unsigned int>(e));
                    } catch (const boost::bad_lexical_cast& e) {
                        throw ss::httpd::bad_param_exception(
                          fmt::format(
                            "Cannot set log level for {{{}}}: invalid expires "
                            "value "
                            "{{{}}}",
                            name,
                            e));
                    }
                }

                // Should we force the supplied expiration over the configured
                // max?
                auto force = false;
                if (auto f = req->get_query_param("force"); !f.empty()) {
                    force = str_to_bool(f);
                }

                auto is_verbose = [](auto new_level) {
                    static std::unordered_set verbose_levels{
                      ss::log_level::debug, ss::log_level::trace};
                    return verbose_levels.contains(new_level);
                };

                auto clamp_expiry = [&is_verbose](
                                      auto& expires, auto level, auto force) {
                    // if no expiration was given, then use some reasonable
                    // default that will prevent the system from remaining in a
                    // non-optimal state (e.g. trace logging) indefinitely.
                    auto exp = expires.value_or(600s);

                    auto verbose = is_verbose(level);

                    // subject to a node-config'ed max value, overrideable by
                    // `force` query param
                    auto max_exp
                      = (!verbose || force)
                          ? std::nullopt
                          : config::node().verbose_logging_timeout_sec_max();

                    // don't allow indefinite trace logging if we have a max
                    // configured
                    if (max_exp.has_value() && exp / 1s == 0) {
                        return max_exp.value();
                    }

                    return std::min(
                      exp, max_exp.value_or(std::chrono::seconds::max()));
                };

                // Maybe clamp expiration to node config
                auto expires_v = clamp_expiry(expires, new_level, force);

                vlog(
                  adminlog.info,
                  "Set log level for {{{}}}: {} -> {} (expiring {})",
                  name,
                  cur_level,
                  new_level,
                  expires_v / 1s > 0
                    ? fmt::format(
                        "{}s",
                        std::chrono::duration_cast<std::chrono::seconds>(
                          expires_v)
                          .count())
                    : "NEVER");

                ss::global_logger_registry().set_logger_level(name, new_level);

                auto when = [&]() -> std::optional<level_reset::time_point> {
                    // expires=0 is same as not specifying it at all
                    if (expires_v / 1s > 0) {
                        return ss::timer<>::clock::now() + expires_v;
                    } else {
                        // new log level never expires, but we still want an
                        // entry in the resets map as a record of the default
                        return std::nullopt;
                    }
                }();

                auto res = as._log_level_resets.try_emplace(
                  name, cur_level, when);
                if (!res.second) {
                    res.first->second.expires = when;
                }

                rsp.expiration = expires_v / 1s;

                as.rearm_log_level_timer();

                return ss::make_ready_future<ss::json::json_return_type>(rsp);
            });
      });
}

namespace {
json::validator make_cluster_config_validator() {
    const std::string_view schema = R"(
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
    return json::validator(schema);
}

ss::sstring join_properties(
  const std::vector<
    std::reference_wrapper<const config::property<std::optional<ss::sstring>>>>&
    props) {
    ss::sstring result = "";
    for (size_t idx = 0; const auto& prop : props) {
        if (idx == props.size() - 1) {
            result += ss::sstring{prop.get().name()};
        } else {
            result += ssx::sformat("{}, ", prop.get().name());
        }

        ++idx;
    };

    return result;
}

/**
 * This function provides special case validation for configuration
 * properties that need to check other properties' values as well
 * as their own.
 *
 * Ideally this would be built into the config_store/property generic
 * interfaces, but that's a lot of plumbing for a few relatively simple
 * checks, so for the moment we just do the checks here by hand.
 */
void config_multi_property_validation(
  const ss::sstring& username,
  pandaproxy::schema_registry::api* schema_registry,
  const cluster::config_update_request& req,
  const config::configuration& updated_config,
  std::map<ss::sstring, ss::sstring>& errors) {
    absl::flat_hash_set<ss::sstring> modified_keys;
    for (const auto& i : req.upsert) {
        modified_keys.insert(i.key);
    }

    if (
      (modified_keys.contains("admin_api_require_auth")
       || modified_keys.contains("superusers"))
      && updated_config.admin_api_require_auth()) {
        // We are switching on admin_api_require_auth.  Apply rules to prevent
        // the user "locking themselves out of the house".
        const bool auth_was_enabled
          = config::shard_local_cfg().admin_api_require_auth();

        // There must be some superusers defined
        const auto& superusers = updated_config.superusers();
        absl::flat_hash_set<ss::sstring> superusers_set(
          superusers.begin(), superusers.end());
        if (superusers.empty()) {
            // Some superusers must be defined, or nobody will be able
            // to use the admin API after this request.
            errors["admin_api_require_auth"] = "No superusers defined";
        } else if (!superusers_set.contains(username)) {
            if (!auth_was_enabled) {
                // When enabling auth, user making the change must be in the
                // list of superusers, or they would be locking themselves out.
                errors["admin_api_require_auth"]
                  = "May only be set by a superuser";
            } else {
                // When auth is enabled, user making the change must be in the
                // list of superusers, or they would be locking themselves out.
                errors["superusers"] = "superusers must contain the user "
                                       "making the change when auth is enabled";
            }
        }
    }

    if (updated_config.cloud_storage_enabled()) {
        // The properties that cloud_storage::configuration requires
        // to be set if cloud storage is enabled.
        using config_properties_seq = std::vector<std::reference_wrapper<
          const config::property<std::optional<ss::sstring>>>>;

        switch (updated_config.cloud_storage_credentials_source.value()) {
        case model::cloud_credentials_source::config_file: {
            config_properties_seq s3_properties = {
              std::ref(updated_config.cloud_storage_region),
              std::ref(updated_config.cloud_storage_bucket),
              std::ref(updated_config.cloud_storage_access_key),
              std::ref(updated_config.cloud_storage_secret_key),
            };

            config_properties_seq abs_properties = {
              std::ref(updated_config.cloud_storage_azure_storage_account),
              std::ref(updated_config.cloud_storage_azure_container),
              std::ref(updated_config.cloud_storage_azure_shared_key),
            };

            std::array<config_properties_seq, 2> valid_configurations = {
              s3_properties, abs_properties};

            bool is_valid_configuration = std::any_of(
              valid_configurations.begin(),
              valid_configurations.end(),
              [](const auto& config) {
                  return std::none_of(
                    config.begin(), config.end(), [](const auto& prop) {
                        return prop() == std::nullopt;
                    });
              });

            if (!is_valid_configuration) {
                errors["cloud_storage_enabled"] = ssx::sformat(
                  "To enable cloud storage you need to configure S3 or "
                  "Azure "
                  "Blob Storage access. For S3 {} must be set. For ABS {} "
                  "must be set",
                  join_properties(s3_properties),
                  join_properties(abs_properties));
            }
        } break;
        case model::cloud_credentials_source::aws_instance_metadata:
        case model::cloud_credentials_source::gcp_instance_metadata:
        case model::cloud_credentials_source::sts: {
            // basic config checks for cloud_storage. for sts it is expected to
            // receive part of the configuration via env variables, while
            // aws_instance_metadata and gcp_instance_metadata do not require
            // extra configuration
            config_properties_seq properties = {
              std::ref(updated_config.cloud_storage_region),
              std::ref(updated_config.cloud_storage_bucket),
            };

            for (auto& p : properties) {
                if (p() == std::nullopt) {
                    errors[ss::sstring(p.get().name())] = ssx::sformat(
                      "Must be set when cloud storage enabled with "
                      "cloud_storage_credentials_source = {}",
                      updated_config.cloud_storage_credentials_source.value());
                }
            }
        } break;
        case model::cloud_credentials_source::azure_aks_oidc_federation: {
            // for azure_aks_oidc_federation it is expected to receive part of
            // the configuration via env variables. this check is just for
            // related cluster properties
            config_properties_seq properties = {
              std::ref(updated_config.cloud_storage_azure_storage_account),
              std::ref(updated_config.cloud_storage_azure_container),
            };

            for (auto& p : properties) {
                if (p() == std::nullopt) {
                    errors[ss::sstring(p.get().name())]
                      = "Must be set when cloud storage enabled with "
                        "cloud_storage_credentials_source = "
                        "azure_aks_oidc_federation";
                }
            }
        } break;
        case model::cloud_credentials_source::azure_vm_instance_metadata: {
            // azure_vm_instance_metadata requires an client_id to work
            // correctly
            config_properties_seq properties = {
              std::ref(updated_config.cloud_storage_azure_storage_account),
              std::ref(updated_config.cloud_storage_azure_container),
              std::ref(updated_config.cloud_storage_azure_managed_identity_id),
            };

            for (auto& p : properties) {
                if (p() == std::nullopt) {
                    errors[ss::sstring(p.get().name())]
                      = "Must be set when cloud storage enabled with "
                        "cloud_storage_credentials_source = "
                        "azure_vm_instance_metadata";
                }
            }
        } break;
        }
    }

    if (
      updated_config.enable_schema_id_validation
        != pandaproxy::schema_registry::schema_id_validation_mode::none
      && !schema_registry) {
        auto name = updated_config.enable_schema_id_validation.name();
        errors[ss::sstring(name)] = ssx::sformat(
          "{} requires schema_registry to be enabled in redpanda.yaml", name);
    }

    // cloud_storage_cache_size/size_percent validation
    if (auto invalid_cache = cloud_io::cache::validate_cache_config(
          updated_config);
        invalid_cache.has_value()) {
        auto name = ss::sstring(updated_config.cloud_storage_cache_size.name());
        errors[name] = invalid_cache.value();
    }

    // Validate iceberg REST catalog configuration
    auto catalog_err = config::validate_iceberg_rest_catalog_config(
      updated_config);
    if (catalog_err.has_value()) {
        errors[ss::sstring{updated_config.iceberg_catalog_type.name()}]
          = catalog_err.value();
    }

    // Validate iceberg authentication mode properties
    auto opt_err = config::validate_iceberg_rest_catalog_auth_mode(
      updated_config);
    if (opt_err.has_value()) {
        errors[ss::sstring{
          updated_config.iceberg_rest_catalog_authentication_mode.name()}]
          = opt_err.value();
    }

    // Validate cloud topics reconciliation intervals
    auto interval_err = config::validate_cloud_topics_reconciliation_intervals(
      updated_config);
    if (interval_err.has_value()) {
        errors[ss::sstring{
          updated_config.cloud_topics_reconciliation_min_interval.name()}]
          = interval_err.value();
    }

    auto pbp_err = config::validate_sane_partition_balancer_timeouts(
      updated_config);
    if (pbp_err.has_value()) {
        errors[ss::sstring{"partition_balancer_planner"}] = *pbp_err;
    }

    // Validate default_redpanda_storage_mode dependencies
    auto storage_mode_err = config::validate_default_redpanda_storage_mode(
      updated_config);
    if (storage_mode_err.has_value()) {
        errors[ss::sstring{updated_config.default_redpanda_storage_mode.name()}]
          = storage_mode_err.value();
    }
}
} // namespace

void admin_server::check_license(const ss::sstring& msg) const {
    if (_controller->get_feature_table().local().should_sanction()) {
        throw ss::httpd::base_exception(
          msg, ss::http::reply::status_type::forbidden);
    }
}

void admin_server::register_cluster_config_routes() {
    register_route<superuser>(
      ss::httpd::cluster_config_json::get_cluster_config_status,
      [this](std::unique_ptr<ss::http::request>) {
          auto& cfg = _controller->get_config_manager();
          return cfg
            .invoke_on(
              cluster::controller_stm_shard,
              [](cluster::config_manager& manager) {
                  return manager.get_projected_status();
              })
            .then([](auto statuses) {
                std::vector<
                  ss::httpd::cluster_config_json::cluster_config_status>
                  res;

                for (const auto& s : statuses) {
                    vlog(adminlog.trace, "status: {}", s.second);
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

                return ss::json::json_return_type(res);
            });
      });

    register_route<publik>(
      ss::httpd::cluster_config_json::get_cluster_config_schema,
      [](std::unique_ptr<ss::http::request>) {
          return ss::make_ready_future<ss::json::json_return_type>(
            util::generate_json_schema(config::shard_local_cfg()));
      });

    register_route<superuser, true>(
      ss::httpd::cluster_config_json::patch_cluster_config,
      [this](
        std::unique_ptr<ss::http::request> req,
        const request_auth_result& auth_state) {
          return patch_cluster_config_handler(std::move(req), auth_state);
      });
}

namespace {
ss::sstring redact_if_secret(std::string_view key, std::string_view val) {
    if (config::shard_local_cfg().contains(ss::sstring{key})) {
        const auto& p = config::shard_local_cfg().get(ss::sstring{key});
        if (p.is_secret()) {
            return "<redacted>";
        }
    }
    return ss::sstring{val};
}
ss::sstring format_upsert_redacted(
  const std::vector<cluster::cluster_property_kv>& upsert) {
    std::vector<ss::sstring> parts;
    parts.reserve(upsert.size());
    for (const auto& p : upsert) {
        parts.emplace_back(
          fmt::format("{}={}", p.key, redact_if_secret(p.key, p.value)));
    }
    return fmt::format("[{}]", fmt::join(parts, ", "));
}
} // namespace

ss::future<ss::json::json_return_type>
admin_server::patch_cluster_config_handler(
  std::unique_ptr<ss::http::request> req,
  const request_auth_result& auth_state) {
    static thread_local auto cluster_config_validator(
      make_cluster_config_validator());
    auto doc = co_await parse_json_body(req.get());
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
        json::StringBuffer val_buf;
        json::Writer<json::StringBuffer> w{val_buf};
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
    absl::flat_hash_set<ss::sstring> upsert_no_op_names;
    if (!get_boolean_query_param(*req, "force")) {
        // A scratch copy of configuration: we must not touch
        // the real live configuration object, that will be updated
        // by config_manager much after config is written to controller
        // log.
        auto cfg = config::make_config();

        // Populate the temporary config object with existing values
        config::shard_local_cfg().for_each(
          [&cfg](const config::base_property& p) {
              auto& tmp_p = cfg->get(p.name());
              tmp_p = p;
          });

        auto should_sanction
          = _controller->get_feature_table().local().should_sanction();

        // Configuration properties cannot do multi-property validation
        // themselves, so there is some special casing here for critical
        // properties.

        std::map<ss::sstring, ss::sstring> errors;
        for (const auto& [yaml_name, yaml_value] : update.upsert) {
            // Decode to a YAML object because that's what the property
            // interface expects.
            // Don't both catching ParserException: this was encoded
            // just a few lines above.
            auto val = YAML::Load(yaml_value);

            if (!cfg->contains(yaml_name)) {
                errors[yaml_name] = "Unknown property";
                continue;
            }
            auto& property = cfg->get(yaml_name);

            try {
                auto validation_err = property.validate(val);
                if (validation_err.has_value()) {
                    errors[yaml_name] = validation_err.value().error_message();
                    vlog(
                      adminlog.warn,
                      "Invalid {}: '{}' ({})",
                      yaml_name,
                      property.format_raw(yaml_value),
                      validation_err.value().error_message());
                } else if (auto restricted_err = property.check_restricted(val);
                           restricted_err.has_value() && should_sanction) {
                    errors[yaml_name] = restricted_err.value().error_message();
                    vlog(
                      adminlog.warn,
                      "Rejected {}: '{}' ({})",
                      yaml_name,
                      property.format_raw(yaml_value),
                      restricted_err.value().error_message());
                } else {
                    // In case any property subclass might throw
                    // from it's value setter even after a non-throwing
                    // call to validate (if this happens validate() was
                    // implemented wrongly, but let's be safe)
                    auto changed = property.set_value(val);
                    if (!changed) {
                        upsert_no_op_names.insert(yaml_name);
                    }
                }
            } catch (const YAML::BadConversion& e) {
                // Be helpful, and give the user an example of what
                // the setting should look like, if we have one.
                ss::sstring example;
                auto example_opt = property.example();
                if (example_opt.has_value()) {
                    example = fmt::format(
                      ", for example '{}'", example_opt.value());
                }

                auto message = fmt::format(
                  "expected type {}{}", property.type_name(), example);

                // Special case: we get BadConversion for out-of-range
                // values on smaller integer sizes (e.g. too
                // large value to an int16_t property).
                // ("integer" is a magic string but it's a stable part
                //  of our outward interface)
                if (property.type_name() == "integer") {
                    int64_t n{0};
                    try {
                        n = val.as<int64_t>();
                        // It's a valid integer:
                        message = fmt::format("out of range: '{}'", n);
                    } catch (...) {
                        // This was not an out-of-bounds case, use
                        // the type error message
                    }
                }

                errors[yaml_name] = message;
                vlog(
                  adminlog.warn,
                  "Invalid {}: '{}' ({})",
                  yaml_name,
                  property.format_raw(yaml_value),
                  std::current_exception());
            } catch (...) {
                auto message = fmt::format("{}", std::current_exception());
                errors[yaml_name] = message;
                vlog(
                  adminlog.warn,
                  "Invalid {}: '{}' ({})",
                  yaml_name,
                  property.format_raw(yaml_value),
                  message);
            }
        }

        for (const auto& key : update.remove) {
            if (cfg->contains(key)) {
                cfg->get(key).reset();
            } else {
                errors[key] = "Unknown property";
            }
        }

        // After checking each individual property, check for
        // any multi-property validation errors
        config_multi_property_validation(
          auth_state.get_username(), _schema_registry, update, *cfg, errors);

        if (!errors.empty()) {
            json::StringBuffer buf;
            json::Writer<json::StringBuffer> w(buf);

            w.StartObject();
            for (const auto& e : errors) {
                w.Key(e.first.data(), e.first.size());
                w.String(e.second.data(), e.second.size());
            }
            w.EndObject();

            throw ss::httpd::base_exception(
              buf.GetString(),
              ss::http::reply::status_type::bad_request,
              "json");
        }
    }

    if (get_boolean_query_param(*req, "dry_run")) {
        auto current_version
          = co_await _controller->get_config_manager().invoke_on(
            cluster::config_manager::shard,
            [](cluster::config_manager& cm) { return cm.get_version(); });

        // A dry run doesn't really need a result, but it's simpler for
        // the API definition if we return the same structure as a
        // normal write.
        ss::httpd::cluster_config_json::cluster_config_write_result result;
        result.config_version = current_version;
        co_return ss::json::json_return_type(result);
    }

    if (
      update.upsert.size() == upsert_no_op_names.size()
      && update.remove.empty()) {
        vlog(
          adminlog.trace,
          "patch_cluster_config: ignoring request, {} upserts resulted "
          "in no-ops",
          update.upsert.size());
        auto current_version
          = co_await _controller->get_config_manager().invoke_on(
            cluster::config_manager::shard,
            [](cluster::config_manager& cm) { return cm.get_version(); });
        ss::httpd::cluster_config_json::cluster_config_write_result result;
        result.config_version = current_version;
        co_return ss::json::json_return_type(result);
    }

    vlog(
      adminlog.trace,
      "patch_cluster_config: {} upserts, {} removes",
      update.upsert.size(),
      update.remove.size());

    auto upserts_str = format_upsert_redacted(update.upsert);
    auto removes_str = update.remove;

    auto patch_result
      = co_await _controller->get_config_frontend().local().patch(
        std::move(update), model::timeout_clock::now() + 5s);

    co_await throw_on_error(*req, patch_result.errc, model::controller_ntp);

    vlog(
      adminlog.info,
      "Successfully updated cluster configuration upsert={} remove={} with "
      "version {}",
      upserts_str,
      removes_str,
      patch_result.version);

    ss::httpd::cluster_config_json::cluster_config_write_result result;
    result.config_version = patch_result.version;
    co_return ss::json::json_return_type(result);
}

ss::future<ss::json::json_return_type>
admin_server::raft_transfer_leadership_handler(
  std::unique_ptr<ss::http::request> req) {
    raft::group_id group_id;
    try {
        group_id = raft::group_id(std::stoll(req->get_path_param("group_id")));
    } catch (...) {
        throw ss::httpd::bad_param_exception(
          fmt::format(
            "Raft group id must be an integer: {}",
            req->get_path_param("group_id")));
    }

    if (group_id() < 0) {
        throw ss::httpd::bad_param_exception(
          fmt::format("Invalid raft group id {}", group_id));
    }

    auto shard = _shard_table.local().shard_for(group_id);
    if (!shard) {
        throw ss::httpd::not_found_exception(
          fmt::format("Raft group {} not found", group_id));
    }

    std::optional<model::node_id> target;
    if (auto node = req->get_query_param("target"); !node.empty()) {
        try {
            target = model::node_id(std::stoi(node));
        } catch (...) {
            throw ss::httpd::bad_param_exception(
              fmt::format("Target node id must be an integer: {}", node));
        }
        if (*target < 0) {
            throw ss::httpd::bad_param_exception(
              fmt::format("Invalid target node id {}", *target));
        }
    }

    vlog(
      adminlog.info,
      "Leadership transfer request for raft group {} to node {}",
      group_id,
      target);

    co_return co_await _partition_manager.invoke_on(
      *shard,
      [group_id, target, this, req = std::move(req)](
        cluster::partition_manager& pm) mutable {
          auto partition = pm.partition_for(group_id);
          if (!partition) {
              throw ss::httpd::not_found_exception();
          }
          const auto ntp = partition->ntp();
          auto r = raft::transfer_leadership_request{
            .group = partition->group(),
            .target = target,
          };
          return partition->transfer_leadership(r).then(
            [this, ntp, req = std::move(req)](auto err) {
                return throw_on_error(*req, err, ntp).then([] {
                    return ss::json::json_return_type(ss::json::json_void());
                });
            });
      });
}

ss::future<ss::json::json_return_type>
admin_server::get_raft_recovery_status_handler(
  std::unique_ptr<ss::http::request>) {
    ss::httpd::raft_json::recovery_status result;

    // Aggregate recovery status from all shards
    auto s = co_await _raft_group_manager.map_reduce0(
      [](auto& rgm) -> raft::recovery_status {
          return rgm.get_recovery_status();
      },
      raft::recovery_status{},
      [](raft::recovery_status acc, raft::recovery_status update) {
          acc.merge(update);
          return acc;
      });

    result.partitions_to_recover = s.partitions_to_recover;
    result.partitions_active = s.partitions_active;
    result.offsets_pending = s.offsets_pending;
    co_return result;
}

void admin_server::register_raft_routes() {
    register_route<superuser>(
      ss::httpd::raft_json::raft_transfer_leadership,
      [this](std::unique_ptr<ss::http::request> req) {
          return raft_transfer_leadership_handler(std::move(req));
      });

    register_route<auth_level::user>(
      ss::httpd::raft_json::get_raft_recovery_status,
      [this](std::unique_ptr<ss::http::request> req) {
          return get_raft_recovery_status_handler(std::move(req));
      });
}

void admin_server::register_status_routes() {
    register_route<publik>(
      ss::httpd::status_json::ready,
      [this](std::unique_ptr<ss::http::request>) {
          std::unordered_map<ss::sstring, ss::sstring> status_map{
            {"status", _ready ? "ready" : "booting"}};
          return ss::make_ready_future<ss::json::json_return_type>(status_map);
      });
}

namespace {
json::validator make_feature_put_validator() {
    const std::string_view schema = R"(
{
    "type": "object",
    "properties": {
        "state": {
            "type": "string",
            "enum": ["active", "disabled"]
        }
    },
    "additionalProperties": false,
    "required": ["state"]
}
)";
    return json::validator(schema);
}

/// Features are state machines, with multiple 'disabled' states.  Simplify
/// this into the higher level states the the admin API reports to users.
/// (see state machine diagram in feature_state.h)
ss::httpd::features_json::feature_state::feature_state_state
feature_state_to_high_level(features::feature_state::state state) {
    switch (state) {
    case features::feature_state::state::active:
        return ss::httpd::features_json::feature_state::feature_state_state::
          active;
        break;
    case features::feature_state::state::unavailable:
        return ss::httpd::features_json::feature_state::feature_state_state::
          unavailable;
        break;
    case features::feature_state::state::available:
        return ss::httpd::features_json::feature_state::feature_state_state::
          available;
        break;
    case features::feature_state::state::preparing:
        return ss::httpd::features_json::feature_state::feature_state_state::
          preparing;
        break;
    case features::feature_state::state::disabled_clean:
    case features::feature_state::state::disabled_active:
    case features::feature_state::state::disabled_preparing:
        return ss::httpd::features_json::feature_state::feature_state_state::
          disabled;
        break;

        // Exhaustive match
    }
}
} // namespace

ss::future<ss::json::json_return_type>
admin_server::put_feature_handler(std::unique_ptr<ss::http::request> req) {
    static thread_local auto feature_put_validator(
      make_feature_put_validator());

    auto doc = co_await parse_json_body(req.get());
    apply_validator(feature_put_validator, doc);

    auto feature_name = req->get_path_param("feature_name");

    auto feature_id = _controller->get_feature_table().local().resolve_name(
      feature_name);
    if (!feature_id.has_value()) {
        throw ss::httpd::bad_request_exception("Unknown feature name");
    }

    // Retrieve the current state and map to high level disabled/enabled value
    auto& feature_state = _controller->get_feature_table().local().get_state(
      feature_id.value());
    auto current_state = feature_state_to_high_level(feature_state.get_state());

    cluster::feature_update_action action{.feature_name = feature_name};
    auto& new_state_str = doc["state"];
    if (new_state_str == "active") {
        if (
          current_state
          == ss::httpd::features_json::feature_state::feature_state_state::
            active) {
            vlog(
              adminlog.info,
              "Ignoring request to activate feature '{}', already active",
              feature_name);
            co_return ss::json::json_void();
        }
        action.action = cluster::feature_update_action::action_t::activate;
    } else if (new_state_str == "disabled") {
        if (
          current_state
          == ss::httpd::features_json::feature_state::feature_state_state::
            disabled) {
            vlog(
              adminlog.info,
              "Ignoring request to disable feature '{}', already disabled",
              feature_name);
            co_return ss::json::json_void();
        }
        action.action = cluster::feature_update_action::action_t::deactivate;
    } else {
        throw ss::httpd::bad_request_exception("Invalid state");
    }

    if (need_redirect_to_leader(model::controller_ntp, _metadata_cache)) {
        throw co_await redirect_to_leader(*req, model::controller_ntp);
    }

    auto& fm = _controller->get_feature_manager();
    auto err = co_await fm.invoke_on(
      cluster::feature_manager::backend_shard,
      [action](cluster::feature_manager& fm) {
          return fm.write_action(action);
      });
    if (err) {
        throw ss::httpd::bad_request_exception(fmt::format("{}", err));
    } else {
        co_return ss::json::json_void();
    }
}

ss::future<ss::json::json_return_type>
admin_server::put_license_handler(std::unique_ptr<ss::http::request> req) {
    auto raw_license = co_await ss::util::read_entire_stream_contiguous(
      *req->content_stream);
    if (raw_license.empty()) {
        throw ss::httpd::bad_request_exception(
          "Missing redpanda license from request body");
    }

    try {
        boost::trim_if(raw_license, boost::is_any_of(" \n\r"));
        auto license = security::make_license(raw_license);
        if (license.is_expired()) {
            throw ss::httpd::bad_request_exception(
              fmt::format("License is expired: {}", license));
        }

        if (need_redirect_to_leader(model::controller_ntp, _metadata_cache)) {
            // In order that we can do a reliable idempotence check, run on the
            // controller leader
            throw co_await redirect_to_leader(*req, model::controller_ntp);
        }

        const auto& ft = _controller->get_feature_table().local();
        const auto& loaded_license = ft.get_license();
        if (loaded_license && (*loaded_license == license)) {
            /// Loaded license is idential to license in request, do
            /// nothing and return 200(OK) for idempotence
            vlog(
              adminlog.info,
              "Attempted to load identical license, doing nothing: {}",
              license);
            co_return ss::json::json_void();
        }
        auto& fm = _controller->get_feature_manager();
        auto err = co_await fm.invoke_on(
          cluster::feature_manager::backend_shard,
          [license = std::move(license)](cluster::feature_manager& fm) mutable {
              return fm.update_license(std::move(license));
          });
        co_await throw_on_error(*req, err, model::controller_ntp);
    } catch (const security::license_malformed_exception& ex) {
        throw ss::httpd::bad_request_exception(
          fmt::format("License is malformed: {}", ex.what()));
    } catch (const security::license_invalid_exception& ex) {
        throw ss::httpd::bad_request_exception(
          fmt::format("License is invalid: {}", ex.what()));
    }
    co_return ss::json::json_void();
}

ss::future<ss::json::json_return_type>
admin_server::get_enterprise_handler(std::unique_ptr<ss::http::request>) {
    using status = ss::httpd::features_json::enterprise_response::
      enterprise_response_license_status;

    const auto& license
      = _controller->get_feature_table().local().get_license();
    auto license_status = [&license]() {
        auto present = license.has_value();
        auto exp = present && license.value().is_expired();
        if (exp) {
            return status::expired;
        }
        if (present) {
            return status::valid;
        }
        return status::not_present;
    }();

    auto& mgr = _controller->get_feature_manager();
    const auto report = co_await mgr.invoke_on(
      cluster::feature_manager::backend_shard,
      [](const cluster::feature_manager& fm) {
          return fm.report_enterprise_features();
      });

    ss::httpd::features_json::enterprise_response res;
    res.license_status = license_status;
    res.violation = license_status != status::valid && report.any();
    auto insert_feature =
      [&res](features::license_required_feature feat, bool enabled) {
          ss::httpd::features_json::enterprise_feature elt;
          elt.name = fmt::format("{}", feat);
          elt.enabled = enabled;
          res.features.push(elt);
      };

    for (auto feat : report.enabled()) {
        insert_feature(feat, true);
    }

    for (auto feat : report.disabled()) {
        insert_feature(feat, false);
    }

    co_return ss::json::json_return_type{res};
}

void admin_server::register_features_routes() {
    register_route<user>(
      ss::httpd::features_json::get_features,
      [this](std::unique_ptr<ss::http::request>) {
          ss::httpd::features_json::features_response res;

          const auto& ft = _controller->get_feature_table().local();
          auto version = ft.get_active_version();

          res.cluster_version = version;
          res.original_cluster_version = ft.get_original_version();
          res.node_earliest_version = ft.get_earliest_logical_version();
          res.node_latest_version = ft.get_latest_logical_version();
          for (const auto& fs : ft.get_feature_state()) {
              ss::httpd::features_json::feature_state item;
              vlog(
                adminlog.trace,
                "feature_state: {} {}",
                fs.spec.name,
                fs.get_state());
              item.name = ss::sstring(fs.spec.name);
              item.state = feature_state_to_high_level(fs.get_state());

              switch (fs.get_state()) {
              case features::feature_state::state::active:
              case features::feature_state::state::preparing:
              case features::feature_state::state::disabled_active:
              case features::feature_state::state::disabled_preparing:
                  item.was_active = true;
                  break;
              default:
                  item.was_active = false;
              }

              res.features.push(item);
          }

          // Report all retired features as active (the code they previously
          // guarded is now on by default).  This enables external programs
          // to check the state of a particular feature flag in perpetuity
          // without having to deal with the ambiguous case of the feature
          // being missing (i.e. unsure if redpanda is too old to have
          // the feature flag, or too new to have it).
          for (const auto& retired_name : features::retired_features) {
              ss::httpd::features_json::feature_state item;
              item.name = ss::sstring(retired_name);
              item.state = ss::httpd::features_json::feature_state::
                feature_state_state::active;
              item.was_active = true;
              res.features.push(item);
          }

          return ss::make_ready_future<ss::json::json_return_type>(
            std::move(res));
      });

    register_route<superuser>(
      ss::httpd::features_json::put_feature,
      [this](std::unique_ptr<ss::http::request> req) {
          return put_feature_handler(std::move(req));
      });

    register_route<publik, true>(
      ss::httpd::features_json::get_license,
      [this](
        std::unique_ptr<ss::http::request>,
        const request_auth_result& auth_result) {
          ss::httpd::features_json::license_response res;
          res.loaded = false;
          const auto& ft = _controller->get_feature_table().local();
          const auto& license = ft.get_license();
          if (license) {
              res.loaded = true;
              ss::httpd::features_json::license_contents lc;
              if (auth_result.is_authenticated()) {
                  lc.format_version = license->format_version;
                  lc.org = license->organization;
              }
              lc.type = license->get_type();
              lc.expires = license->expiry.count();
              lc.sha256 = license->checksum;
              lc.products = license->products;
              res.license = lc;
          }
          return ss::make_ready_future<ss::json::json_return_type>(
            std::move(res));
      });

    register_route<superuser>(
      ss::httpd::features_json::put_license,
      [this](std::unique_ptr<ss::http::request> req) {
          return put_license_handler(std::move(req));
      });
    register_route<user>(
      ss::httpd::features_json::get_enterprise,
      [this](std::unique_ptr<ss::http::request> req) {
          return get_enterprise_handler(std::move(req));
      });
}

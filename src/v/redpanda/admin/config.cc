#include "config/configuration.h"
#include "config/node_config.h"
#include "redpanda/admin/api-doc/config.json.h"
#include "redpanda/admin/server.h"
#include "vlog.h"

namespace admin {

void admin_server::register_config_routes() {
    static ss::httpd::handle_function get_config_handler =
      []([[maybe_unused]] ss::const_req req, ss::reply& reply) {
          rapidjson::StringBuffer buf;
          rapidjson::Writer<rapidjson::StringBuffer> writer(buf);
          config::shard_local_cfg().to_json(writer);

          reply.set_status(ss::httpd::reply::status_type::ok, buf.GetString());
          return "";
      };

    auto get_config_handler_f = new ss::httpd::function_handler{
      get_config_handler, "json"};

    ss::httpd::config_json::get_config.set(
      _server._routes, get_config_handler_f);

    static ss::httpd::handle_function get_node_config_handler =
      []([[maybe_unused]] ss::const_req req, ss::reply& reply) {
          rapidjson::StringBuffer buf;
          rapidjson::Writer<rapidjson::StringBuffer> writer(buf);
          config::node().to_json(writer);

          reply.set_status(ss::httpd::reply::status_type::ok, buf.GetString());
          return "";
      };

    auto get_node_config_handler_f = new ss::httpd::function_handler{
      get_node_config_handler, "json"};

    ss::httpd::config_json::get_node_config.set(
      _server._routes, get_node_config_handler_f);

    ss::httpd::config_json::set_log_level.set(
      _server._routes, [this](ss::const_req req) {
          auto name = req.param["name"];

          // current level: will be used revert after a timeout (optional)
          ss::log_level cur_level;
          try {
              cur_level = ss::global_logger_registry().get_logger_level(name);
          } catch (std::out_of_range&) {
              throw ss::httpd::bad_param_exception(fmt::format(
                "Cannot set log level: unknown logger {{{}}}", name));
          }

          // decode new level
          ss::log_level new_level;
          try {
              new_level = boost::lexical_cast<ss::log_level>(
                req.get_query_param("level"));
          } catch (const boost::bad_lexical_cast& e) {
              throw ss::httpd::bad_param_exception(fmt::format(
                "Cannot set log level for {{{}}}: unknown level {{{}}}",
                name,
                req.get_query_param("level")));
          }

          // how long should the new log level be active
          std::optional<std::chrono::seconds> expires;
          if (auto e = req.get_query_param("expires"); !e.empty()) {
              try {
                  expires = std::chrono::seconds(
                    boost::lexical_cast<unsigned int>(e));
              } catch (const boost::bad_lexical_cast& e) {
                  throw ss::httpd::bad_param_exception(fmt::format(
                    "Cannot set log level for {{{}}}: invalid expires value "
                    "{{{}}}",
                    name,
                    e));
              }
          }

          vlog(
            logger.info,
            "Set log level for {{{}}}: {} -> {}",
            name,
            cur_level,
            new_level);

          ss::global_logger_registry().set_logger_level(name, new_level);

          if (!expires) {
              // if no expiration was given, then use some reasonable default
              // that will prevent the system from remaining in a non-optimal
              // state (e.g. trace logging) indefinitely.
              expires = std::chrono::minutes(10);
          }

          // expires=0 is same as not specifying it at all
          if (expires->count() > 0) {
              auto when = ss::timer<>::clock::now() + expires.value();
              auto res = _log_level_resets.try_emplace(name, cur_level, when);
              if (!res.second) {
                  res.first->second.expires = when;
              }
          } else {
              // perm change. no need to store prev level
              _log_level_resets.erase(name);
          }

          rearm_log_level_timer();

          return ss::json::json_return_type(ss::json::json_void());
      });
}

} // namespace admin

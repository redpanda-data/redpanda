#include "finjector/hbadger.h"

#include "redpanda/admin/api-doc/hbadger.json.h"
#include "redpanda/admin/server.h"
#include "vlog.h"

#include <seastar/util/later.hh>

namespace admin {

void admin_server::register_hbadger_routes() {
    /**
     * we always register `v1/failure-probes` route. It will ALWAYS return empty
     * list of probes in production mode, and flag indicating that honey badger
     * is disabled
     */

    if constexpr (!finjector::honey_badger::is_enabled()) {
        ss::httpd::hbadger_json::get_failure_probes.set(
          _server._routes, [](std::unique_ptr<ss::httpd::request>) {
              ss::httpd::hbadger_json::failure_injector_status status;
              status.enabled = false;
              return ss::make_ready_future<ss::json::json_return_type>(
                std::move(status));
          });
        return;
    }
    ss::httpd::hbadger_json::get_failure_probes.set(
      _server._routes, [](std::unique_ptr<ss::httpd::request>) {
          auto modules = finjector::shard_local_badger().modules();
          ss::httpd::hbadger_json::failure_injector_status status;
          status.enabled = true;

          for (auto& m : modules) {
              ss::httpd::hbadger_json::failure_probes pr;
              pr.module = m.first.data();
              for (auto& p : m.second) {
                  pr.points.push(p.data());
              }
              status.probes.push(pr);
          }

          return ss::make_ready_future<ss::json::json_return_type>(
            std::move(status));
      });
    /*
     * Enable failure injector
     */
    static constexpr std::string_view delay_type = "delay";
    static constexpr std::string_view exception_type = "exception";
    static constexpr std::string_view terminate_type = "terminate";

    ss::httpd::hbadger_json::set_failure_probe.set(
      _server._routes, [](std::unique_ptr<ss::httpd::request> req) {
          auto m = req->param["module"];
          auto p = req->param["point"];
          auto type = req->param["type"];
          vlog(
            logger.info,
            "Request to set failure probe of type '{}' in  '{}' at point '{}'",
            type,
            m,
            p);
          auto f = ss::now();

          if (type == delay_type) {
              f = ss::smp::invoke_on_all(
                [m, p] { finjector::shard_local_badger().set_delay(m, p); });
          } else if (type == exception_type) {
              f = ss::smp::invoke_on_all([m, p] {
                  finjector::shard_local_badger().set_exception(m, p);
              });
          } else if (type == terminate_type) {
              f = ss::smp::invoke_on_all([m, p] {
                  finjector::shard_local_badger().set_termination(m, p);
              });
          } else {
              throw ss::httpd::bad_param_exception(fmt::format(
                "Type parameter has to be one of "
                "['{}','{}','{}']",
                delay_type,
                exception_type,
                terminate_type));
          }

          return f.then(
            [] { return ss::json::json_return_type(ss::json::json_void()); });
      });
    /*
     * Remove all failure injectors at given point
     */
    ss::httpd::hbadger_json::delete_failure_probe.set(
      _server._routes, [](std::unique_ptr<ss::httpd::request> req) {
          auto m = req->param["module"];
          auto p = req->param["point"];
          vlog(
            logger.info,
            "Request to unset failure probe '{}' at point '{}'",
            m,
            p);
          return ss::smp::invoke_on_all(
                   [m, p] { finjector::shard_local_badger().unset(m, p); })
            .then(
              [] { return ss::json::json_return_type(ss::json::json_void()); });
      });
}

} // namespace admin

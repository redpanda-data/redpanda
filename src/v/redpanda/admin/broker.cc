#include "cluster/controller.h"
#include "cluster/members_frontend.h"
#include "cluster/metadata_cache.h"
#include "redpanda/admin/api-doc/broker.json.h"
#include "redpanda/admin/server.h"

namespace admin {

namespace {
model::node_id parse_broker_id(const ss::httpd::request& req) {
    try {
        return model::node_id(
          boost::lexical_cast<model::node_id::type>(req.param["id"]));
    } catch (...) {
        throw ss::httpd::bad_param_exception(
          fmt::format("Broker id: {}, must be an integer", req.param["id"]));
    }
}
} // namespace

void admin_server::register_broker_routes() {
    ss::httpd::broker_json::get_cluster_view.set(
      _server._routes, [this](std::unique_ptr<ss::httpd::request>) {
          cluster::node_report_filter filter;

          return _controller->get_health_monitor()
            .local()
            .get_cluster_health(
              cluster::cluster_report_filter{
                .node_report_filter = std::move(filter),
              },
              cluster::force_refresh::no,
              model::no_timeout)
            .then([this](result<cluster::cluster_health_report> h_report) {
                if (h_report.has_error()) {
                    throw ss::httpd::base_exception(
                      fmt::format(
                        "Unable to get cluster health: {}",
                        h_report.error().message()),
                      ss::httpd::reply::status_type::service_unavailable);
                }

                std::map<model::node_id, ss::httpd::broker_json::broker> result;

                auto& members_table = _controller->get_members_table().local();
                for (auto& broker : members_table.all_brokers()) {
                    ss::httpd::broker_json::broker b;
                    b.node_id = broker->id();
                    b.num_cores = broker->properties().cores;
                    b.membership_status = fmt::format(
                      "{}", broker->get_membership_state());
                    b.is_alive = true;
                    result[broker->id()] = b;
                }

                for (auto& ns : h_report.value().node_states) {
                    auto it = result.find(ns.id);
                    if (it == result.end()) {
                        continue;
                    }
                    it->second.is_alive = (bool)ns.is_alive;

                    auto r_it = std::find_if(
                      h_report.value().node_reports.begin(),
                      h_report.value().node_reports.end(),
                      [id = ns.id](const cluster::node_health_report& nhr) {
                          return nhr.id == id;
                      });
                    if (r_it != h_report.value().node_reports.end()) {
                        it->second.version = r_it->local_state.redpanda_version;
                        for (auto& ds : r_it->local_state.disks) {
                            ss::httpd::broker_json::disk_space_info dsi;
                            dsi.path = ds.path;
                            dsi.free = ds.free;
                            dsi.total = ds.total;
                            it->second.disk_space.push(dsi);
                        }
                    }
                }

                ss::httpd::broker_json::cluster_view ret;
                ret.version = members_table.version();
                for (auto& [_, b] : result) {
                    ret.brokers.push(b);
                }

                return ss::make_ready_future<ss::json::json_return_type>(
                  std::move(ret));
            });
      });

    ss::httpd::broker_json::get_brokers.set(
      _server._routes, [this](std::unique_ptr<ss::httpd::request>) {
          cluster::node_report_filter filter;

          return _controller->get_health_monitor()
            .local()
            .get_cluster_health(
              cluster::cluster_report_filter{
                .node_report_filter = std::move(filter),
              },
              cluster::force_refresh::no,
              model::no_timeout)
            .then([this](result<cluster::cluster_health_report> h_report) {
                if (h_report.has_error()) {
                    throw ss::httpd::base_exception(
                      fmt::format(
                        "Unable to get cluster health: {}",
                        h_report.error().message()),
                      ss::httpd::reply::status_type::service_unavailable);
                }

                std::vector<ss::httpd::broker_json::broker> res;

                for (auto& ns : h_report.value().node_states) {
                    auto broker = _metadata_cache.local().get_broker(ns.id);
                    if (!broker) {
                        continue;
                    }
                    auto& b = res.emplace_back();
                    b.node_id = ns.id;
                    b.num_cores = (*broker)->properties().cores;
                    b.membership_status = fmt::format(
                      "{}", ns.membership_state);
                    b.is_alive = (bool)ns.is_alive;

                    auto r_it = std::find_if(
                      h_report.value().node_reports.begin(),
                      h_report.value().node_reports.end(),
                      [id = ns.id](const cluster::node_health_report& nhr) {
                          return nhr.id == id;
                      });
                    if (r_it != h_report.value().node_reports.end()) {
                        b.version = r_it->local_state.redpanda_version;
                        for (auto& ds : r_it->local_state.disks) {
                            ss::httpd::broker_json::disk_space_info dsi;
                            dsi.path = ds.path;
                            dsi.free = ds.free;
                            dsi.total = ds.total;
                            b.disk_space.push(dsi);
                        }
                    }
                }

                return ss::make_ready_future<ss::json::json_return_type>(
                  std::move(res));
            });
      });

    ss::httpd::broker_json::get_broker.set(
      _server._routes, [this](std::unique_ptr<ss::httpd::request> req) {
          model::node_id id = parse_broker_id(*req);
          auto broker = _metadata_cache.local().get_broker(id);
          if (!broker) {
              throw ss::httpd::not_found_exception(
                fmt::format("broker with id: {} not found", id));
          }
          ss::httpd::broker_json::broker ret;
          ret.node_id = (*broker)->id();
          ret.num_cores = (*broker)->properties().cores;
          ret.membership_status = fmt::format(
            "{}", (*broker)->get_membership_state());
          return ss::make_ready_future<ss::json::json_return_type>(ret);
      });

    ss::httpd::broker_json::decommission.set(
      _server._routes,
      [this](std::unique_ptr<ss::httpd::request> req)
        -> ss::future<ss::json::json_return_type> {
          model::node_id id = parse_broker_id(*req);

          auto ec = co_await _controller->get_members_frontend()
                      .local()
                      .decommission_node(id);

          co_await throw_on_error(*req, ec, model::controller_ntp, id);
          co_return ss::json::json_void();
      });
    ss::httpd::broker_json::recommission.set(
      _server._routes,
      [this](std::unique_ptr<ss::httpd::request> req)
        -> ss::future<ss::json::json_return_type> {
          model::node_id id = parse_broker_id(*req);

          auto ec = co_await _controller->get_members_frontend()
                      .local()
                      .recommission_node(id);
          co_await throw_on_error(*req, ec, model::controller_ntp, id);
          co_return ss::json::json_void();
      });
}

} // namespace admin

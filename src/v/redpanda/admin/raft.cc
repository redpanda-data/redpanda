#include "cluster/partition_manager.h"
#include "cluster/shard_table.h"
#include "redpanda/admin/api-doc/raft.json.h"
#include "redpanda/admin/server.h"
#include "vlog.h"

namespace admin {

void admin_server::register_raft_routes() {
    ss::httpd::raft_json::raft_transfer_leadership.set(
      _server._routes, [this](std::unique_ptr<ss::httpd::request> req) {
          raft::group_id group_id;
          try {
              group_id = raft::group_id(std::stoll(req->param["group_id"]));
          } catch (...) {
              throw ss::httpd::bad_param_exception(fmt::format(
                "Raft group id must be an integer: {}",
                req->param["group_id"]));
          }

          if (group_id() < 0) {
              throw ss::httpd::bad_param_exception(
                fmt::format("Invalid raft group id {}", group_id));
          }

          if (!_shard_table.local().contains(group_id)) {
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
            logger.info,
            "Leadership transfer request for raft group {} to node {}",
            group_id,
            target);

          auto shard = _shard_table.local().shard_for(group_id);

          return _partition_manager.invoke_on(
            shard,
            [group_id, target, this, req = std::move(req)](
              cluster::partition_manager& pm) mutable {
                auto consensus = pm.consensus_for(group_id);
                if (!consensus) {
                    throw ss::httpd::not_found_exception();
                }
                const auto ntp = consensus->ntp();
                return consensus->do_transfer_leadership(target).then(
                  [this, req = std::move(req), ntp](std::error_code err)
                    -> ss::future<ss::json::json_return_type> {
                      co_await throw_on_error(*req, err, ntp);
                      co_return ss::json::json_return_type(
                        ss::json::json_void());
                  });
            });
      });
}

} // namespace admin

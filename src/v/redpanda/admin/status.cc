#include "redpanda/admin/api-doc/status.json.h"
#include "redpanda/admin/server.h"

#include <unordered_map>

namespace admin {

void admin_server::register_status_routes() {
    ss::httpd::status_json::ready.set(
      _server._routes, [this](std::unique_ptr<ss::httpd::request>) {
          std::unordered_map<ss::sstring, ss::sstring> status_map{
            {"status", _ready ? "ready" : "booting"}};
          return ss::make_ready_future<ss::json::json_return_type>(status_map);
      });
}

} // namespace admin

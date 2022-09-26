#include "cluster/node_status_rpc_handler.h"

namespace cluster {

node_status_rpc_handler::node_status_rpc_handler(
  ss::scheduling_group sg,
  ss::smp_service_group ssg,
  ss::sharded<node_status_backend>& node_status_backend)
  : node_status_rpc_service(sg, ssg)
  , _node_status_backend(node_status_backend) {}

ss::future<node_status_reply> node_status_rpc_handler::node_status(
  node_status_request&& r, rpc::streaming_context&) {
    return _node_status_backend.invoke_on(
      node_status_backend::shard, [r = std::move(r)](auto& service) {
          return service.process_request(std::move(r));
      });
}

} // namespace cluster

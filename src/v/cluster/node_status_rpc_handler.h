#pragma once

#include "cluster/node_status_backend.h"
#include "cluster/node_status_rpc_service.h"
#include "cluster/node_status_rpc_types.h"

namespace cluster {

class node_status_rpc_handler final : public node_status_rpc_service {
public:
    node_status_rpc_handler(
      ss::scheduling_group,
      ss::smp_service_group,
      ss::sharded<node_status_backend>&);

    virtual ss::future<node_status_reply>
    node_status(node_status_request&&, rpc::streaming_context&) override;

private:
    ss::sharded<node_status_backend>& _node_status_backend;
};

} // namespace cluster

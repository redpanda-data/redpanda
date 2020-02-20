#pragma once
#include "cluster/controller_service.h"

#include <seastar/core/sharded.hh>

namespace cluster {
class controller;
class metadata_cache;

class service : public controller_service {
public:
    service(
      ss::scheduling_group,
      ss::smp_service_group,
      ss::sharded<controller>&,
      ss::sharded<metadata_cache>&);

    virtual ss::future<join_reply>
    join(join_request&&, rpc::streaming_context&) override;

private:
    ss::sharded<controller>& _controller;
    ss::sharded<metadata_cache>& _md_cache;
};
} // namespace cluster

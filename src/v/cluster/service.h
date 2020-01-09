#pragma once
#include "cluster/controller_service.h"

namespace cluster {
class controller;

class service : public controller_service {
public:
    service(ss::scheduling_group, ss::smp_service_group, controller&);

    virtual ss::future<join_reply>
    join(join_request&&, rpc::streaming_context&) override;

private:
    controller& _controller;
};
} // namespace cluster

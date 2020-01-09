#include "cluster/service.h"

#include "cluster/controller.h"
#include "cluster/types.h"

namespace cluster {
service::service(
  ss::scheduling_group sg, ss::smp_service_group ssg, controller& c)
  : controller_service(sg, ssg)
  , _controller(c) {}

ss::future<join_reply>
service::join(join_request&& req, rpc::streaming_context&) {
    return ss::with_scheduling_group(
      get_scheduling_group(), [this, broker = std::move(req.node)]() mutable {
          return ss::smp::submit_to(
                   controller::shard,
                   get_smp_service_group(),
                   [this, broker = std::move(broker)]() mutable {
                       return _controller.process_join_request(
                         std::move(broker));
                   })
            .then([] { return join_reply{true}; });
      });
}
} // namespace cluster

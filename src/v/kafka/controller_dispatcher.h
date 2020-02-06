#pragma once
#include "cluster/controller.h"

namespace kafka {

class controller_dispatcher {
public:
    controller_dispatcher(
      ss::sharded<cluster::controller>& controller,
      ss::smp_service_group ssg,
      ss::scheduling_group sg)
      : _controller(controller)
      , _ssg(ssg)
      , _sg(sg) {}

    template<typename Func>
    ss::futurize_t<std::result_of_t<Func(cluster::controller&)>>
    dispatch_to_controller(Func&& f) {
        return with_scheduling_group(
          _sg, [this, f = std::forward<Func>(f)]() mutable {
              return _controller.invoke_on(
                cluster::controller::shard,
                _ssg,
                [f = std::forward<Func>(f)](cluster::controller& c) mutable {
                    return f(c);
                });
          });
    }

private:
    // controller lives on core 0, do not use it directly
    ss::sharded<cluster::controller>& _controller;
    ss::smp_service_group _ssg;
    ss::scheduling_group _sg;
};

} // namespace kafka

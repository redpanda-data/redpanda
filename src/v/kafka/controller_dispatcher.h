#pragma once
#include "cluster/controller.h"

namespace kafka {

class controller_dispatcher {
public:
    controller_dispatcher(
      cluster::controller& controller,
      smp_service_group ssg,
      scheduling_group sg)
      : _controller(controller)
      , _ssg(ssg)
      , _sg(sg) {}

    template<typename Func>
    futurize_t<std::result_of_t<Func(cluster::controller&)>>
    dispatch_to_controller(Func&& f) {
        return with_scheduling_group(
          _sg, [this, f = std::forward<Func>(f)]() mutable {
              return smp::submit_to(
                cluster::controller::shard,
                _ssg,
                [this, f = std::forward<Func>(f)]() mutable {
                    return f(_controller);
                });
          });
    }

private:
    // controller lives on core 0, do not use it directly
    cluster::controller& _controller;
    smp_service_group _ssg;
    scheduling_group _sg;
};

} // namespace kafka
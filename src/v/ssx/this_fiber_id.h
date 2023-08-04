#include "seastarx.h"
#include "vassert.h"

#include <seastar/core/reactor.hh>
#include <seastar/core/thread.hh>

namespace ssx {

/// Return unique id of the current fiber
///
/// Terminate will be called if the function is called outside of the coroutine.
/// If one coroutine awaits another one they both will have the same fiber id.
inline uint64_t this_fiber_id() {
    auto task = ss::engine().current_task();
    if (task == nullptr) {
        task = seastar::thread_impl::get()->waiting_task();
    }
    vassert(task != nullptr, "this_fiber_id can only be called from coroutine");
    while (true) {
        auto child = task->waiting_task();
        if (child == nullptr) {
            break;
        } else {
            task = child;
        }
    }
    uint64_t task_id = 0;
    std::memcpy(&task_id, &task, sizeof(task_id));
    return task_id;
}

} // namespace ssx

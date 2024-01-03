#include "cluster/tx_helpers.h"

#include "base/vlog.h"
#include "cluster/logger.h"

#include <seastar/core/coroutine.hh>

namespace cluster {

ss::future<bool>
sleep_abortable(std::chrono::milliseconds dur, ss::abort_source& as) {
    try {
        co_await ss::sleep_abortable(dur, as);
        co_return true;
    } catch (const ss::sleep_aborted&) {
        co_return false;
    }
}

} // namespace cluster

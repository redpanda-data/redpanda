#include "cluster/tx_helpers.h"

#include <seastar/core/coroutine.hh>

namespace cluster {

ss::future<bool> sleep_abortable(std::chrono::milliseconds dur) {
    try {
        co_await ss::sleep_abortable(dur);
        co_return true;
    } catch (const ss::sleep_aborted&) {
        co_return false;
    }
}

} // namespace cluster

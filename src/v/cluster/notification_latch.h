#pragma once

#include "cluster/errc.h"
#include "model/fundamental.h"
#include "model/timeout_clock.h"
#include "utils/expiring_promise.h"

#include <absl/container/flat_hash_map.h>

namespace cluster {
/// Cache notifications
class notification_latch {
private:
    using promise_t = expiring_promise<errc, model::timeout_clock>;
    using promise_ptr = std::unique_ptr<promise_t>;
    using underlying_t = absl::flat_hash_map<model::offset, promise_ptr>;

public:
    ss::future<errc> wait_for(model::offset, model::timeout_clock::time_point);
    void notify(model::offset);

private:
    underlying_t _promises;
};
} // namespace cluster
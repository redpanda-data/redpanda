#include "cluster/notification_latch.h"

#include "utils/expiring_promise.h"

#include <seastar/core/future.hh>
#include <seastar/core/shared_ptr.hh>

#include <memory>
#include <stdexcept>
#include <utility>

namespace cluster {

ss::future<errc> notification_latch::wait_for(
  model::offset o, model::timeout_clock::time_point timeout) {
    promise_ptr pr = std::make_unique<promise_t>();
    auto [it, _] = _promises.emplace(o, std::move(pr));

    return it->second
      ->get_future_with_timeout(
        timeout, [] { return errc::notification_wait_timeout; })
      .then([this, o](errc ec) {
          if (auto it = _promises.find(o); it != _promises.end()) {
              _promises.erase(it);
          }
          return ec;
      });
}

void notification_latch::notify(model::offset o) {
    if (auto it = _promises.find(o); it != _promises.end()) {
        it->second->set_value(errc::success);
        _promises.erase(it);
    }
}
} // namespace cluster
#pragma once
#include "raft/types.h"
#include "ssx/future-util.h"
#include "utils/mutex.h"

#include <seastar/core/condition-variable.hh>
#include <seastar/core/do_with.hh>
#include <seastar/core/future.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/loop.hh>
#include <seastar/core/semaphore.hh>
#include <seastar/core/shared_ptr.hh>

namespace raft::details {

template<typename Request, typename Response>
class mutex_buffer {
public:
    explicit mutex_buffer(mutex& m, size_t max_buffered_elements)
      : _mutex(m)
      , _max_buffered(max_buffered_elements) {}

    ss::future<Response> enqueue(Request&& r) {
        if (_gate.is_closed()) {
            return ss::make_exception_future<Response>(
              ss::gate_closed_exception{});
        }
        // we do not use a semaphore as we do not want to wait, waiting may
        // cause requests reordering
        if (_requests.size() >= _max_buffered) {
            return ss::make_exception_future<Response>(
              std::overflow_error("max buffered entries count reached."));
        }
        ss::promise<Response> p;
        auto f = p.get_future();
        _requests.push_back(std::move(r));
        _responsens.push_back(std::move(p));
        _enequeued.signal();
        return f;
    }
    // clang-format off
    template<typename Func>
    CONCEPT(requires requires(Func f, Request req) {
        { f(std::move(req)) } -> ss::future<Response>;
    })
    // clang-format on
    void start(Func&& f);

    ss::future<> stop() {
        _enequeued.broken();
        return _gate.close();
    }

private:
    using request_t = std::vector<Request>;
    using response_t = std::vector<ss::promise<Response>>;
    template<typename Func>
    ss::future<> flush(Func&& f);
    template<typename Func>
    ss::future<> do_flush(request_t, response_t, Func&& f);

    request_t _requests;
    response_t _responsens;
    ss::condition_variable _enequeued;
    ss::gate _gate;
    mutex& _mutex;
    const size_t _max_buffered;
};

template<typename Request, typename Response>
template<typename Func>
void mutex_buffer<Request, Response>::start(Func&& f) {
    (void)ss::with_gate(_gate, [this, f = std::forward<Func>(f)]() mutable {
        return ss::do_until(
          [this] { return _gate.is_closed(); },
          [this, f = std::forward<Func>(f)]() mutable {
              return _enequeued.wait([this] { return !_requests.empty(); })
                .then([this, f = std::forward<Func>(f)]() mutable {
                    return flush(std::forward<Func>(f));
                })
                .handle_exception_type(
                  [](const ss::broken_condition_variable&) {
                      // ignore exception, we are about to stop
                  });
          });
    });
}
template<typename Request, typename Response>
template<typename Func>
ss::future<> mutex_buffer<Request, Response>::flush(Func&& f) {
    auto requests = std::exchange(_requests, {});
    auto response_promises = std::exchange(_responsens, {});

    return _mutex.with(
      [this,
       f = std::forward<Func>(f),
       requests = std::move(requests),
       response_promises = std::move(response_promises)]() mutable {
          return do_flush(
            std::move(requests),
            std::move(response_promises),
            std::forward<Func>(f));
      });
}

template<typename Request, typename Response>
template<typename Func>
ss::future<> mutex_buffer<Request, Response>::do_flush(
  request_t requests, response_t response_promises, Func&& f) {
    auto resp = ss::make_lw_shared(std::move(response_promises));
    return ss::do_with(
             std::move(requests),
             resp->begin(),
             [f = std::forward<Func>(f)](
               request_t& requests, typename response_t::iterator& it) mutable {
                 return ss::do_for_each(
                   requests,
                   [&it, f = std::forward<Func>(f)](Request& req) mutable {
                       return f(std::move(req))
                         .then([&it](Response r) {
                             (it++)->set_value(std::move(r));
                         })
                         .handle_exception([&it](const std::exception_ptr& e) {
                             (it++)->set_exception(e);
                         });
                   });
             })
      .finally([resp] {});
}
} // namespace raft::details

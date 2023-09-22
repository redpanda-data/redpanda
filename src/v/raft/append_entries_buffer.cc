#include "raft/append_entries_buffer.h"

#include "raft/consensus.h"
#include "raft/types.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/do_with.hh>
#include <seastar/core/loop.hh>
#include <seastar/util/later.hh>
#include <seastar/util/variant_utils.hh>

#include <exception>
#include <variant>
#include <vector>
namespace raft {

append_entries_buffer::append_entries_buffer(
  consensus& c, size_t max_buffered_elements)
  : _consensus(c)
  , _max_buffered(max_buffered_elements) {}

ss::future<append_entries_reply>
append_entries_buffer::enqueue(append_entries_request&& r) {
    auto guard = _gate.hold();

    // we normally do not want to wait as it would cause requests
    // reordering. Reordering may only happend if we would wait on condition
    // variable.

    return _flushed.wait([this] { return _requests.size() < _max_buffered; })
      .then([this, r = std::move(r), guard = std::move(guard)]() mutable {
          ss::promise<append_entries_reply> p;
          auto f = p.get_future();
          _requests.push_back(std::move(r));
          _responses.push_back(std::move(p));
          _enqueued.signal();
          // do not wait for the future to finish inside the gate
          return f;
      });
}

ss::future<> append_entries_buffer::stop() {
    auto f = _gate.close();
    // break the condition variables to initiate shutdown process
    _enqueued.broken();
    _flushed.broken();
    // wait for gate to be closed so all the pending requests will finish before
    // we invalidate pending promisses
    co_await std::move(f);
    auto response_promises = std::exchange(_responses, {});
    // set errors
    for (auto& p : response_promises) {
        p.set_exception(ss::gate_closed_exception());
    }
    vassert(
      _responses.empty(),
      "response promises queue should be empty when append entries buffer is "
      "about to stop");
}

void append_entries_buffer::start() {
    ssx::spawn_with_gate(_gate, [this] {
        return ss::do_until(
          [this] { return _gate.is_closed(); },
          [this] {
              return _enqueued.wait([this] { return !_requests.empty(); })
                .then([this] { return flush(); });
          });
    });
}

ss::future<> append_entries_buffer::flush() {
    // empty requests, do nothing
    if (_requests.empty()) {
        return ss::now();
    }
    auto requests = std::exchange(_requests, {});
    auto response_promises = std::exchange(_responses, {});

    return _consensus._op_lock.get_units().then(
      [this,
       requests = std::move(requests),
       response_promises = std::move(response_promises)](
        ssx::semaphore_units u) mutable {
          return do_flush(
            std::move(requests), std::move(response_promises), std::move(u));
      });
}

ss::future<> append_entries_buffer::do_flush(
  request_t requests, response_t response_promises, ssx::semaphore_units u) {
    bool needs_flush = false;
    reply_list_t replies;
    auto f = ss::now();
    {
        ssx::semaphore_units op_lock_units = std::move(u);
        replies.reserve(requests.size());
        for (auto& req : requests) {
            if (req.is_flush_required()) {
                needs_flush = true;
            }
            try {
                // NOTE: do_append_entries do not flush
                auto reply = co_await _consensus.do_append_entries(
                  std::move(req));
                replies.emplace_back(reply);
            } catch (...) {
                replies.emplace_back(std::current_exception());
            }
        }
        if (needs_flush) {
            f = _consensus.flush_log().discard_result();
        }
    }

    // units were released before flushing log
    co_await std::move(f);

    propagate_results(std::move(replies), std::move(response_promises));
    _flushed.broadcast();
    co_return;
}

void append_entries_buffer::propagate_results(
  reply_list_t replies, response_t response_promises) {
    vassert(
      replies.size() == response_promises.size(),
      "Number of requests and response promiseshave to be equal. Have {} "
      "response promises and {} requests",
      response_promises.size(),
      replies.size());
    auto resp_it = response_promises.begin();
    for (auto& reply : replies) {
        ss::visit(
          reply,
          [&resp_it, this](append_entries_reply r) {
              // this is important, we want to update response committed
              // offset here as we flushed after the response structure was
              // created
              r.last_flushed_log_index = _consensus._flushed_offset;
              resp_it->set_value(r);
          },
          [&resp_it](std::exception_ptr& e) {
              resp_it->set_exception(std::move(e));
          });
        resp_it++;
    }
}
} // namespace raft

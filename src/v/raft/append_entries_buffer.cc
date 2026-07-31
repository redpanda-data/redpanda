#include "raft/append_entries_buffer.h"

#include "raft/consensus.h"
#include "raft/types.h"
#include "ssx/future-util.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/loop.hh>
#include <seastar/util/later.hh>
#include <seastar/util/variant_utils.hh>

#include <exception>
namespace raft {

append_entries_buffer::append_entries_buffer(
  consensus& c, size_t max_buffered_elements)
  : _consensus(c)
  , _max_buffered(max_buffered_elements) {}

ss::future<append_entries_reply>
append_entries_buffer::enqueue(append_entries_request r) {
    std::optional<ss::future<append_entries_reply>> response;
    {
        auto guard = _gate.hold();

        // we normally do not want to wait as it would cause requests
        // reordering. Reordering may only happend if we would wait on condition
        // variable.
        co_await ss::coroutine::without_preemption_check(
          _flushed.wait([this] { return _requests.size() < _max_buffered; }));

        ss::promise<append_entries_reply> promise;
        response.emplace(promise.get_future());
        _requests.push_back(std::move(r));
        _responses.push_back(std::move(promise));
        _enqueued.signal();
    }

    // Do not wait for the response while holding the gate.
    co_return co_await ss::coroutine::without_preemption_check(
      std::move(*response));
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
        return ss::with_scheduling_group(
          _consensus._scheduling.recv_sg, [this] { return dispatch_loop(); });
    });
}

ss::future<> append_entries_buffer::dispatch_loop() {
    while (!_gate.is_closed()) {
        co_await ss::coroutine::without_preemption_check(
          _enqueued.wait([this] { return !_requests.empty(); }));
        co_await flush();
    }
}

ss::future<> append_entries_buffer::flush() {
    // empty requests, do nothing
    if (_requests.empty()) {
        co_return;
    }
    auto requests = std::exchange(_requests, {});
    auto response_promises = std::exchange(_responses, {});

    co_await ss::coroutine::without_preemption_check(
      do_flush(std::move(requests), std::move(response_promises)));
}

ss::future<> append_entries_buffer::do_flush(
  request_t requests, response_t response_promises) {
    bool needs_flush = false;
    reply_list_t replies;
    std::optional<ss::future<consensus::flushed>> flush;
    _consensus._probe->append_entries_buffer_flush();
    {
        auto op_lock_units = co_await ss::coroutine::without_preemption_check(
          _consensus._op_lock.get_units());
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
            flush.emplace(_consensus.flush_log());
        }
    }

    // units were released before flushing log
    if (flush) {
        (void)co_await std::move(*flush);
    }

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

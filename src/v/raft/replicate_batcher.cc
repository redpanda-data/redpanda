#include "raft/replicate_batcher.h"

#include "model/fundamental.h"
#include "model/record_batch_reader.h"
#include "raft/consensus_utils.h"
#include "raft/errc.h"
#include "raft/replicate_entries_stm.h"
#include "raft/types.h"

#include <seastar/core/semaphore.hh>
#include <seastar/core/sleep.hh>

#include <chrono>
#include <exception>

namespace raft {
using namespace std::chrono_literals; // NOLINT
replicate_batcher::replicate_batcher(consensus* ptr, size_t cache_size)
  : _ptr(ptr)
  , _max_batch_size(cache_size) {
    _flush_timer.set_callback([this] {
        // background block further caching too
        (void)ss::with_semaphore(_batch_sem, 1, [this] { return flush(); });
    });
}

ss::future<result<replicate_result>>
replicate_batcher::replicate(model::record_batch_reader&& r) {
    return ss::with_semaphore(
             _batch_sem,
             1,
             [this, r = std::move(r)]() mutable {
                 return do_cache(std::move(r));
             })
      .then([this](item_ptr i) {
          if (_pending_bytes < _max_batch_size || !_flush_timer.armed()) {
              _flush_timer.rearm(clock_type::now() + 4ms);
          }
          return i->_promise.get_future();
      });
}

replicate_batcher::~replicate_batcher() noexcept {
    _flush_timer.cancel();
    if (_pending_bytes > 0) {
        _ptr->_ctxlog.info(
          "Setting exceptional futures to {} pending writes",
          _item_cache.size());
        for (auto& i : _item_cache) {
            i->_promise.set_exception(
              std::runtime_error("replicate_batcher destructor called. Cannot "
                                 "finish replicating pending entries"));
        }
        _item_cache.clear();
        _data_cache.clear();
        _pending_bytes = 0;
    }
}
ss::future<replicate_batcher::item_ptr>
replicate_batcher::do_cache(model::record_batch_reader&& r) {
    return model::consume_reader_to_memory(std::move(r), model::no_timeout)
      .then([this](ss::circular_buffer<model::record_batch> batches) {
          return ss::with_semaphore(
            _ptr->_op_sem, 1, [this, batches = std::move(batches)]() mutable {
                auto i = ss::make_lw_shared<item>();
                size_t record_count = 0;
                for (auto& b : batches) {
                    record_count += b.record_count();
                    _pending_bytes += b.size_bytes();
                    _data_cache.emplace_back(std::move(b));
                }
                i->record_count = record_count;
                _item_cache.emplace_back(i);
                return i;
            });
      });
}

ss::future<> replicate_batcher::flush() {
    if (_pending_bytes == 0) {
        return ss::make_ready_future<>();
    }
    auto notifications = std::exchange(_item_cache, {});
    auto data = std::exchange(_data_cache, {});
    _pending_bytes = 0;
    return ss::with_gate(
      _ptr->_bg,
      [this,
       data = std::move(data),
       notifications = std::move(notifications)]() mutable {
          return ss::with_semaphore(
            _ptr->_op_sem,
            1,
            [this,
             data = std::move(data),
             notifications = std::move(notifications)]() mutable {
                // we have to check if we are the leader
                // it is critical as term could have been updated already by
                // vote request and entries from current node could be accepted
                // by the followers while it is no longer a leader
                // this problem caused truncation failure.

                if (!_ptr->is_leader()) {
                    for (auto& n : notifications) {
                        n->_promise.set_value(errc::not_leader);
                    }
                    return ss::make_ready_future<>();
                }
                const auto& meta = _ptr->_meta;
                auto const term = model::term_id(meta.term);
                for (auto& b : data) {
                    b.set_term(term);
                }
                append_entries_request req{
                  .node_id = _ptr->_self,
                  .meta = meta,
                  .batches = model::make_memory_record_batch_reader(
                    std::move(data))};
                return do_flush(std::move(notifications), std::move(req));
            });
      });
}
static void propagate_result(
  result<replicate_result> r,
  std::vector<replicate_batcher::item_ptr>& notifications) {
    if (r.has_value()) {
        // iterate backward to calculate last offsets
        auto last_offset = r.value().last_offset;
        for (auto it = notifications.rbegin(); it != notifications.rend();
             ++it) {
            (*it)->_promise.set_value(replicate_result{last_offset});
            last_offset = last_offset - model::offset((*it)->record_count);
        }
        return;
    }
    // propagate an error
    for (auto& n : notifications) {
        n->_promise.set_value(r.error());
    }
}

static void propagate_current_exception(
  std::vector<replicate_batcher::item_ptr>& notifications) {
    // iterate backward to calculate last offsets
    auto e = std::current_exception();
    for (auto& n : notifications) {
        n->_promise.set_exception(e);
    }
}

ss::future<> replicate_batcher::do_flush(
  std::vector<replicate_batcher::item_ptr>&& notifications,
  append_entries_request&& req) {
    auto stm = ss::make_lw_shared<replicate_entries_stm>(
      _ptr, 3, std::move(req));
    return stm->apply().then_wrapped(
      [this, stm, notifications = std::move(notifications)](
        ss::future<result<replicate_result>> fut) mutable {
          try {
              auto ret = fut.get0();
              propagate_result(ret, notifications);
          } catch (...) {
              propagate_current_exception(notifications);
          }
          auto f = stm->wait().finally([stm] {});
          // if gate is closed wait for all futures
          if (_ptr->_bg.is_closed()) {
              _ptr->_ctxlog.info(
                "gate-closed, waiting to finish background requests");
              return std::move(f);
          }
          // background
          (void)with_gate(_ptr->_bg, [this, stm, f = std::move(f)]() mutable {
              return std::move(f).handle_exception(
                [this](const std::exception_ptr& e) {
                    _ptr->_ctxlog.error(
                      "Error waiting for background acks to finish - {}", e);
                });
          });
          return ss::make_ready_future<>();
      });
}
} // namespace raft

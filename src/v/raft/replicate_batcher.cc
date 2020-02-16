#include "raft/replicate_batcher.h"

#include "model/record_batch_reader.h"
#include "raft/consensus_utils.h"
#include "raft/replicate_entries_stm.h"

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
          if (_pending_bytes < _max_batch_size) {
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
          auto i = ss::make_lw_shared<item>();
          if (_data_cache.empty()) {
              i->ret.last_offset = model::offset(_ptr->_meta.prev_log_index);
          } else {
              i->ret.last_offset = _data_cache.back().last_offset();
          }
          size_t record_count = 0;
          for (auto& b : batches) {
              record_count += b.record_count();
              _pending_bytes += b.size_bytes();
              _data_cache.emplace_back(std::move(b));
          }
          i->ret.last_offset = details::next_offset(i->ret.last_offset)
                               + model::offset(record_count - 1);
          _item_cache.emplace_back(i);
          return i;
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
              for (auto& n : notifications) {
                  n->_promise.set_value(n->ret);
              }
          } catch (...) {
              auto err = std::current_exception();
              for (auto& n : notifications) {
                  n->_promise.set_exception(err);
              }
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

// Copyright 2020 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "raft/replicate_batcher.h"

#include "model/fundamental.h"
#include "model/record_batch_reader.h"
#include "raft/consensus_utils.h"
#include "raft/errc.h"
#include "raft/replicate_entries_stm.h"
#include "raft/types.h"

#include <seastar/core/semaphore.hh>
#include <seastar/core/sleep.hh>
#include <seastar/core/smp.hh>

#include <chrono>
#include <exception>

namespace raft {
using namespace std::chrono_literals; // NOLINT
replicate_batcher::replicate_batcher(
  consensus* ptr,
  std::chrono::milliseconds debounce_duration,
  size_t cache_size)
  : _ptr(ptr)
  , _debounce_duration(debounce_duration)
  , _max_batch_size(cache_size) {
    _flush_timer.set_callback([this] { dispatch_background_flush(); });
}

void replicate_batcher::dispatch_background_flush() {
    (void)ss::with_gate(_ptr->_bg, [this] {
        // background block further caching too
        return _lock.with([this] { return flush(); });
    }).handle_exception_type([this](const ss::gate_closed_exception&) {
        vlog(
          _ptr->_ctxlog.debug, "Gate closed while flushing replicate requests");
    });
}

ss::future<result<replicate_result>>
replicate_batcher::replicate(model::record_batch_reader&& r) {
    return _lock
      .with(
        [this, r = std::move(r)]() mutable { return do_cache(std::move(r)); })
      .then([this](item_ptr i) {
          if (_pending_bytes >= _max_batch_size) {
              _flush_timer.cancel();
              dispatch_background_flush();
              return i->_promise.get_future();
          }

          if (!_flush_timer.armed()) {
              _flush_timer.rearm(clock_type::now() + _debounce_duration);
          }
          return i->_promise.get_future();
      });
}

ss::future<> replicate_batcher::stop() {
    _flush_timer.cancel();
    // we keep a lock here to make sure that all inflight requests have finished
    // already
    return _lock.with([this]() {
        if (_pending_bytes > 0) {
            _ptr->_ctxlog.info(
              "Setting exceptional futures to {} pending writes",
              _item_cache.size());
            for (auto& i : _item_cache) {
                i->_promise.set_exception(std::runtime_error(
                  "replicate_batcher destructor called. Cannot "
                  "finish replicating pending entries"));
            }
            _item_cache.clear();
            _data_cache.clear();
            _pending_bytes = 0;
        }
    });
}
ss::future<replicate_batcher::item_ptr>
replicate_batcher::do_cache(model::record_batch_reader&& r) {
    return model::consume_reader_to_memory(std::move(r), model::no_timeout)
      .then([this](ss::circular_buffer<model::record_batch> batches) {
          auto i = ss::make_lw_shared<item>();
          size_t record_count = 0;
          for (auto& b : batches) {
              record_count += b.record_count();
              _pending_bytes += b.size_bytes();
              if (b.header().ctx.owner_shard == ss::this_shard_id()) {
                  _data_cache.emplace_back(std::move(b));
              } else {
                  _data_cache.emplace_back(b.copy());
              }
          }
          i->record_count = record_count;
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
          return _ptr->_op_lock.get_units().then(
            [this,
             data = std::move(data),
             notifications = std::move(notifications)](
              ss::semaphore_units<> u) mutable {
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

                auto meta = _ptr->meta();
                auto const term = model::term_id(meta.term);
                for (auto& b : data) {
                    b.set_term(term);
                }
                auto seqs = _ptr->next_followers_request_seq();
                append_entries_request req(
                  _ptr->_self,
                  std::move(meta),
                  model::make_memory_record_batch_reader(std::move(data)));
                return do_flush(
                  std::move(notifications),
                  std::move(req),
                  std::move(u),
                  std::move(seqs));
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
  append_entries_request&& req,
  ss::semaphore_units<> u,
  absl::flat_hash_map<model::node_id, follower_req_seq> seqs) {
    _ptr->_probe.replicate_batch_flushed();
    auto stm = ss::make_lw_shared<replicate_entries_stm>(
      _ptr, std::move(req), std::move(seqs));
    return stm->apply(std::move(u))
      .then_wrapped([this, stm, notifications = std::move(notifications)](
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
              return f;
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

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
#include "model/record.h"
#include "model/record_batch_reader.h"
#include "raft/consensus_utils.h"
#include "raft/errc.h"
#include "raft/replicate_entries_stm.h"
#include "raft/types.h"

#include <seastar/core/circular_buffer.hh>
#include <seastar/core/semaphore.hh>
#include <seastar/core/sleep.hh>
#include <seastar/core/smp.hh>

#include <chrono>
#include <cstddef>
#include <exception>

namespace raft {
using namespace std::chrono_literals; // NOLINT
replicate_batcher::replicate_batcher(consensus* ptr, size_t cache_size)
  : _ptr(ptr)
  , _max_batch_size_sem(cache_size) {}

ss::future<result<replicate_result>> replicate_batcher::replicate(
  std::optional<model::term_id> expected_term, model::record_batch_reader&& r) {
    return do_cache(expected_term, std::move(r)).then([this](item_ptr i) {
        return _lock.with([this] { return flush(); }).then([i] {
            return i->_promise.get_future();
        });
    });
}

ss::future<> replicate_batcher::stop() {
    // we keep a lock here to make sure that all inflight requests have finished
    // already
    return _lock.with([this]() {
        for (auto& i : _item_cache) {
            i->_promise.set_exception(
              std::runtime_error("replicate_batcher destructor called. Cannot "
                                 "finish replicating pending entries"));
        }
        _item_cache.clear();
    });
}

ss::future<replicate_batcher::item_ptr> replicate_batcher::do_cache(
  std::optional<model::term_id> expected_term, model::record_batch_reader&& r) {
    return model::consume_reader_to_memory(std::move(r), model::no_timeout)
      .then([this,
             expected_term](ss::circular_buffer<model::record_batch> batches) {
          ss::circular_buffer<model::record_batch> data;
          size_t bytes = std::accumulate(
            batches.cbegin(),
            batches.cend(),
            size_t{0},
            [](size_t sum, const model::record_batch& b) {
                return sum + b.size_bytes();
            });
          return do_cache_with_backpressure(
            expected_term, std::move(batches), bytes);
      });
}

ss::future<replicate_batcher::item_ptr>
replicate_batcher::do_cache_with_backpressure(
  std::optional<model::term_id> expected_term,
  ss::circular_buffer<model::record_batch> batches,
  size_t bytes) {
    return ss::get_units(_max_batch_size_sem, bytes)
      .then([this, expected_term, batches = std::move(batches)](

              ss::semaphore_units<> u) mutable {
          size_t record_count = 0;
          auto i = ss::make_lw_shared<item>();
          for (auto& b : batches) {
              record_count += b.record_count();
              if (b.header().ctx.owner_shard == ss::this_shard_id()) {
                  i->data.push_back(std::move(b));
              } else {
                  i->data.push_back(b.copy());
              }
          }
          i->expected_term = expected_term;
          i->record_count = record_count;
          i->units = std::move(u);
          _item_cache.emplace_back(i);
          return i;
      });
}

ss::future<> replicate_batcher::flush() {
    auto item_cache = std::exchange(_item_cache, {});
    if (item_cache.empty()) {
        return ss::now();
    }
    return ss::with_gate(
      _ptr->_bg, [this, item_cache = std::move(item_cache)]() mutable {
          return _ptr->_op_lock.get_units().then(
            [this, item_cache = std::move(item_cache)](
              ss::semaphore_units<> u) mutable {
                // we have to check if we are the leader
                // it is critical as term could have been updated already by
                // vote request and entries from current node could be accepted
                // by the followers while it is no longer a leader
                // this problem caused truncation failure.

                if (!_ptr->is_leader()) {
                    for (auto& n : item_cache) {
                        n->_promise.set_value(errc::not_leader);
                    }
                    return ss::make_ready_future<>();
                }

                auto meta = _ptr->meta();
                auto const term = model::term_id(meta.term);
                ss::circular_buffer<model::record_batch> data;
                std::vector<item_ptr> notifications;

                for (auto& n : item_cache) {
                    if (
                      !n->expected_term.has_value()
                      || n->expected_term.value() == term) {
                        for (auto& b : n->data) {
                            b.set_term(term);
                            data.push_back(std::move(b));
                        }
                        notifications.push_back(std::move(n));
                    } else {
                        n->_promise.set_value(errc::not_leader);
                    }
                }

                if (notifications.empty()) {
                    return ss::now();
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
  absl::flat_hash_map<vnode, follower_req_seq> seqs) {
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

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
#include "raft/consensus.h"
#include "raft/consensus_utils.h"
#include "raft/errc.h"
#include "raft/types.h"
#include "utils/gate_guard.h"

#include <seastar/core/circular_buffer.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/core/do_with.hh>
#include <seastar/core/gate.hh>
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
  , _max_batch_size_sem(cache_size)
  , _max_batch_size(cache_size) {}

replicate_stages replicate_batcher::replicate(
  std::optional<model::term_id> expected_term,
  model::record_batch_reader&& r,
  ss::lw_shared_ptr<leader_ack_result> leader_outcome) {
    ss::promise<> enqueued;
    auto enqueued_f = enqueued.get_future();
    try {
        gate_guard guard(_bg);
        auto f
          = do_cache(expected_term, std::move(r), leader_outcome)
              .then_wrapped(
                [this,
                 leader_outcome,
                 enqueued = std::move(enqueued),
                 guard = std::move(guard)](ss::future<item_ptr> f) mutable {
                    if (f.failed()) {
                        enqueued.set_exception(f.get_exception());
                        if (leader_outcome) {
                            leader_outcome->offset.set_value(
                              errc::replicate_batcher_cache_error);
                        }
                        return ss::make_ready_future<result<replicate_result>>(
                          errc::replicate_batcher_cache_error);
                    }

                    enqueued.set_value();
                    auto item = f.get();
                    return _lock.get_units()
                      .then([this](ss::semaphore_units<> u) {
                          return flush(std::move(u), false);
                      })
                      .then_wrapped(
                        [this, item, guard = std::move(guard)](ss::future<> f) {
                            if (f.failed()) {
                                vlog(
                                  _ptr->_ctxlog.warn,
                                  "replicate flush failed - {}",
                                  f.get_exception());
                            } else {
                                f.ignore_ready_future();
                            }
                            _ptr->_probe.replicate_done();
                            // we wait for the future outside of the batcher
                            // gate since we do not access any resources
                            return item->_promise.get_future();
                        });
                });
        return replicate_stages(std::move(enqueued_f), std::move(f));
    } catch (...) {
        if (leader_outcome) {
            leader_outcome->offset.set_value(errc::shutting_down);
        }
        return replicate_stages(
          ss::current_exception_as_future<>(),
          ss::make_ready_future<result<replicate_result>>(errc::shutting_down));
    }
}

ss::future<> replicate_batcher::stop() {
    return _bg.close().then([this] {
        // we keep a lock here to make sure that all inflight requests have
        // finished already
        return _lock.with([this] {
            for (auto& i : _item_cache) {
                i->_promise.set_exception(ss::gate_closed_exception());
                if (i->leader_outcome) {
                    i->leader_outcome->offset.set_value(errc::shutting_down);
                }
            }
            _item_cache.clear();
        });
    });
}

ss::future<replicate_batcher::item_ptr> replicate_batcher::do_cache(
  std::optional<model::term_id> expected_term,
  model::record_batch_reader&& r,
  ss::lw_shared_ptr<leader_ack_result> leader_outcome) {
    return model::consume_reader_to_memory(std::move(r), model::no_timeout)
      .then([this, leader_outcome, expected_term](
              ss::circular_buffer<model::record_batch> batches) {
          ss::circular_buffer<model::record_batch> data;
          size_t bytes = std::accumulate(
            batches.cbegin(),
            batches.cend(),
            size_t{0},
            [](size_t sum, const model::record_batch& b) {
                return sum + b.size_bytes();
            });
          return do_cache_with_backpressure(
            expected_term, std::move(batches), bytes, leader_outcome);
      });
}

ss::future<replicate_batcher::item_ptr>
replicate_batcher::do_cache_with_backpressure(
  std::optional<model::term_id> expected_term,
  ss::circular_buffer<model::record_batch> batches,
  size_t bytes,
  ss::lw_shared_ptr<leader_ack_result> leader_outcome) {
    /**
     * Produce a message larger than the internal raft batch accumulator
     * (default 1Mb) the semaphore can't be acquired. Closing
     * the connection doesn't propagate this error and allow those units to be
     * returned, so the entire partition is no longer writable.
     * see:
     *
     * https://github.com/vectorizedio/redpanda/issues/1503.
     *
     * When batch size exceed available semaphore units we just acquire all of
     * them to be able to continue.
     */

    return ss::get_units(_max_batch_size_sem, std::min(bytes, _max_batch_size))
      .then([this, expected_term, leader_outcome, batches = std::move(batches)](
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
          i->leader_outcome = leader_outcome;
          _item_cache.emplace_back(i);
          return i;
      });
}

ss::future<>
replicate_batcher::flush(ss::semaphore_units<> u, bool const transfer_flush) {
    return ss::try_with_gate(
      _bg, [this, batcher_units = std::move(u), transfer_flush]() mutable {
          auto item_cache = std::exchange(_item_cache, {});
          if (item_cache.empty()) {
              return ss::now();
          }

          return _ptr->_op_lock.get_units().then_wrapped(
            [this,
             item_cache = std::move(item_cache),
             batcher_units = std::move(batcher_units),
             transfer_flush](ss::future<ss::semaphore_units<>> f) mutable {
                // if we failed to acquire units, propagate exception to cached
                // promises
                if (f.failed()) {
                    for (auto& i : item_cache) {
                        i->_promise.set_exception(f.get_exception());
                        if (i->leader_outcome) {
                            i->leader_outcome->offset.set_value(
                              errc::oplock_exception);
                        }
                    }
                    return ss::now();
                }
                auto u = f.get();

                if (!transfer_flush && _ptr->_transferring_leadership) {
                    vlog(
                      _ptr->_ctxlog.warn,
                      "Dropping flush, leadership transferring");
                    for (auto& n : item_cache) {
                        n->_promise.set_value(errc::not_leader);
                        if (n->leader_outcome) {
                            n->leader_outcome->offset.set_value(
                              errc::not_leader);
                        }
                    }
                    return ss::now();
                }

                // release batcher replicate batcher lock
                batcher_units.return_all();
                // we have to check if we are the leader
                // it is critical as term could have been updated already by
                // vote request and entries from current node could be accepted
                // by the followers while it is no longer a leader
                // this problem caused truncation failure.

                if (!_ptr->is_leader()) {
                    for (auto& n : item_cache) {
                        n->_promise.set_value(errc::not_leader);
                        if (n->leader_outcome) {
                            n->leader_outcome->offset.set_value(
                              errc::not_leader);
                        }
                    }
                    return ss::make_ready_future<>();
                }

                auto meta = _ptr->meta();
                auto const term = model::term_id(meta.term);
                ss::circular_buffer<model::record_batch> data;
                std::vector<item_ptr> notifications;
                ss::semaphore_units<> item_memory_units(_max_batch_size_sem, 0);
                for (auto& n : item_cache) {
                    item_memory_units.adopt(std::move(n->units));
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
                        if (n->leader_outcome) {
                            n->leader_outcome->offset.set_value(
                              errc::not_leader);
                        }
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
                std::vector<ss::semaphore_units<>> units;
                units.reserve(2);
                units.push_back(std::move(u));
                // we will release memory semaphore as soon as append entry
                // requests will be dispatched
                units.push_back(std::move(item_memory_units));
                return do_flush(
                  std::move(notifications),
                  std::move(req),
                  std::move(units),
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
    auto e = std::current_exception();
    for (auto& n : notifications) {
        n->_promise.set_exception(e);
    }
}

static void propagate_leader_result(
  result<replicate_result> r,
  std::vector<replicate_batcher::item_ptr>& notifications) {
    if (r.has_value()) {
        // iterate backward to calculate last offsets
        auto last_offset = r.value().last_offset;
        for (auto it = notifications.rbegin(); it != notifications.rend();
             ++it) {
            if ((*it)->leader_outcome) {
                (*it)->leader_outcome->offset.set_value(
                  replicate_result{last_offset});
            }
            last_offset = last_offset - model::offset((*it)->record_count);
        }
        return;
    }
    // propagate an error
    for (auto& n : notifications) {
        if (n->leader_outcome) {
            n->leader_outcome->offset.set_value(r.error());
        }
    }
}

ss::future<> replicate_batcher::do_flush(
  std::vector<replicate_batcher::item_ptr> notifications,
  append_entries_request req,
  std::vector<ss::semaphore_units<>> u,
  absl::flat_hash_map<vnode, follower_req_seq> seqs) {
    _ptr->_probe.replicate_batch_flushed();

    auto leader_outcome = ss::make_lw_shared<leader_ack_result>();
    auto lo_f = leader_outcome->offset.get_future();
    auto f = _ptr->dispatch_replicate(
      std::move(req), std::move(u), std::move(seqs), leader_outcome);

    return lo_f.then([notifications = std::move(notifications),
                      f = std::move(f)](result<replicate_result> er) mutable {
        propagate_leader_result(er, notifications);

        return f.then_wrapped(
          [notifications = std::move(notifications)](
            ss::future<result<replicate_result>> result_f) mutable {
              try {
                  auto result = result_f.get();
                  propagate_result(result, notifications);
              } catch (...) {
                  propagate_current_exception(notifications);
              }
          });
    });
}
} // namespace raft

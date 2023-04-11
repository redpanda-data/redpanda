// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "raft/replicate_batcher.h"

#include "raft/consensus.h"
#include "raft/replicate_entries_stm.h"
#include "raft/types.h"
#include "ssx/future-util.h"
#include "utils/gate_guard.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/future.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/shared_ptr.hh>

#include <optional>

namespace raft {
using namespace std::chrono_literals; // NOLINT
replicate_batcher::replicate_batcher(consensus* ptr, size_t cache_size)
  : _ptr(ptr)
  , _max_batch_size_sem(cache_size, "raft/repl-batch")
  , _max_batch_size(cache_size) {}

replicate_stages replicate_batcher::replicate(
  std::optional<model::term_id> expected_term,
  model::record_batch_reader r,
  consistency_level consistency_lvl,
  std::optional<std::chrono::milliseconds> timeout) {
    ss::promise<> enqueued;
    auto enqueued_f = enqueued.get_future();

    auto f = cache_and_wait_for_result(
      std::move(enqueued),
      expected_term,
      std::move(r),
      consistency_lvl,
      timeout);
    return {std::move(enqueued_f), std::move(f)};
}

ss::future<result<replicate_result>>
replicate_batcher::cache_and_wait_for_result(
  ss::promise<> enqueued,
  std::optional<model::term_id> expected_term,
  model::record_batch_reader r,
  consistency_level consistency_lvl,
  std::optional<std::chrono::milliseconds> timeout) {
    item_ptr item;
    try {
        auto holder = _bg.hold();
        item = co_await do_cache(
          expected_term, std::move(r), consistency_lvl, timeout);

        // now request is already enqueued, we can release first
        // stage future
        enqueued.set_value();

        /**
         * Dispatching flush in a background.
         *
         * The batcher mutex may be grabbed without the timeout as each item
         * has its own timeout timer. The shutdown related exceptions may be
         * ignored here as all pending item promises will be completed in
         * replicate batcher stop method
         *
         */
        if (!_flush_pending) {
            _flush_pending = true;
            ssx::background = ssx::spawn_with_gate_then(_bg, [this]() {
                return _lock.get_units()
                  .then([this](auto units) {
                      return flush(std::move(units), false);
                  })
                  .handle_exception([this](const std::exception_ptr& e) {
                      // an exception here is quite unlikely, since the flush()
                      // method generally catches all its exceptions and
                      // propagates them to the promises associated with the
                      // items being flushed
                      vlog(
                        _ptr->_ctxlog.error,
                        "Error in background flush: {}",
                        e);
                  });
            });
        }
    } catch (...) {
        // exception in caching phase
        enqueued.set_to_current_exception();
        co_return errc::replicate_batcher_cache_error;
    }

    co_return co_await item->get_future();
}

ss::future<> replicate_batcher::stop() {
    return _bg.close().then([this] {
        // we keep a lock here to make sure that all inflight requests have
        // finished already
        return _lock.with([this] {
            for (auto& i : _item_cache) {
                i->set_exception(
                  std::make_exception_ptr(ss::gate_closed_exception()));
            }
            _item_cache.clear();
        });
    });
}

ss::future<replicate_batcher::item_ptr> replicate_batcher::do_cache(
  std::optional<model::term_id> expected_term,
  model::record_batch_reader r,
  consistency_level consistency_lvl,
  std::optional<std::chrono::milliseconds> timeout) {
    auto batches = co_await model::consume_reader_to_memory(
      std::move(r),
      timeout ? model::timeout_clock::now() + *timeout : model::no_timeout);

    size_t bytes = std::accumulate(
      batches.cbegin(),
      batches.cend(),
      size_t{0},
      [](size_t sum, const model::record_batch& b) {
          return sum + b.size_bytes();
      });
    co_return co_await do_cache_with_backpressure(
      expected_term, std::move(batches), bytes, consistency_lvl, timeout);
}

ss::future<replicate_batcher::item_ptr>
replicate_batcher::do_cache_with_backpressure(
  std::optional<model::term_id> expected_term,
  ss::circular_buffer<model::record_batch> batches,
  size_t bytes,
  consistency_level consistency_lvl,
  std::optional<std::chrono::milliseconds> timeout) {
    /**
     * Produce a message larger than the internal raft batch accumulator
     * (default 1Mb) the semaphore can't be acquired. Closing
     * the connection doesn't propagate this error and allow those units to be
     * returned, so the entire partition is no longer writable.
     * see:
     *
     * https://github.com/redpanda-data/redpanda/issues/1503.
     *
     * When batch size exceed available semaphore units we just acquire all of
     * them to be able to continue.
     */
    ssx::semaphore_units u;
    if (timeout) {
        u = co_await ss::get_units(
          _max_batch_size_sem,
          std::min(bytes, _max_batch_size),
          ssx::semaphore::clock::now() + *timeout);
    } else {
        u = co_await ss::get_units(
          _max_batch_size_sem, std::min(bytes, _max_batch_size));
    }

    size_t record_count = 0;
    std::vector<model::record_batch> data;
    data.reserve(batches.size());
    for (auto& b : batches) {
        record_count += b.record_count();
        if (b.header().ctx.owner_shard == ss::this_shard_id()) {
            data.push_back(std::move(b));
        } else {
            data.push_back(b.copy());
        }
    }
    auto i = ss::make_lw_shared<item>(
      record_count,
      std::move(data),
      std::move(u),
      expected_term,
      consistency_lvl,
      timeout);

    _item_cache.emplace_back(i);
    co_return i;
}

ss::future<> replicate_batcher::flush(
  ssx::semaphore_units batcher_units, bool const transfer_flush) {
    auto item_cache = std::exchange(_item_cache, {});
    // this function should not throw, nor return exceptional futures,
    // since it is usually invoked in the background and there is
    // nowhere suitable to
    try {
        _flush_pending = false;
        if (item_cache.empty()) {
            co_return;
        }
        auto u = co_await _ptr->_op_lock.get_units();

        if (!transfer_flush && _ptr->_transferring_leadership) {
            vlog(_ptr->_ctxlog.warn, "Dropping flush, leadership transferring");
            for (auto& n : item_cache) {
                n->set_value(errc::not_leader);
            }
            co_return;
        }

        // release batcher replicate batcher lock
        batcher_units.return_all();
        // we have to check if we are the leader
        // it is critical as term could have been updated already by
        // vote request and entries from current node could be accepted
        // by the followers while it is no longer a leader
        // this problem caused truncation failure.

        if (!_ptr->is_elected_leader()) {
            for (auto& n : item_cache) {
                n->set_value(errc::not_leader);
            }
            co_return;
        }

        auto meta = _ptr->meta();
        auto const term = model::term_id(meta.term);
        ss::circular_buffer<model::record_batch> data;
        std::vector<item_ptr> notifications;
        ssx::semaphore_units item_memory_units(_max_batch_size_sem, 0);
        auto needs_flush = append_entries_request::flush_after_append::no;

        for (auto& n : item_cache) {
            if (
              !n->get_expected_term().has_value()
              || n->get_expected_term().value() == term) {
                auto [batches, units] = n->release_data();
                item_memory_units.adopt(std::move(units));
                if (
                  n->get_consistency_level() == consistency_level::quorum_ack) {
                    needs_flush
                      = append_entries_request::flush_after_append::yes;
                }
                for (auto& b : batches) {
                    b.set_term(term);
                    data.push_back(std::move(b));
                }
                notifications.push_back(std::move(n));
            } else {
                n->set_value(errc::not_leader);
            }
        }

        if (notifications.empty()) {
            co_return;
        }

        auto seqs = _ptr->next_followers_request_seq();
        append_entries_request req(
          _ptr->_self,
          meta,
          model::make_memory_record_batch_reader(std::move(data)),
          needs_flush);

        std::vector<ssx::semaphore_units> units;
        units.reserve(2);
        units.push_back(std::move(u));
        // we will release memory semaphore as soon as append entry
        // requests will be dispatched
        units.push_back(std::move(item_memory_units));
        co_await do_flush(
          std::move(notifications),
          std::move(req),
          std::move(units),
          std::move(seqs));
    } catch (...) {
        for (auto& i : item_cache) {
            i->set_exception(std::current_exception());
        }
        co_return;
    }
}

template<typename Predicate>
static void propagate_result(
  result<replicate_result> r,
  std::vector<replicate_batcher::item_ptr>& notifications,
  const Predicate& pred) {
    if (r.has_error()) {
        // propagate an error
        for (auto& n : notifications) {
            n->set_value(r.error());
        }
        return;
    }

    // iterate backward to calculate last offsets
    auto last_offset = r.value().last_offset;
    for (auto it = notifications.rbegin(); it != notifications.rend(); ++it) {
        if (pred(*it)) {
            (*it)->set_value(replicate_result{last_offset});
        }
        last_offset = last_offset - model::offset((*it)->get_record_count());
    }
}

static void propagate_current_exception(
  std::vector<replicate_batcher::item_ptr>& notifications) {
    // iterate backward to calculate last offsets
    auto e = std::current_exception();
    for (auto& n : notifications) {
        n->set_exception(e);
    }
}

ss::future<> replicate_batcher::do_flush(
  std::vector<replicate_batcher::item_ptr> notifications,
  append_entries_request req,
  std::vector<ssx::semaphore_units> u,
  absl::flat_hash_map<vnode, follower_req_seq> seqs) {
    auto needs_flush = req.flush;
    _ptr->_probe.replicate_batch_flushed();
    auto stm = ss::make_lw_shared<replicate_entries_stm>(
      _ptr, std::move(req), std::move(seqs));
    try {
        auto holder = _bg.hold();
        auto leader_result = co_await stm->apply(std::move(u));

        /**
         * First phase, if leader result has error just propagate error
         * otherwise propagate result to relaxed consistency requests
         */
        propagate_result(
          leader_result, notifications, [](const item_ptr& item) {
              return item->get_consistency_level()
                       == consistency_level::leader_ack
                     || item->get_consistency_level()
                          == consistency_level::no_ack;
          });
        /**
         * Second phase, wait for majority to replicate if leader result has
         * error just propagate error otherwise propagate results to relaxed
         * consistency requests.
         *
         * NOTE: this happens in background since we do not want to block
         * replicate batcher
         */
        if (leader_result && needs_flush) {
            (void)stm->wait_for_majority()
              .then([holder = std::move(holder),
                     notifications = std::move(notifications)](
                      result<replicate_result> quorum_result) mutable {
                  propagate_result(
                    quorum_result, notifications, [](const item_ptr& item) {
                        return item->get_consistency_level()
                               == consistency_level::quorum_ack;
                    });
              })
              .finally([stm] {});
        }
    } catch (...) {
        propagate_current_exception(notifications);
    }

    auto f = stm->wait_for_shutdown().finally([stm] {});
    if (_bg.is_closed()) {
        _ptr->_ctxlog.info(
          "gate-closed, waiting to finish background requests");
        co_return co_await std::move(f);
    }
    ssx::spawn_with_gate(_bg, [this, stm, f = std::move(f)]() mutable {
        return std::move(f).handle_exception(
          [this](const std::exception_ptr& e) {
              _ptr->_ctxlog.debug(
                "Error waiting for background acks to finish - {}", e);
          });
    });
}

} // namespace raft

/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */
#include "io/scheduler.h"

#include "io/persistence.h"
#include "logger.h"

#include <seastar/core/coroutine.hh>

namespace experimental::io {

scheduler::scheduler(size_t num_files) noexcept
  : open_file_limit_(num_files, "io::open_file_limit") {}

void scheduler::add_queue(queue* queue) noexcept {
    queues_.push_back(*queue);
    queue->io_queue_.start();
    queue->monitor_ = monitor(queue);
}

seastar::future<> scheduler::remove_queue(queue* queue) noexcept {
    queue->sched_hook_.unlink();
    queue->stop_ = true;
    queue->monitor_work_.signal();
    auto monitor = std::exchange(
      queue->monitor_, seastar::make_ready_future<>());
    co_await std::move(monitor);
    co_await queue->io_queue_.stop();
}

void scheduler::submit_read(queue* queue, page* page) noexcept {
    queue->io_queue_.submit_read(*page);
    if (!queue->io_queue_.opened()) {
        queue->monitor_work_.signal();
    } else if (queue->lru_hook_.is_linked()) {
        queue->lru_hook_.unlink();
        lru_.push_back(*queue);
    }
}

void scheduler::submit_write(queue* queue, page* page) noexcept {
    queue->io_queue_.submit_write(*page);
    if (!queue->io_queue_.opened()) {
        queue->monitor_work_.signal();
    } else if (queue->lru_hook_.is_linked()) {
        queue->lru_hook_.unlink();
        lru_.push_back(*queue);
    }
}

scheduler::queue::queue(
  persistence* storage,
  std::filesystem::path path,
  io_queue::completion_callback_type complete)
  : io_queue_(storage, std::move(path), std::move(complete)) {}

const std::filesystem::path& scheduler::queue::path() const noexcept {
    return io_queue_.path();
}

// NOLINTNEXTLINE(readability-function-cognitive-complexity)
seastar::future<> scheduler::monitor(queue* queue) noexcept {
    while (true) {
        /*
         * cooperative scheduling for cache eviction. consider the case when
         * nofiles_ semaphore is initialized with 0 units (weird, but ok, maybe
         * we want to be able to pause all I/O). two queues block on nofiles_
         * semaphore. later a unit is deposited into the semaphore and the first
         * wakes up and opens its underlying io_queue. at this point the running
         * queue monitor could be suspended, but we'd like to be able for all
         * queues to make progress. to handle this the active queue monitor will
         * evict one or more other queues before suspending itself. however,
         * because an io queue needs to wake up to be closed there is a delay
         * before the eviction is visible. this delay is why we track waiters_
         * separately rather than using the nofiles_ semaphore state.
         *
         * another way to look at this is that waiters_ is used to handle the
         * case that we adjust the open_file_limit at runtime. currently it is a
         * static value but will later be adjustable.
         */
        while (waiters_ > 0 && !lru_.empty()) {
            waiters_--;
            auto& queue = lru_.front();
            lru_.pop_front();
            queue.monitor_work_.signal();
        }

        /*
         * when stopping the queue clean it up fully including removing it from
         * the cache and closing the underlying io-queue.
         */
        if (queue->stop_) {
            if (queue->io_queue_.opened()) {
                if (queue->lru_hook_.is_linked()) {
                    queue->lru_hook_.unlink();
                }
                co_await queue->io_queue_.close();
                queue->open_file_limit_units_.return_all();
            }
            co_return;
        }

        /*
         * semaphore is used instead of a condition variable because we don't
         * want to miss a wake-up. the normal way to handle this would be to add
         * a predicate, but the condition isn't a simple boolean flag or two.
         */
        co_await queue->monitor_work_.wait(
          std::max<size_t>(queue->monitor_work_.current(), 1));

        /*
         * in the common case the queue is already open so there is nothing else
         * to do and the monitor can loop around to suspend itself. however, if
         * the queue has also been removed from the lru cache list then this is
         * a signal that the opened queue should be closed and nofiles_ units
         * released.
         */
        if (queue->io_queue_.opened()) {
            if (!queue->lru_hook_.is_linked()) {
                co_await queue->io_queue_.close();
                queue->open_file_limit_units_.return_all();
            }
            continue;
        }

        /*
         * open the queue. if we can't get nofiles units then we'll try to
         * evict another queue and wait for units to become available.
         */
        auto units = seastar::try_get_units(open_file_limit_, 1);
        if (!units.has_value()) {
            if (lru_.empty()) {
                /*
                 * this case is possible, if for example, `nofiles_` was
                 * initialized with 0 units.
                 */
                waiters_++;
            } else {
                /*
                 * signal another queue to be closed. once closed this queue
                 * will acquire a nofiles_ unit and be allowed to open.
                 */
                auto& queue = lru_.front();
                lru_.pop_front();
                queue.monitor_work_.signal();
            }
            units = co_await seastar::get_units(open_file_limit_, 1);
        }

        try {
            co_await queue->io_queue_.open();
            queue->open_file_limit_units_ = std::move(units.value());
            lru_.push_back(*queue);

        } catch (...) {
            log.warn(
              "Unable to open file {}: {}",
              queue->path(),
              std::current_exception());
        }
    }
}

} // namespace experimental::io

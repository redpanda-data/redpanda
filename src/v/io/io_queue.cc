/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */
#include "io/io_queue.h"

#include "base/vassert.h"
#include "base/vlog.h"
#include "logger.h"
#include "ssx/future-util.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/sleep.hh>

namespace experimental::io {

io_queue::io_queue(
  persistence* storage,
  std::filesystem::path path,
  completion_callback_type complete) noexcept
  : storage_(storage)
  , path_(std::move(path))
  , complete_(std::move(complete)) {}

void io_queue::start() noexcept {
    vlog(log.debug, "Starting io queue for {}", path_);
    dispatcher_ = dispatch();
}

seastar::future<> io_queue::stop() noexcept {
    vlog(log.debug, "Stopping io queue for {}", path_);
    auto dispatcher = std::exchange(
      dispatcher_, seastar::make_ready_future<>());
    stop_ = true;
    cond_.signal();
    co_await std::move(dispatcher);
    co_await gate_.close();
}

const std::filesystem::path& io_queue::path() const noexcept { return path_; }

seastar::future<> io_queue::open() noexcept {
    vlog(log.debug, "Opening {}", path_);
    assert(!opened());
    assert(running_.empty());
    file_ = co_await storage_->open(path_);
    cond_.signal();
}

bool io_queue::opened() const noexcept { return file_ != nullptr; }

/*
 * closing the file always succeeds. once seastar::file::close is invoked the
 * handle is not longer usable, so we swallow any errors here and release units
 * to avoid blocking other queues as there isn't much else we can do.
 *
 * failure to close a file is quite rare outside of the normal cases like trying
 * to close an unknown or already closed file handle. these cases won't occur
 * using the seastar api since it prevents those mistakes. in any case, what we
 * care about most when closing a file is that its file descriptor resource is
 * freed in the kernel to avoid reaching an open file limit.
 */
seastar::future<> io_queue::close() noexcept {
    vlog(log.debug, "Closing {}", path_);
    assert(opened());

    // ensure all on-going ops have completed
    auto units = co_await seastar::get_units(
      ops_, seastar::semaphore::max_counter());

    auto file = std::exchange(file_, nullptr);
    try {
        co_await file->close();
    } catch (...) {
        vlog(
          log.warn,
          "Unable to close file {}: {}",
          path_,
          std::current_exception());
    }
}

void io_queue::submit_read(page& page) noexcept {
    page.set_flag(page::flags::read);
    enqueue_pending(page);
}

void io_queue::submit_write(page& page) noexcept {
    /*
     * if the page is not on any list, then in queue it.
     */
    if (!page.io_queue_hook.is_linked()) {
        page.set_flag(page::flags::write);
        enqueue_pending(page);
        return;
    }

    assert(page.test_flag(page::flags::write));

    /*
     * if it isn't queued, then that means the dispatcher has handled it. in
     * this case, new data may have arrived into the page. we mark it as queued
     * as a signal to the I/O completion continuation to requeue the write.
     */
    if (!page.test_flag(page::flags::queued)) {
        vlog(log.debug, "Marking inflight I/O to be requeued");
        page.set_flag(page::flags::queued);
    }
}

void io_queue::enqueue_pending(page& page) noexcept {
    pending_.push_back(page);
    page.set_flag(page::flags::queued);
    cond_.signal();
}

seastar::future<> io_queue::dispatch() noexcept {
    while (true) {
        co_await cond_.wait([this] { return !pending_.empty() || stop_; });

        if (stop_) {
            co_return;
        }

        auto units = co_await seastar::get_units(ops_, 1);

        /*
         * dispatch loop and close() run concurrently, and synchronize ont he
         * ops_ semaphore. if dispatch wakes up after close() completed, then we
         * need to identify that case here--we no longer have a file descriptor.
         */
        if (file_ == nullptr) {
            continue;
        }

        auto& page = pending_.front();
        pending_.pop_front();
        assert(page.test_flag(page::flags::queued));
        page.clear_flag(page::flags::queued);
        running_.push_back(page);

        if (page.test_flag(page::flags::read)) {
            dispatch_read(page, std::move(units));

        } else if (page.test_flag(page::flags::write)) {
            dispatch_write(page, std::move(units));

        } else {
            vassert(false, "Expected a read or write flag");
        }
    }
}

void io_queue::dispatch_read(
  page& page, seastar::semaphore_units<> units) noexcept {
    vlog(
      log.debug,
      "Reading {} at {}~{}",
      path_,
      page.offset(),
      page.data().size());

    ssx::background = seastar::with_gate(
      gate_, [this, &page, units = std::move(units)]() mutable {
          return file_
            ->dma_read(
              page.offset(), page.data().get_write(), page.data().size())
            .then([this, &page, units = std::move(units)](auto) mutable {
                /*
                 * unlike writes, reads are not requeued. the caller should
                 * handle read/write coherency.
                 */
                running_.erase(request_list_type::s_iterator_to(page));
                if (complete_) {
                    units.return_all();
                    complete_(page);
                }
            })
            .handle_exception([this](std::exception_ptr eptr) {
                vassert(false, "Read failed to {}: {}", path_, eptr);
            });
      });
}

void io_queue::dispatch_write(
  page& page, seastar::semaphore_units<> units) noexcept {
    vlog(
      log.debug,
      "Writing {} at {}~{}",
      path_,
      page.offset(),
      page.data().size());

    ssx::background = seastar::with_gate(
      gate_, [this, &page, units = std::move(units)]() mutable {
          return file_
            ->dma_write(page.offset(), page.data().get(), page.data().size())
            .then([this, &page, units = std::move(units)](auto) mutable {
                running_.erase(request_list_type::s_iterator_to(page));
                units.return_all();
                /*
                 * the queued flag is cleared before dispatch_write is invoked,
                 * but while the write is in flight, submit_write may be called
                 * again. for example, if more data was written into the page
                 * and the page needs to be written again. it's requeued here.
                 *
                 * completion callback is invoked only after the write completes
                 * without being requeued.
                 */
                if (page.test_flag(page::flags::queued)) {
                    pending_.push_back(page);
                    cond_.signal();
                } else if (complete_) {
                    complete_(page);
                }
            })
            .handle_exception([this](std::exception_ptr eptr) {
                vassert(false, "Write failed to {}: {}", path_, eptr);
            });
      });
}

} // namespace experimental::io

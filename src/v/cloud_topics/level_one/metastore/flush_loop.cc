/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#include "cloud_topics/level_one/metastore/flush_loop.h"

#include "base/vlog.h"
#include "cloud_topics/level_one/metastore/metastore.h"
#include "cloud_topics/logger.h"
#include "config/configuration.h"
#include "ssx/semaphore.h"
#include "ssx/time.h"

namespace cloud_topics::l1 {

// Loop that flushes the metastore periodically until stopped.
class flush_loop {
public:
    explicit flush_loop(
      metastore* metastore,
      config::binding<std::chrono::milliseconds> flush_interval)
      : metastore_(metastore)
      , flush_interval_(std::move(flush_interval)) {
        flush_interval_.watch([this] { sem_.signal(); });
    }

    void start() {
        ssx::spawn_with_gate(gate_, [this] { return run_loop(); });
    }

    ss::future<> stop_and_wait() {
        vlog(cd_log.debug, "Metastore flush loop stopping...");
        as_.request_abort();
        sem_.broken();
        co_await gate_.close();
        vlog(cd_log.debug, "Metastore flush loop stopped");
    }

private:
    ss::future<> run_loop() {
        const auto retry_interval = ssx::duration::seconds(10);
        while (!as_.abort_requested()) {
            auto start = ssx::instant::from_chrono(ss::lowres_clock::now());
            // Periodic flushes let a domain that flushed recently decline, so
            // that retries after a failed partition don't force every healthy
            // domain to persist a small SST every retry interval.
            auto res = co_await metastore_->flush(
              metastore::flush_type::skip_if_recent);
            auto finish = ssx::instant::from_chrono(ss::lowres_clock::now());

            ssx::duration sleep_duration;
            if (!res.has_value()) {
                vlog(
                  cd_log.warn,
                  "Failed to flush metastore, retrying in {}: {}",
                  retry_interval,
                  res.error());
                sleep_duration = retry_interval;
            } else {
                auto flush_interval = ssx::duration::from_chrono(
                  flush_interval_());
                auto flush_time = finish - start;
                sleep_duration = flush_interval - flush_time;
            }

            if (sleep_duration > ssx::duration::zero()) {
                try {
                    co_await sem_.wait(
                      sleep_duration.to_chrono<std::chrono::milliseconds>(),
                      std::max(sem_.current(), size_t(1)));
                } catch (const ss::semaphore_timed_out&) {
                    // Time to wake up! Continue onto the next iteration.
                } catch (...) {
                    auto eptr = std::current_exception();
                    auto log_lvl = ssx::is_shutdown_exception(eptr)
                                     ? ss::log_level::debug
                                     : ss::log_level::warn;
                    vlogl(
                      cd_log,
                      log_lvl,
                      "Metastore flush loop hit exception while sleeping: {}",
                      eptr);
                }
            }
        }
    }

    ss::gate gate_;
    ss::abort_source as_;
    metastore* metastore_;
    config::binding<std::chrono::milliseconds> flush_interval_;
    ssx::semaphore sem_{0, "flush_loop"};
};

flush_loop_manager::flush_loop_manager(metastore* metastore)
  : metastore_(metastore) {}

flush_loop_manager::~flush_loop_manager() = default;

ss::future<> flush_loop_manager::reset_flush_loop(
  flush_loop_manager::needs_loop needs_loop) {
    if (!needs_loop) {
        // We should not have a running loop. Stop it if one exists.
        if (flush_loop_) {
            auto loop = std::exchange(flush_loop_, nullptr);
            auto stop_fut = co_await ss::coroutine::as_future(
              loop->stop_and_wait());
            if (stop_fut.failed()) {
                auto ex = stop_fut.get_exception();
                vlog(cd_log.error, "Stopping flush loop failed: {}", ex);
            }
        }
        co_return;
    }
    if (flush_loop_) {
        // We need a loop and already have one.
        co_return;
    }
    auto loop = std::make_unique<flush_loop>(
      metastore_,
      config::shard_local_cfg().cloud_topics_long_term_flush_interval.bind());
    loop->start();
    flush_loop_ = std::move(loop);
}

void flush_loop_manager::enqueue_loop_reset(
  flush_loop_manager::needs_loop needs_loop) {
    tell(needs_loop);
}

ss::future<>
flush_loop_manager::process(flush_loop_manager::needs_loop needs_loop) {
    return reset_flush_loop(needs_loop);
}

void flush_loop_manager::on_error(std::exception_ptr ex) noexcept {
    vlog(cd_log.error, "Unexpected flush loop manager error: {}", ex);
}

ss::future<> flush_loop_manager::stop() {
    co_await actor::stop();
    if (flush_loop_) {
        auto fut = co_await ss::coroutine::as_future(
          flush_loop_->stop_and_wait());
        if (fut.failed()) {
            auto ex = fut.get_exception();
            vlog(cd_log.error, "Error stopping flush loop manager: {}", ex);
        }
    }
}

} // namespace cloud_topics::l1

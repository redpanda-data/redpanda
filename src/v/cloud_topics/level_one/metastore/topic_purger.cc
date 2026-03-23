/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#include "cloud_topics/level_one/metastore/topic_purger.h"

#include "base/vlog.h"
#include "cloud_topics/level_one/metastore/metastore.h"
#include "cloud_topics/level_one/metastore/retry.h"
#include "cloud_topics/logger.h"
#include "cluster/topic_table.h"
#include "cluster/topics_frontend.h"
#include "ssx/sleep_abortable.h"

using namespace std::chrono_literals;

namespace cloud_topics::l1 {

topic_purger::topic_purger(
  metastore* metastore,
  cluster::topic_table* topics,
  remove_tombstone_fn_t remove_fn)
  : metastore_(metastore)
  , topics_(topics)
  , remove_tombstone_(std::move(remove_fn)) {}

ss::future<std::expected<void, topic_purger::error>>
topic_purger::purge_tombstoned_topics(ss::abort_source* as) {
    static constexpr auto max_topics_per_req = 10;
    static constexpr auto max_concurrent_purges = 10;
    while (true) {
        const auto& tombstones = topics_->get_cloud_topic_tombstones();
        chunked_vector<model::topic_id> topics_to_remove;
        chunked_vector<cluster::nt_revision> ntrs_to_purge;
        for (const auto& [ntr, tombstone] : tombstones) {
            if (topics_to_remove.size() == max_topics_per_req) {
                break;
            }
            auto tid = tombstone.topic_id;
            topics_to_remove.emplace_back(tid);
            ntrs_to_purge.emplace_back(ntr);
        }
        if (topics_to_remove.empty()) {
            // No tombstones to remove! We're done!
            co_return std::expected<void, error>{};
        }

        // TODO: ensure all reconcilers for the given topics are stopped,
        // otherwise we may end up with orphaned topics in the metastore.
        while (!topics_to_remove.empty()) {
            auto remove_res = co_await retry_metastore_op_with_default_rtc(
              [this, &topics_to_remove]() {
                  return metastore_->remove_topics(topics_to_remove);
              },
              *as);
            if (!remove_res.has_value()) {
                co_return std::unexpected(
                  error{fmt::format(
                    "Error removing topics from metastore: {}",
                    remove_res.error())});
            }
            const auto& resp = remove_res.value();
            if (resp.not_removed.empty()) {
                break;
            }
            vlog(
              cd_log.debug,
              "Retrying removal of {} topics",
              resp.not_removed.size());
            topics_to_remove.clear();
            for (const auto& t : resp.not_removed) {
                topics_to_remove.push_back(t);
            }
        }
        if (as->abort_requested()) {
            co_return std::unexpected(error{"Shutting down topic purger"});
        }
        // The metastore update has succeeded, go ahead and purge all the
        // topics we just removed.
        std::optional<topic_purger::error> first_error;
        auto purge_fut = co_await ss::coroutine::as_future(
          ss::max_concurrent_for_each(
            ntrs_to_purge,
            max_concurrent_purges,
            [this, as, &first_error](const cluster::nt_revision& ntr) {
                as->check();
                return remove_tombstone_(ntr).then(
                  [&first_error](
                    const topic_purger::remove_tombstone_ret_t& ret) {
                      if (!ret.has_value() && !first_error.has_value()) {
                          first_error = ret.error();
                      }
                  });
            }));
        if (purge_fut.failed()) {
            auto ex = purge_fut.get_exception();
            co_return std::unexpected(
              error{fmt::format("Exception purging tombstones: {}", ex)});
        }
        if (first_error.has_value()) {
            co_return std::unexpected(
              error{fmt::format("Error purging tombstones: {}", *first_error)});
        }
    }
}

// Simple loop that purges topics periodically until stopped.
class topic_purge_loop {
public:
    topic_purge_loop(
      metastore* metastore,
      cluster::topic_table* topics,
      cluster::topics_frontend* topics_fe)
      : metastore_(metastore)
      , purger_(
          metastore_, topics, [topics_fe](const cluster::nt_revision& ntr) {
              return topics_fe
                ->purged_topic(
                  ntr, cluster::topic_purge_domain::cloud_topic, 5s)
                .then(
                  [&ntr](const cluster::topic_result& res)
                    -> topic_purger::remove_tombstone_ret_t {
                      if (res.ec != cluster::errc::success) {
                          return std::unexpected(
                            topic_purger::error{fmt::format(
                              "Error purging topic {}: {}", ntr, res)});
                      }
                      return std::nullopt;
                  });
          }) {}

    void start() {
        ssx::spawn_with_gate(gate_, [this] { return run_loop(); });
    }

    ss::future<> stop_and_wait() {
        vlog(cd_log.debug, "Topic purge loop stopping...");
        as_.request_abort();
        co_await gate_.close();
        vlog(cd_log.debug, "Topic purge loop stopped...");
    }

private:
    ss::future<> run_loop() {
        while (!as_.abort_requested()) {
            static const auto purge_interval = 30s;
            auto res = co_await purger_.purge_tombstoned_topics(&as_);
            if (!res.has_value()) {
                vlog(
                  cd_log.warn,
                  "Failed to purge tombstoned cloud topics: {}",
                  res.error());
            }
            auto sleep_res = co_await ss::coroutine::as_future(
              ssx::sleep_abortable(purge_interval, as_));
            if (sleep_res.failed()) {
                auto eptr = sleep_res.get_exception();
                auto log_lvl = ssx::is_shutdown_exception(eptr)
                                 ? ss::log_level::debug
                                 : ss::log_level::warn;
                vlogl(
                  cd_log,
                  log_lvl,
                  "Topic purge loop hit exception while sleeping: {}",
                  eptr);
            }
        }
    }

    ss::gate gate_;
    ss::abort_source as_;
    metastore* metastore_;
    topic_purger purger_;
};

topic_purger_manager::topic_purger_manager(
  metastore* metastore,
  ss::sharded<cluster::topic_table>* topics,
  ss::sharded<cluster::topics_frontend>* topics_fe)
  : metastore_(metastore)
  , topics_(topics)
  , topics_fe_(topics_fe)
  , queue_([](const std::exception_ptr& ex) {
      vlog(cd_log.error, "Unexpected topic purger manager error: {}", ex);
  }) {}

topic_purger_manager::~topic_purger_manager() = default;

ss::future<> topic_purger_manager::reset_purge_loop(
  topic_purger_manager::needs_loop needs_loop) {
    if (!needs_loop) {
        // We should not have a running loop.
        if (topic_purge_loop_) {
            auto purge_loop = std::exchange(topic_purge_loop_, nullptr);
            auto stop_fut = co_await ss::coroutine::as_future(
              purge_loop->stop_and_wait());
            if (stop_fut.failed()) {
                auto ex = stop_fut.get_exception();
                vlog(cd_log.error, "Stopping purge loop failed: {}", ex);
            }
        }
        co_return;
    }
    // We need a running loop.
    if (topic_purge_loop_) {
        co_return;
    }
    auto loop = std::make_unique<topic_purge_loop>(
      metastore_, &topics_->local(), &topics_fe_->local());
    loop->start();
    topic_purge_loop_ = std::move(loop);
}

void topic_purger_manager::enqueue_loop_reset(
  topic_purger_manager::needs_loop needs_loop) {
    queue_.submit(
      [this, needs_loop]() mutable { return reset_purge_loop(needs_loop); });
}

ss::future<> topic_purger_manager::stop() {
    co_await queue_.shutdown();
    if (topic_purge_loop_) {
        auto fut = co_await ss::coroutine::as_future(
          topic_purge_loop_->stop_and_wait());
        if (fut.failed()) {
            auto ex = fut.get_exception();
            vlog(cd_log.error, "Error stopping topic purger manager: {}", ex);
        }
    }
}

} // namespace cloud_topics::l1

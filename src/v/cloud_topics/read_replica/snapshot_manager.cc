/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_topics/read_replica/snapshot_manager.h"

#include "base/vlog.h"
#include "cloud_storage_clients/types.h"
#include "cloud_topics/level_one/common/file_io.h"
#include "cloud_topics/logger.h"
#include "cloud_topics/read_replica/snapshot_metastore.h"
#include "config/configuration.h"
#include "lsm/io/cloud_persistence.h"
#include "ssx/future-util.h"
#include "ssx/sleep_abortable.h"

#include <seastar/coroutine/as_future.hh>

#include <exception>

using namespace std::chrono_literals;

namespace cloud_topics::read_replica {

// Starts a loop that periodically refreshes the database and can be signaled
// to refresh immediately if needed.
class database_refresher {
public:
    database_refresher(
      std::filesystem::path staging_directory,
      cloud_storage_clients::bucket_name bucket,
      cloud_io::remote* remote,
      cloud_io::cache* cache,
      l1::domain_uuid);

    void start();
    ss::future<> stop_and_wait();

    // Tells the refresher to refresh immediately. If the refresh loop is
    // sleeping, it wakes up immediately to refresh.
    void signal_refresh();

    // Waits for the database to be refreshed starting at a point at or later
    // than the input time, and returns a snapshot of the database.
    using errc = snapshot_provider::errc;
    using error = snapshot_provider::error;
    ss::future<std::expected<snapshot_handle, error>> get_newer_snapshot(
      ss::lowres_clock::time_point min_refresh_time,
      ss::lowres_clock::duration timeout,
      std::optional<lsm::sequence_number> min_seqno);

    // Test helper: returns the start time of the last successful refresh.
    ss::lowres_clock::time_point last_refresh_time() const {
        return last_refreshed_at_;
    }

private:
    // The loop in charge of opening and refreshing the database.
    ss::future<> run_loop();

    ss::future<> open_or_refresh();

    ss::gate gate_;
    ss::abort_source as_;

    // TODO: integrate with the cloud cache?
    const std::filesystem::path staging_directory_;
    const cloud_storage_clients::bucket_name bucket_;
    cloud_io::remote* remote_;
    l1::domain_uuid domain_uuid_;
    prefix_logger logger_;

    std::optional<lsm::database> db_;
    std::unique_ptr<l1::file_io> io_;

    // Used to sleep in between refreshing the database, but can be interrupted
    // if callers need a refresh now.
    ssx::named_semaphore<> sleep_sem_{0, "refresh_sleep_sem"};

    // Used to wait for refreshes and indicate that callers are waiting on
    // database refreshes.
    ss::condition_variable refreshed_cv_;

    // The last time we started a refresh that succeeded.
    ss::lowres_clock::time_point last_refreshed_at_{
      ss::lowres_clock::time_point::min()};
};

database_refresher::database_refresher(
  std::filesystem::path staging_directory,
  cloud_storage_clients::bucket_name bucket,
  cloud_io::remote* remote,
  cloud_io::cache* cache,
  l1::domain_uuid domain_uuid)
  : staging_directory_(std::move(staging_directory))
  , bucket_(std::move(bucket))
  , remote_(remote)
  , domain_uuid_(domain_uuid)
  , logger_(
      cd_log, fmt::format("database_refresher {}, {}", domain_uuid_, bucket_))
  , io_(
      std::make_unique<l1::file_io>(
        staging_directory_, remote_, bucket_, cache)) {}

void database_refresher::start() {
    ssx::spawn_with_gate(gate_, [this] { return run_loop(); });
}

ss::future<> database_refresher::stop_and_wait() {
    vlog(logger_.debug, "Stopping");
    as_.request_abort();
    sleep_sem_.broken();
    refreshed_cv_.broken();
    co_await gate_.close();
    if (db_) {
        co_await db_->close();
    }
    vlog(logger_.debug, "Stopped");
}

void database_refresher::signal_refresh() { sleep_sem_.signal(); }

ss::future<> database_refresher::open_or_refresh() {
    vlog(logger_.debug, "{}", db_.has_value() ? "Refreshing" : "Opening");
    if (db_.has_value()) {
        co_await db_->refresh();
        vlog(logger_.debug, "Refreshed to seqno {}", db_->max_applied_seqno());
        co_return;
    }
    cloud_storage_clients::object_key domain_prefix{
      domain_cloud_prefix(domain_uuid_)};
    auto data_persist = co_await lsm::io::open_cloud_data_persistence(
      staging_directory_,
      remote_,
      bucket_,
      domain_prefix,
      ss::sstring(domain_uuid_()));
    auto meta_persist = co_await lsm::io::open_cloud_metadata_persistence(
      remote_, bucket_, domain_prefix);
    lsm::io::persistence io{
      .data = std::move(data_persist),
      .metadata = std::move(meta_persist),
    };
    // NOTE: passing the max epoch allows us to find the latest manifest in the
    // domain.
    auto db = co_await lsm::database::open(
      lsm::options{
        .database_epoch = lsm::internal::database_epoch::max(),
        .readonly = true,
        // TODO: tuning.
      },
      std::move(io));
    vlog(logger_.debug, "Opened with seqno {}", db.max_applied_seqno());
    db_ = std::move(db);
}

ss::future<> database_refresher::run_loop() {
    while (!gate_.is_closed()) {
        // Capture the time before refresh completes so we have an accurate
        // idea of when the refresh was started, and can therefore make
        // accurate claims about the staleness of the database.
        auto refresh_time = ss::lowres_clock::now();
        auto refresh_fut = co_await ss::coroutine::as_future(open_or_refresh());
        if (refresh_fut.failed()) {
            auto ex = refresh_fut.get_exception();
            auto lvl = ssx::is_shutdown_exception(ex) ? ss::log_level::debug
                                                      : ss::log_level::warn;
            vlogl(logger_, lvl, "Exception while refreshing database: {}", ex);
            co_await ssx::sleep_abortable(1s, as_);
            continue;
        }
        last_refreshed_at_ = refresh_time;
        refreshed_cv_.broadcast();
        if (refreshed_cv_.has_waiters()) {
            // There are still waiters whose predicates were not satisfied
            // (e.g., waiting for a future min_refresh_time). Sleep briefly
            // before refreshing again to avoid busy-looping.
            auto sleep_fut = co_await ss::coroutine::as_future(
              ssx::sleep_abortable(100ms, as_));
            if (sleep_fut.failed()) {
                auto ex = sleep_fut.get_exception();
                vlog(logger_.debug, "Error sleeping: {}", ex);
            }
            continue;
        }
        auto sync_interval
          = config::shard_local_cfg()
              .cloud_storage_readreplica_manifest_sync_timeout_ms();
        try {
            // Wait to timeout or get signaled by someone in need of an
            // immediate refresh.
            co_await sleep_sem_.wait(sync_interval);
        } catch (const ss::semaphore_timed_out&) {
            // Sync interval has elapsed, time to refresh.
            continue;
        } catch (...) {
            // Expected at shutdown.
            auto ex = std::current_exception();
            vlog(
              logger_.debug, "Exception thrown from sleep semaphore: {}", ex);
        }
    }
    vlog(logger_.debug, "Database refresher run_loop exiting");
}

ss::future<std::expected<snapshot_handle, snapshot_provider::error>>
database_refresher::get_newer_snapshot(
  ss::lowres_clock::time_point min_refresh_time,
  ss::lowres_clock::duration timeout,
  std::optional<lsm::sequence_number> min_seqno) {
    auto gh = gate_.try_hold();
    if (!gh.has_value()) {
        co_return std::unexpected(error(errc::shutting_down));
    }
    vlog(
      logger_.debug,
      "Waiting for snapshot newer than {}, seqno {}",
      min_refresh_time.time_since_epoch(),
      min_seqno);
    auto has_fresh_snapshot = [this, min_refresh_time, min_seqno] {
        return db_ && last_refreshed_at_ >= min_refresh_time
               && (!min_seqno || db_->max_applied_seqno() >= *min_seqno);
    };
    if (!has_fresh_snapshot()) {
        try {
            signal_refresh();
            co_await refreshed_cv_.when(timeout, std::move(has_fresh_snapshot));
        } catch (const ss::condition_variable_timed_out&) {
            co_return std::unexpected(
              error(errc::io_error, "Timed out waiting for fresh snapshot"));
        } catch (...) {
            auto ex = std::current_exception();
            co_return std::unexpected(
              error(errc::shutting_down, "Shutting down: {}", ex));
        }
    }
    auto seqno = db_->max_applied_seqno();
    vlog(logger_.debug, "Returning snapshot seqno {}", seqno);
    co_return snapshot_handle{
      .metastore = std::make_unique<snapshot_metastore>(
        std::move(*gh), db_->create_snapshot()),
      .seqno = seqno,
      .io = io_.get(),
    };
}

snapshot_manager::snapshot_manager(
  std::filesystem::path staging_dir,
  cloud_io::remote* remote,
  cloud_io::cache* cache)
  : staging_dir_(std::move(staging_dir))
  , remote_(remote)
  , cache_(cache) {}

ss::future<std::expected<snapshot_handle, snapshot_manager::error>>
snapshot_manager::get_snapshot(
  l1::domain_uuid domain_uuid,
  cloud_storage_clients::bucket_name bucket,
  ss::lowres_clock::time_point earliest_refresh,
  std::optional<lsm::sequence_number> min_seqno,
  ss::lowres_clock::duration timeout) {
    auto gh = gate_.try_hold();
    if (!gh.has_value()) {
        co_return std::unexpected(error(errc::shutting_down));
    }
    auto db_it = databases_.find(domain_uuid);
    if (db_it == databases_.end()) {
        auto [it, inserted] = databases_.try_emplace(domain_uuid);
        vassert(inserted, "Expected new entry to be inserted");

        auto& entry = it->second;
        entry.refresher = std::make_unique<database_refresher>(
          staging_dir_, bucket, remote_, cache_, domain_uuid);

        // Set up the timer so that when it fires (after enough of an idle
        // period) it cleans up the database.
        entry.idle_timer = std::make_unique<ss::timer<ss::lowres_clock>>();
        entry.idle_timer->set_callback([this, domain_uuid] {
            auto it = databases_.find(domain_uuid);
            if (it == databases_.end()) {
                return;
            }
            if (it->second.num_waiters != 0) {
                // This shouldn't happen, but in case it does, be conservative
                // and exit without destructing anything.
                vlog(
                  cd_log.error,
                  "Idle callback called for {} with {} active waiters",
                  domain_uuid,
                  it->second.num_waiters);
                return;
            }
            // Make sure to not clobber over the timer while we're running the
            // callback.
            auto db = std::move(it->second.refresher);
            auto t = std::move(it->second.idle_timer);
            databases_.erase(it);
            ssx::spawn_with_gate(
              gate_, [db = std::move(db), t = std::move(t)] mutable {
                  return db->stop_and_wait().finally(
                    [db = std::move(db), t = std::move(t)] mutable {});
              });
        });

        entry.refresher->start();
        db_it = it;
    }
    // The database is no longer idle if we're accessing it, so cancel the
    // timer.
    db_it->second.idle_timer->cancel();
    ++db_it->second.num_waiters;

    auto& db_refresher = db_it->second.refresher;
    auto snap_handle = co_await db_refresher->get_newer_snapshot(
      earliest_refresh, timeout, min_seqno);

    // Refresh the lookup after the above scheduling point.
    db_it = databases_.find(domain_uuid);

    // The only thing that can remove this database is the idle timer callback,
    // and that should only ever be called if there are no fibers running
    // through getting a snapshot (which is what we're doing right now).
    vassert(
      db_it != databases_.end(), "Database removed with waiter in flight");
    --db_it->second.num_waiters;

    // Only the last fiber to exit this method should arm the timer, ensuring
    // that other fibers don't arm this while we're waiting for a new snapshot
    // and stop the refresher out from under us.
    if (db_it->second.num_waiters == 0) {
        auto sync_interval
          = config::shard_local_cfg()
              .cloud_storage_readreplica_manifest_sync_timeout_ms();
        db_it->second.idle_timer->arm(
          ss::lowres_clock::now() + sync_interval * 2);
    }
    if (!snap_handle.has_value()) {
        co_return std::unexpected(snap_handle.error());
    }
    co_return std::move(snap_handle.value());
}

ss::future<> snapshot_manager::stop() {
    vlog(cd_log.info, "Stopping read replica snapshot manager");

    // Cancel all timers first so they don't kick off background cleanup of any
    // of our databases.
    for (auto& [uuid, entry] : databases_) {
        entry.idle_timer->cancel();
    }
    co_await gate_.close();

    // Stop all databases.
    auto dbs = std::move(databases_);
    for (auto& [uuid, db] : dbs) {
        co_await db.refresher->stop_and_wait();
        db.refresher.reset();
    }
    vlog(cd_log.info, "Stopped read replica snapshot manager");
}

std::optional<ss::lowres_clock::time_point>
snapshot_manager::last_refresh_time(l1::domain_uuid domain) const {
    auto it = databases_.find(domain);
    if (it == databases_.end()) {
        return std::nullopt;
    }
    return it->second.refresher->last_refresh_time();
}

snapshot_manager::database_entry::~database_entry() = default;

} // namespace cloud_topics::read_replica

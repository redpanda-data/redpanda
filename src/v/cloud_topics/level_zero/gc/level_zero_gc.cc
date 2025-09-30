/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#include "cloud_topics/level_zero/gc/level_zero_gc.h"

#include "base/vlog.h"
#include "cloud_io/remote.h"
#include "cloud_topics/logger.h"
#include "cloud_topics/object_utils.h"
#include "cluster/health_monitor_frontend.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/sleep.hh>
#include <seastar/coroutine/as_future.hh>

namespace cloud_topics {

class object_storage_remote_impl : public level_zero_gc::object_storage {
public:
    // TODO(noah) some random-but-not-awful values for the retry chain that
    // cloud io requires. will need to be fine tuned at some point.
    static constexpr std::chrono::seconds timeout{5};
    static constexpr std::chrono::seconds backoff{1};

    object_storage_remote_impl(
      cloud_io::remote* remote, cloud_storage_clients::bucket_name bucket)
      : remote_(remote)
      , bucket_(std::move(bucket)) {}

    seastar::future<std::expected<
      cloud_storage_clients::client::list_bucket_result,
      cloud_storage_clients::error_outcome>>
    list_objects(seastar::abort_source* asrc) override {
        retry_chain_node rtc(*asrc, timeout, backoff);
        auto res = co_await remote_->list_objects(
          bucket_, rtc, object_path_factory::level_zero_data_dir());
        if (res.has_value()) {
            co_return std::move(res).assume_value();
        }
        co_return std::unexpected(res.assume_error());
    }

    seastar::future<std::expected<void, cloud_io::upload_result>>
    delete_objects(
      seastar::abort_source* asrc,
      std::vector<cloud_storage_clients::client::list_bucket_item> objects)
      override {
        retry_chain_node rtc(*asrc, timeout, backoff);
        auto keys
          = objects | std::views::transform([](auto& obj) { return obj.key; })
            | std::ranges::to<std::vector<cloud_storage_clients::object_key>>();
        auto res = co_await remote_->delete_objects(
          bucket_, keys, rtc, [](auto) {});
        if (res == cloud_io::upload_result::success) {
            co_return std::expected<void, cloud_io::upload_result>();
        }
        co_return std::unexpected(res);
    }

private:
    cloud_io::remote* remote_;
    const cloud_storage_clients::bucket_name bucket_;
};

class epoch_source_cluster_impl : public level_zero_gc::epoch_source {
public:
    explicit epoch_source_cluster_impl(
      seastar::sharded<cluster::health_monitor_frontend>* health_monitor)
      : health_monitor_(health_monitor) {}

    seastar::future<std::expected<std::optional<cluster_epoch>, std::string>>
    max_gc_eligible_epoch(seastar::abort_source*) override {
        /*
         * TODO(noah): missing an integration of epochs with the partition
         * table. for now we report no eligible epoch.
         */
        co_return std::nullopt;
    }

private:
    [[maybe_unused]] seastar::sharded<cluster::health_monitor_frontend>*
      health_monitor_;
};

level_zero_gc::level_zero_gc(
  level_zero_gc_config config,
  std::unique_ptr<object_storage> storage,
  std::unique_ptr<epoch_source> epoch_source)
  : config_(config)
  , storage_(std::move(storage))
  , epoch_source_(std::move(epoch_source))
  , should_run_(false) // begin in a stopped state
  , should_shutdown_(false)
  , worker_(worker()) {}

level_zero_gc::level_zero_gc(
  cloud_io::remote* remote,
  cloud_storage_clients::bucket_name bucket,
  seastar::sharded<cluster::health_monitor_frontend>* health_monitor,
  level_zero_gc_config config)
  : level_zero_gc(
      config,
      std::make_unique<object_storage_remote_impl>(remote, std::move(bucket)),
      std::make_unique<epoch_source_cluster_impl>(health_monitor)) {}

void level_zero_gc::start() {
    vlog(cd_log.info, "Starting cloud topics L0 GC worker");
    should_run_ = true;
    worker_cv_.signal();
}

void level_zero_gc::pause() {
    vlog(cd_log.info, "Pausing cloud topics L0 GC worker");
    should_run_ = false;
    asrc_.request_abort();
}

seastar::future<> level_zero_gc::stop() {
    vlog(cd_log.info, "Stopping cloud topics L0 GC worker");
    should_shutdown_ = true;
    asrc_.request_abort();
    worker_cv_.signal();
    co_await std::exchange(worker_, seastar::make_ready_future<>());
}

// internal error codes used between the worker fiber and the main GC function
enum class level_zero_gc::collection_error : int8_t {
    // problem occurred interacting with the storage or epoch services
    service_error,
    // the cluster is reporting that no collectible epoch exists
    no_collectible_epoch,
    // object listing contained an invalid object name
    invalid_object_name,
};

seastar::future<> level_zero_gc::worker() {
    std::chrono::milliseconds backoff{0};

    while (true) {
        try {
            co_await worker_cv_.wait(
              [this] { return should_run_ || should_shutdown_; });

            if (should_shutdown_) {
                break;
            }

            // stop() and shutdown() may request an abort, but only the worker
            // may subscribe or reset the abort source since it is able to
            // ensure that the abort source is unreferenced at this time.
            asrc_ = {};

            if (backoff.count() > 0) {
                (co_await seastar::coroutine::as_future(
                   seastar::sleep_abortable(backoff, asrc_)))
                  .ignore_ready_future();
                backoff = std::chrono::seconds{0};
            }

            auto res = co_await try_to_collect();
            if (res.has_value()) {
                if (res.value() > 0) {
                    backoff = config_.throttle_progress;
                } else {
                    backoff = config_.throttle_no_progress;
                }
            } else {
                switch (res.error()) {
                case collection_error::service_error:
                case collection_error::invalid_object_name:
                case collection_error::no_collectible_epoch:
                    backoff = config_.throttle_no_progress;
                }
            }

        } catch (...) {
            vlog(
              cd_log.info,
              "Level zero GC restarting after error: {}",
              std::current_exception());
            backoff = config_.throttle_no_progress;
        }
    }

    vlog(cd_log.info, "Level zero GC worker is exiting");
}

seastar::future<std::expected<size_t, level_zero_gc::collection_error>>
level_zero_gc::try_to_collect() {
    const auto candidate_objects = co_await storage_->list_objects(&asrc_);
    if (!candidate_objects.has_value()) {
        vlog(
          cd_log.debug,
          "Received error listing objects during L0 GC: {}",
          candidate_objects.error());
        co_return std::unexpected(collection_error::service_error);
    }

    const auto maybe_max_gc_epoch
      = co_await epoch_source_->max_gc_eligible_epoch(&asrc_);
    if (!maybe_max_gc_epoch.has_value()) {
        vlog(
          cd_log.debug,
          "Received error retrieving GC eligible epoch: {}",
          maybe_max_gc_epoch.error());
        co_return std::unexpected(collection_error::service_error);
    }

    const auto max_gc_epoch = maybe_max_gc_epoch.value();
    if (!max_gc_epoch.has_value()) {
        vlog(cd_log.info, "No GC eligible epoch currently exists");
        co_return std::unexpected(collection_error::no_collectible_epoch);
    }

    const auto max_gc_birthday = std::chrono::system_clock::now()
                                 - config_.deletion_grace_period;

    // objects that can be safely deleted
    std::vector<cloud_storage_clients::client::list_bucket_item>
      eligible_objects;

    // used to detect unsorted object listings
    seastar::sstring last_key;
    std::optional<cluster_epoch> last_epoch;

    for (const auto& object : candidate_objects.value().contents) {
        const auto object_epoch = object_path_factory::level_zero_path_to_epoch(
          object.key);

        // validate expected L0 object name format, and extract epoch
        if (!object_epoch.has_value()) {
            vlog(
              cd_log.error,
              "Unable to parse epoch during L0 GC: {}",
              object_epoch.error());
            co_return std::unexpected(collection_error::invalid_object_name);
        }

        // detect non-lexicographic ordering. this may indicate that GC will not
        // operate efficiently with the underlying storage system. see the class
        // comment for more details about what this means in practice.
        if (!last_epoch.has_value()) {
            last_key = object.key;
            last_epoch = object_epoch.value();
        }

        if (object_epoch.value() < last_epoch) {
            constexpr std::chrono::minutes rate_limit{1};
            static seastar::logger::rate_limit rate(rate_limit);
            vloglr(
              cd_log,
              seastar::log_level::error,
              rate,
              "Non-lexicographic object listing detected during L0 GC {} < {}",
              object.key,
              last_key);
        }

        last_key = object.key;
        last_epoch = object_epoch.value();

        // object's epoch is not yet eligible
        if (object_epoch.value() > max_gc_epoch.value()) {
            vlog(
              cd_log.debug,
              "Ignoring object with non-collectible epoch: {} > {}",
              object.key,
              max_gc_epoch.value());
            continue;
        }

        // object is too young
        if (object.last_modified > max_gc_birthday) {
            vlog(
              cd_log.debug,
              "Ignoring object with too recent creation time: {} @ {} < {}",
              object.key,
              object.last_modified,
              max_gc_birthday);
            continue;
        }

        eligible_objects.push_back(object);
    }

    const auto num_eligible = eligible_objects.size();

    auto res = co_await storage_->delete_objects(
      &asrc_, std::move(eligible_objects));
    if (!res.has_value()) {
        vlog(
          cd_log.info,
          "Received an error deleting L0 data objects: {}",
          res.error());
        co_return std::unexpected(collection_error::service_error);
    }

    vlog(
      cd_log.info, "Deleted {} L0 data objects eligible for GC", num_eligible);

    co_return num_eligible;
}

} // namespace cloud_topics

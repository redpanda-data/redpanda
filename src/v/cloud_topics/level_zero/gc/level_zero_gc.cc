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

#include "base/format_to.h"
#include "base/vlog.h"
#include "cloud_io/remote.h"
#include "cloud_storage_clients/types.h"
#include "cloud_topics/logger.h"
#include "cloud_topics/object_utils.h"
#include "cluster/health_monitor_frontend.h"
#include "cluster/members_table.h"
#include "cluster/topic_table.h"
#include "ssx/semaphore.h"
#include "ssx/work_queue.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/shard_id.hh>
#include <seastar/core/sleep.hh>
#include <seastar/coroutine/as_future.hh>

namespace cloud_topics {

class level_zero_gc::list_delete_worker {
public:
    explicit list_delete_worker(
      std::unique_ptr<object_storage> storage,
      std::unique_ptr<node_info> node_info,
      level_zero_gc_probe& probe)
      : storage_(std::move(storage))
      , node_info_(std::move(node_info))
      , probe_(&probe)
      , worker_([](std::exception_ptr eptr) {
          vlog(cd_log.warn, "Exception from delete worker: {}", eptr);
      }) {}
    void start() {
        vlog(cd_log.info, "Starting cloud topics list/delete worker");
        if (as_.abort_requested()) {
            as_ = {};
        }
        if (gate_.is_closed()) {
            gate_ = {};
        }
    }
    void pause() {
        as_.request_abort();
        continuation_token_.reset();
    }
    seastar::future<> stop() {
        if (gate_.is_closed()) {
            co_return;
        }
        vlog(cd_log.info, "Stopping cloud topics list/delete worker");
        as_.request_abort();
        delete_sem_.broken();
        page_sem_.broken();
        co_await worker_.shutdown();
        co_await gate_.close();
        vlog(cd_log.info, "Stopped cloud topics list/delete worker");
    }

    bool has_capacity() const { return page_sem_.available_units() > 0; }

    seastar::future<std::expected<
      chunked_vector<cloud_storage_clients::client::list_bucket_item>,
      cloud_storage_clients::error_outcome>>
    next_page() {
        while (
          !as_.abort_requested()
          && (continuation_token_.has_value() || (curr_prefix_ = next_prefix()).has_value())) {
            vassert(
              curr_prefix_.has_value(),
              "Expected curr_prefix_ to be populated here...");
            auto objects = co_await do_next_page(curr_prefix_.value());

            // we could filter here to ensure that all the prefixes are in
            // range, but if they're not it doesn't really matter. all best
            // effort.
            if (!objects.has_value() || !objects.value().empty()) {
                co_return std::move(objects);
            }

            // nothing to do...try the next prefix
        }
        co_return chunked_vector<
          cloud_storage_clients::client::list_bucket_item>{};
    }

    size_t delete_objects(
      chunked_vector<cloud_storage_clients::client::list_bucket_item> objects,
      size_t keys_total_bytes) {
        auto n_objects = objects.size();
        if (n_objects > 0) {
            auto u = seastar::try_get_units(page_sem_, keys_total_bytes);
            if (!u.has_value()) {
                vlog(cd_log.trace, "Delete pipeline saturated");
                // take the units unsafely. we don't really care about the
                // memory limit in particular, just don't want to grow
                // unbounded.
                u.emplace(seastar::consume_units(page_sem_, keys_total_bytes));
            }
            worker_.submit([this,
                            o = std::move(objects),
                            u = std::move(u).value()]() mutable {
                return do_delete_objects(std::move(o), std::move(u));
            });
        }
        return n_objects;
    }

    seastar::future<> do_delete_objects(
      chunked_vector<cloud_storage_clients::client::list_bucket_item>
        eligible_objects,
      ssx::semaphore_units page_u) noexcept {
        auto u_fut = co_await ss::coroutine::as_future(
          seastar::get_units(delete_sem_, 1, as_));
        if (u_fut.failed()) {
            vlog(
              cd_log.debug,
              "Failed to get units in delete worker: {}",
              u_fut.get_exception());
            co_return;
        }
        if (gate_.is_closed()) {
            vlog(cd_log.trace, "Gate closed");
            co_return;
        }
        ssx::spawn_with_gate(
          gate_,
          [this,
           u = std::move(u_fut.get()),
           pu = std::move(page_u),
           eo = std::move(eligible_objects)]() mutable {
              const auto num_eligible = eo.size();
              return storage_->delete_objects(&as_, std::move(eo))
                .then([this, num_eligible](
                        std::expected<void, cloud_io::upload_result> res) {
                    if (!res.has_value()) {
                        vlog(
                          cd_log.info,
                          "Received an error deleting L0 data objects: {}",
                          res.error());
                    } else {
                        probe_->objects_deleted(num_eligible);
                        vlog(
                          cd_log.debug,
                          "Deleted {} L0 data objects eligible for GC",
                          num_eligible);
                    }
                })
                .finally([u = std::move(u), pu = std::move(pu)] {})
                .handle_exception([](std::exception_ptr eptr) {
                    vlog(cd_log.debug, "Delete objects failed: {}", eptr);
                });
          });
    }

private:
    std::optional<cloud_storage_clients::object_key> next_prefix() {
        key_prefixes_.set_range(compute_prefix_range(
          node_info_->shard_index(), node_info_->total_shards()));
        return key_prefixes_.consume_prefix();
    }

    seastar::future<std::expected<
      chunked_vector<cloud_storage_clients::client::list_bucket_item>,
      cloud_storage_clients::error_outcome>>
    do_next_page(const cloud_storage_clients::object_key& prefix) {
        vlog(
          cd_log.trace,
          "list_delete_worker: Processing key prefix {}",
          curr_prefix_);
        // cached continuation is single use. pass it to list_objects and
        // null it out immediately.
        auto list_result = co_await storage_->list_objects(
          &as_, curr_prefix_, std::exchange(continuation_token_, std::nullopt));
        if (!list_result.has_value()) {
            co_return std::unexpected{list_result.error()};
        }

        auto objects = std::move(list_result).value();
        if (objects.contents.empty()) {
            co_return std::move(objects.contents);
        }

        // fairly naive approach to caching the token. if the list request
        // failed, we leave the cached token empty, but if some other error
        // occurs while processing a page, we keep the token and "skip" that
        // page. with lexicographically ordered list results and monotonic
        // epochs, any eligible keys in a skipped page are guaranteed to
        // appear in a subsequent round. given the volume of L0 objects at
        // higher throughput rates, we're going to err on the side of making
        // progress (vs performing a perfect sweep of outstanding objects).
        if (objects.is_truncated && !objects.next_continuation_token.empty()) {
            continuation_token_.emplace(
              std::move(objects.next_continuation_token));
        }
        co_return std::move(objects.contents);
    }

    std::unique_ptr<object_storage> storage_;
    std::unique_ptr<node_info> node_info_;
    level_zero_gc_probe* probe_;
    ssx::work_queue worker_;
    // TODO: configurable limits?
    // max number of in-flight delete ops
    ssx::semaphore delete_sem_{5, "ct/gc/delete"};
    // control (approximate) total memory held by gc-eligible list pages in
    // flight (these may be queued depending on delete concurrency)
    ssx::semaphore page_sem_{1_MiB, "ct/gc/page"};
    seastar::abort_source as_{};
    seastar::gate gate_{};
    std::optional<ss::sstring> continuation_token_{};
    std::optional<cloud_storage_clients::object_key> curr_prefix_;
    prefix_compressor key_prefixes_;
};

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
    list_objects(
      seastar::abort_source* asrc,
      std::optional<cloud_storage_clients::object_key> prefix,
      std::optional<ss::sstring> continuation_token) override {
        retry_chain_node rtc(*asrc, timeout, backoff);
        auto res = co_await remote_->list_objects(
          bucket_,
          rtc,
          std::move(prefix).value_or(
            object_path_factory::level_zero_data_dir()),
          std::nullopt /*delimiter*/,
          std::nullopt /*item_filter*/,
          // TODO: should depend on cloud backend (abs is 5000 max)
          1000 /*max_keys*/,
          std::move(continuation_token));
        if (res.has_value()) {
            co_return std::move(res).assume_value();
        }
        co_return std::unexpected(res.assume_error());
    }

    seastar::future<std::expected<void, cloud_io::upload_result>>
    delete_objects(
      seastar::abort_source* asrc,
      chunked_vector<cloud_storage_clients::client::list_bucket_item> objects)
      override {
        retry_chain_node rtc(*asrc, timeout, backoff);
        auto keys = objects
                    | std::views::transform([](auto& obj) { return obj.key; })
                    | std::ranges::to<
                      chunked_vector<cloud_storage_clients::object_key>>();
        auto res = co_await remote_->delete_objects(
          bucket_, std::move(keys), rtc, [](auto) {});
        if (res == cloud_io::upload_result::success) {
            co_return std::expected<void, cloud_io::upload_result>();
        }
        co_return std::unexpected(res);
    }

private:
    cloud_io::remote* remote_;
    const cloud_storage_clients::bucket_name bucket_;
};

seastar::future<std::expected<std::optional<cluster_epoch>, std::string>>
level_zero_gc::epoch_source::max_gc_eligible_epoch(seastar::abort_source* as) {
    /*
     * First retrieve a consistent snapshot of cloud topic partitions. This
     * establishes a set of partitions from which we must obtain an epoch
     * bound on garbage collection.
     */
    auto partitions = co_await get_partitions(as);
    if (!partitions.has_value()) {
        co_return std::unexpected(partitions.error());
    }
    if (partitions.value().partitions.empty()) {
        co_return std::nullopt;
    }

    /*
     * Next we retrieve the latest reported epoch bounds from all cloud
     * topic partitions. The source for this information is distributed,
     * while the source for the `partitions` set above is centralized, and
     * this is why we have these two different collection steps.
     */
    auto gc_epochs = co_await get_partitions_max_gc_epoch(as);
    if (!gc_epochs.has_value()) {
        co_return std::unexpected(gc_epochs.error());
    }

    /*
     * The final result begins as the maximum epoch for the given snapshot.
     * Below we merge the two result sets and walk the final result back to
     * account for the partition with the smallest eligible gc epoch.
     */
    auto result = partitions.value().snap_revision;

    vlog(
      cd_log.debug,
      "Calculating max GC eligible epoch with snapshot epoch {}",
      result);

    for (const auto& partition : partitions.value().partitions) {
        const auto& tp_ns = partition.first;
        auto nit = gc_epochs.value().find(tp_ns);
        if (nit == gc_epochs.value().end()) {
            co_return std::unexpected(
              fmt::format(
                "Topic '{}' in snapshot has no reported max GC epoch", tp_ns));
        }

        for (const auto p_id : partition.second) {
            auto pit = nit->second.find(p_id);
            if (pit == nit->second.end()) {
                co_return std::unexpected(
                  fmt::format(
                    "Partition '{}/{}' in snapshot has no reported max GC "
                    "epoch",
                    tp_ns,
                    p_id));
            }

            // this partition may hold back the max GC eligible epoch
            const auto prev_result = result;
            result = std::min(result, pit->second);

            vlog(
              cd_log.debug,
              "Reducing result {} from min(result={}, p={}) for {}/{}",
              result,
              prev_result,
              pit->second,
              tp_ns,
              p_id);
        }
    }

    co_return result;
}

class epoch_source_impl : public level_zero_gc::epoch_source {
public:
    explicit epoch_source_impl(
      seastar::sharded<cluster::health_monitor_frontend>* health_monitor,
      seastar::sharded<cluster::controller_stm>* controller_stm,
      seastar::sharded<cluster::topic_table>* topic_table)
      : health_monitor_(health_monitor)
      , controller_stm_(controller_stm)
      , topic_table_(topic_table) {}

    seastar::future<std::expected<partitions_snapshot, std::string>>
    get_partitions(seastar::abort_source* as) override {
        const auto& topic_table = topic_table_->local();

        // this revision is for detecting concurrent modifications
        const auto iter_start_rev = topic_table.topics_map_revision();

        /*
         * The controller stm last applied offset is used, as opposed to using
         * the topic table last applied offset, because we need the version to
         * move forward. the controller stm offset is a at the top of the stm
         * hierarchy and is consistent with the topic table last applied offset.
         */
        partitions_snapshot snap;
        snap.snap_revision = cluster_epoch(
          co_await controller_stm_->invoke_on(
            cluster::controller_stm_shard, [](auto& stm) {
                return model::revision_id(stm.get_last_applied_offset());
            }));

        for (const auto& topic : topic_table.topics_map()) {
            // we only care about cloud topics
            if (!topic.second.get_metadata()
                   .get_configuration()
                   .is_cloud_topic()) {
                continue;
            }

            auto& partitions = snap.partitions[topic.first];
            for (const auto& partition : topic.second.partitions) {
                partitions.push_back(model::partition_id(partition.first));
            }

            co_await seastar::maybe_yield();

            if (as && as->abort_requested()) {
                co_return std::unexpected("Abort requested");
            }

            // Detect concurrent changes to the topic table to avoid accessing
            // an invalid iterator.
            try {
                topic_table.check_topics_map_stable(iter_start_rev);
            } catch (...) {
                // TODO: its rare, so should we retry immediately or abort this
                // round and wait for the next GC loop? i think it's a balance
                // of more code/complexity and behavior. For now I think it is
                // fine.
                co_return std::unexpected(
                  "Concurrent container iteration invalidation. Will retry");
            }
        }

        co_return snap;
    }

    seastar::future<std::expected<partitions_max_gc_epoch, std::string>>
    get_partitions_max_gc_epoch(seastar::abort_source* as) override {
        /*
         * Get a recent health report. Partitions use the health reporting
         * mechanism to self-report their max GC eligible epoch.
         */
        constexpr auto health_report_query_timeout = 10s;

        auto health_report
          = co_await health_monitor_->local().get_cluster_health(
            cluster::cluster_report_filter{},
            cluster::force_refresh::no,
            model::timeout_clock::now() + health_report_query_timeout);

        if (!health_report.has_value()) {
            co_return std::unexpected(
              fmt::format(
                "Error retrieving cluster health report: {}",
                health_report.error()));
        }

        partitions_max_gc_epoch result;
        for (const auto& node_health : health_report.value().node_reports) {
            for (const auto& topic_status : node_health->topics) {
                const auto& tp_ns = topic_status.first;
                for (const auto& partition_status : topic_status.second) {
                    /*
                     * calculate the max gc epoch for each partition. the catch
                     * here is that this value is reported through the health
                     * reporting system, and that system reports information for
                     * all partition replicas (leader and followers). so how do
                     * we know which value to use here? first, the max gc epoch
                     * only increases in value. second, we recognize that
                     * all reported values from any replica are valid at (and
                     * forever after) the moment they are reported. third, only
                     * the leader advances the epoch.
                     *
                     * because of the second point, using max gc epoch from any
                     * replica will result in correct behavior, however it may
                     * be pessimistic. using the one from the leader is better,
                     * but leadership is a lagging signal. instead, we can take
                     * the maximum reported as the most optimistic value.
                     *
                     * if a replica reports no epoch then it is considered to be
                     * in an indeterminite state and it has no affect on the
                     * computed result (effectively it is treated as having
                     * epoch 0 in the max reduction across replicas).
                     *
                     * if all replicas report no epoch then the partition is not
                     * included in the result set returned to the caller. this
                     * covers two cases.
                     *
                     * the first case is that the partition is part of a
                     * standard topic. in this case the partition will also not
                     * be in the set returned by `get_partitions` and thus the
                     * join in `max_gc_eligible_epoch` will ignore the topic.
                     *
                     * in the second case the join would fail, and later succeed
                     * in the once at least one replica is returning max gc
                     * epoch. this shouldn't be a problem in practice: there is
                     * a narrow window at start-up time where a partition is
                     * bootstrapping the L0 CT STM state where the state is
                     * unknown. for brand new partitions this should be the
                     * partition's creation revision ID.
                     */
                    const auto maybe_max_gc_epoch
                      = partition_status.second
                          .cloud_topic_max_gc_eligible_epoch;
                    if (!maybe_max_gc_epoch.has_value()) {
                        continue;
                    }
                    const auto max_gc_epoch = cluster_epoch(
                      maybe_max_gc_epoch.value());

                    const auto p_id = partition_status.first;
                    auto& partition_epochs = result[tp_ns];
                    const auto it = partition_epochs.find(p_id);
                    if (it == partition_epochs.end()) {
                        partition_epochs.try_emplace(p_id, max_gc_epoch);
                    } else {
                        it->second = std::max(it->second, max_gc_epoch);
                    }
                }
            }

            /*
             * A scheduling point is injected after looking at each node's
             * report. We own the list of node reports which is a set of shared
             * pointers, so iteration is safe, and the scheduling point is
             * intended to help avoid reactor stalls. If we need to inject
             * scheduling points at a finer granularity we'll need to take a
             * closer look at concurrency rules of the reports themselves.
             */
            co_await seastar::maybe_yield();

            if (as && as->abort_requested()) {
                co_return std::unexpected("Abort requested");
            }
        }

        co_return result;
    }

private:
    seastar::sharded<cluster::health_monitor_frontend>* health_monitor_;
    seastar::sharded<cluster::controller_stm>* controller_stm_;
    seastar::sharded<cluster::topic_table>* topic_table_;
};

class node_info_impl : public level_zero_gc::node_info {
public:
    node_info_impl(
      model::node_id self, seastar::sharded<cluster::members_table>* mt)
      : self_(self)
      , members_table_(mt) {}

    size_t shard_index() const final {
        return shards_up_to(self_) + seastar::this_shard_id();
    }
    size_t total_shards() const final {
        return shards_up_to(model::node_id::max());
    }

private:
    size_t shards_up_to(model::node_id ub) const {
        size_t total{0};
        for (const auto& [id, node] : members_table_->local().nodes()) {
            if (id < ub) {
                total += node.broker.properties().cores;
            }
        }
        return total;
    }
    model::node_id self_;
    seastar::sharded<cluster::members_table>* members_table_;
};

level_zero_gc::level_zero_gc(
  level_zero_gc_config config,
  std::unique_ptr<object_storage> storage,
  std::unique_ptr<epoch_source> epoch_source,
  std::unique_ptr<node_info> node_info)
  : config_(std::move(config))
  , epoch_source_(std::move(epoch_source))
  , should_run_(false) // begin in a stopped state
  , should_shutdown_(false)
  , worker_(worker())
  , probe_(config::shard_local_cfg().disable_metrics())
  , delete_worker_(
      std::make_unique<list_delete_worker>(
        std::move(storage), std::move(node_info), probe_)) {}

level_zero_gc::level_zero_gc(
  model::node_id self,
  cloud_io::remote* remote,
  cloud_storage_clients::bucket_name bucket,
  seastar::sharded<cluster::health_monitor_frontend>* health_monitor,
  seastar::sharded<cluster::controller_stm>* controller_stm,
  seastar::sharded<cluster::topic_table>* topic_table,
  seastar::sharded<cluster::members_table>* members_table)
  : level_zero_gc(
      level_zero_gc_config{
        .deletion_grace_period
        = config::shard_local_cfg()
            .cloud_topics_short_term_gc_minimum_object_age.bind(),
        .throttle_progress
        = config::shard_local_cfg().cloud_topics_short_term_gc_interval.bind(),
        .throttle_no_progress
        = config::shard_local_cfg()
            .cloud_topics_short_term_gc_backoff_interval.bind(),
      },
      std::make_unique<object_storage_remote_impl>(remote, std::move(bucket)),
      std::make_unique<epoch_source_impl>(
        health_monitor, controller_stm, topic_table),
      std::make_unique<node_info_impl>(self, members_table)) {}

level_zero_gc::~level_zero_gc() = default;

void level_zero_gc::start() {
    vlog(cd_log.info, "Starting cloud topics L0 GC worker");
    delete_worker_->start();
    should_run_ = true;
    worker_cv_.signal();
}

void level_zero_gc::pause() {
    vlog(cd_log.info, "Pausing cloud topics L0 GC worker");
    should_run_ = false;
    asrc_.request_abort();
    delete_worker_->pause();
}

seastar::future<> level_zero_gc::stop() {
    vlog(cd_log.info, "Stopping cloud topics L0 GC worker");
    should_shutdown_ = true;
    asrc_.request_abort();
    worker_cv_.signal();
    co_await delete_worker_->stop();
    co_await std::exchange(worker_, seastar::make_ready_future<>());
    vlog(cd_log.info, "Stopped cloud_topics L0 GC worker");
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
                    backoff = config_.throttle_progress();
                } else {
                    backoff = config_.throttle_no_progress();
                }
            } else {
                switch (res.error()) {
                case collection_error::service_error:
                case collection_error::invalid_object_name:
                case collection_error::no_collectible_epoch:
                    backoff = config_.throttle_no_progress();
                }
            }

        } catch (...) {
            vlog(
              cd_log.info,
              "Level zero GC restarting after error: {}",
              std::current_exception());
            backoff = config_.throttle_no_progress();
        }
    }

    vlog(cd_log.info, "Level zero GC worker is exiting");
}

seastar::future<std::expected<size_t, level_zero_gc::collection_error>>
level_zero_gc::try_to_collect() {
    // Ultra-temporary cache to avoid repeatedly querying for max gc-able epoch.
    // Since the result will always be valid clusterwide, compute exactly once
    // per collection loop.
    std::optional<cluster_epoch> max_gc_epoch;
    size_t total_eligible{0};
    while (delete_worker_->has_capacity()) {
        auto res = co_await do_try_to_collect(std::ref(max_gc_epoch));
        if (!res.has_value()) {
            co_return res;
        }
        if (res.value() == 0) {
            break;
        }
        total_eligible += res.value();
    }

    co_return total_eligible;
}

seastar::future<std::expected<size_t, level_zero_gc::collection_error>>
level_zero_gc::do_try_to_collect(std::optional<cluster_epoch>& max_gc_epoch) {
    auto candidate_objects = co_await delete_worker_->next_page();
    if (!candidate_objects.has_value()) {
        vlog(
          cd_log.debug,
          "Received error listing objects during L0 GC: {}",
          candidate_objects.error());
        co_return std::unexpected(collection_error::service_error);
    }

    if (!max_gc_epoch.has_value()) {
        const auto maybe_max_gc_epoch
          = co_await epoch_source_->max_gc_eligible_epoch(&asrc_);
        if (!maybe_max_gc_epoch.has_value()) {
            vlog(
              cd_log.debug,
              "Received error retrieving GC eligible epoch: {}",
              maybe_max_gc_epoch.error());
            co_return std::unexpected(collection_error::service_error);
        }
        max_gc_epoch = maybe_max_gc_epoch.value();
    }

    if (!max_gc_epoch.has_value()) {
        vlog(cd_log.info, "No GC eligible epoch currently exists");
        co_return std::unexpected(collection_error::no_collectible_epoch);
    }

    const auto max_gc_birthday = std::chrono::system_clock::now()
                                 - config_.deletion_grace_period();

    vlog(
      cd_log.debug,
      "Attempting L0 GC at epoch {} and last modified limit {}",
      max_gc_epoch.value(),
      max_gc_birthday);

    // objects that can be safely deleted
    chunked_vector<cloud_storage_clients::client::list_bucket_item>
      eligible_objects;
    // total size of eligible keys
    size_t object_keys_total_bytes = 0;

    // used to detect unsorted object listings
    seastar::sstring last_key;
    std::optional<cluster_epoch> last_epoch;
    object_id::prefix_t last_prefix{0};

    for (const auto& object : candidate_objects.value()) {
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

        const auto object_pfx = object_path_factory::level_zero_path_to_prefix(
          object.key);

        if (!object_pfx.has_value()) {
            vlog(
              cd_log.error,
              "Unable to parse prefix during L0 GC: {}",
              object_pfx.error());
            co_return std::unexpected(collection_error::invalid_object_name);
        }

        // detect non-lexicographic ordering. this may indicate that GC will
        // not operate efficiently with the underlying storage system. see
        // the class comment for more details about what this means in
        // practice.
        if (!last_epoch.has_value()) {
            last_key = object.key;
            last_epoch = object_epoch.value();
            last_prefix = object_pfx.value();
        }

        if (
          object_pfx.value() < last_prefix
          || (object_pfx.value() == last_prefix && object_epoch.value() < last_epoch)) {
            constexpr std::chrono::minutes rate_limit{1};
            static seastar::logger::rate_limit rate(rate_limit);
            vloglr(
              cd_log,
              seastar::log_level::error,
              rate,
              "Non-lexicographic object listing detected during L0 GC {} < "
              "{}",
              object.key,
              last_key);
        }

        last_key = object.key;
        last_epoch = object_epoch.value();
        last_prefix = object_pfx.value();

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

        object_keys_total_bytes += object.key.size();
        eligible_objects.push_back(object);
    }

    co_return delete_worker_->delete_objects(
      std::move(eligible_objects), object_keys_total_bytes);
}

std::optional<prefix_range_inclusive>
compute_prefix_range(size_t shard_idx, size_t total_shards) {
    auto total_prefixes = object_id::prefix_max + 1;
    total_shards = std::min(total_shards, static_cast<size_t>(total_prefixes));
    if (shard_idx >= total_shards) {
        return std::nullopt;
    }
    auto stride = total_prefixes / total_shards;
    auto min = static_cast<object_id::prefix_t>(shard_idx * stride);
    auto max = static_cast<object_id::prefix_t>(min + stride - 1);
    if (shard_idx == total_shards - 1) {
        max = object_id::prefix_max;
    }

    return prefix_range_inclusive{min, max};
}

} // namespace cloud_topics

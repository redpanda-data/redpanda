/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "datalake/datalake_manager.h"

#include "cluster/partition_manager.h"
#include "cluster/topic_table.h"
#include "cluster/types.h"
#include "config/configuration.h"
#include "config/node_config.h"
#include "datalake/backlog_controller.h"
#include "datalake/catalog_schema_manager.h"
#include "datalake/cloud_data_io.h"
#include "datalake/coordinator/catalog_factory.h"
#include "datalake/coordinator/frontend.h"
#include "datalake/logger.h"
#include "datalake/record_schema_resolver.h"
#include "datalake/record_translator.h"
#include "raft/group_manager.h"
#include "schema/registry.h"
#include "utils/directory_walker.h"
#include "utils/human.h"

#include <memory>
#include <ranges>

constexpr std::chrono::milliseconds translation_jitter{500};
constexpr std::chrono::milliseconds translation_jitter_base{5000};

namespace datalake {

namespace {

static std::unique_ptr<type_resolver> make_type_resolver(
  const model::iceberg_mode& mode,
  model::topic_view topic_name,
  schema::registry& sr,
  schema_cache& cache,
  resolved_type_cache& type_cache) {
    switch (mode.kind()) {
    case model::iceberg_mode::variant::disabled:
        vassert(
          false,
          "Cannot make record translator when iceberg is disabled, logic bug.");
    case model::iceberg_mode::variant::key_value:
        return std::make_unique<binary_type_resolver>();
    case model::iceberg_mode::variant::value_schema_id_prefix:
        return std::make_unique<record_schema_resolver>(sr, cache, type_cache);
    case model::iceberg_mode::variant::value_schema_latest:
        auto subject = pandaproxy::schema_registry::subject(
          fmt::format("{}-value", topic_name));
        if (auto explicit_subject = mode.subject_name()) {
            subject = pandaproxy::schema_registry::subject(*explicit_subject);
        }
        return std::make_unique<latest_subject_schema_resolver>(
          sr,
          subject,
          mode.protobuf_full_name(),
          config::shard_local_cfg().iceberg_latest_schema_cache_ttl_ms.bind(),
          cache,
          type_cache);
    }
}

static std::unique_ptr<record_translator>
make_record_translator(const model::iceberg_mode& mode) {
    switch (mode.kind()) {
    case model::iceberg_mode::variant::disabled:
        vassert(
          false,
          "Cannot make record translator when iceberg is disabled, logic bug.");
    case model::iceberg_mode::variant::key_value:
        return std::make_unique<key_value_translator>();
    case model::iceberg_mode::variant::value_schema_id_prefix:
    case model::iceberg_mode::variant::value_schema_latest:
        return std::make_unique<structured_data_translator>();
    }
}
} // namespace

/*
 * high-level design of space management
 *
 * Core 0 manages the total space reservation on the system. It is modeled as a
 * semaphore that records the unreserved space (aka free space), and is
 * initialized to the total allowable scratch space size. Each scheduler
 * also has a disk reservation which is initialized to 0.
 *
 * When a translator writes data to disk is attempts to acquire disk reservation
 * from its core-local scheduler. When the core-local scheduler does not have
 * any reservation available then it tries to acquire a block of reservation
 * from the core 0 manager.
 *
 * If core 0 has sufficient units to hand out to a scheduler, then the request
 * is granted immediately. Otherwise, no units are granted and the scheduler
 * will go into a polling loop until units become available.
 *
 * When core 0 unused space dips below the soft limit (e.g. 80% of the total)
 * then it initiates a process to bring utilization down below the soft limit
 * and free up space.
 *
 * It is a two step process. First, core 0 requests all cores to release their
 * unused disk reservation to core 0. If this brings down the usage below soft
 * limit then the process completes. Otherwise, core 0 requests that translators
 * with large reservations finish immedinately and release their units. The
 * process continues until usage falls below the soft limit.
 */
class core_0_disk_manager : public translation::scheduling::disk_manager {
public:
    static constexpr ss::shard_id manager_shard = 0;

    /*
     * ideally we could initialize with container() in the datalake manager
     * constructor. however, seastar doesn't set the underlying container()
     * pointer until after service instance is constructed. so we need to patch
     * the proxy pointer, and a convenient place is datalake_manager::start.
     */
    void set_manager_reference(ss::sharded<datalake_manager>& manager) {
        vassert(_manager == nullptr, "manager reference set twice");
        _manager = &manager;
    }

    ss::future<size_t> reserve() override {
        if (_manager) {
            const auto from = ss::this_shard_id();
            return _manager->invoke_on(
              manager_shard, [from](datalake_manager& manager) {
                  return manager.reserve_disk(from);
              });
        }
        vlog(datalake_log.debug, "Dropping disk reservation request");
        return ss::make_ready_future<size_t>(0);
    }

private:
    ss::sharded<datalake_manager>* _manager{nullptr};
};

datalake_manager::datalake_manager(
  model::node_id self,
  std::unique_ptr<cluster::partition_change_notifier> notifications,
  ss::sharded<cluster::partition_manager>* partition_mgr,
  ss::sharded<cluster::topic_table>* topic_table,
  ss::sharded<features::feature_table>* features,
  ss::sharded<coordinator::frontend>* frontend,
  ss::sharded<cloud_io::remote>* cloud_io,
  std::unique_ptr<coordinator::catalog_factory> catalog_factory,
  pandaproxy::schema_registry::api* sr_api,
  ss::sharded<ss::abort_source>* as,
  cloud_storage_clients::bucket_name bucket_name,
  ss::scheduling_group sg,
  size_t memory_limit)
  : _self(self)
  , _partition_notifications(std::move(notifications))
  , _partition_mgr(partition_mgr)
  , _topic_table(topic_table)
  , _features(features)
  , _coordinator_frontend(frontend)
  , _cloud_data_io(
      std::make_unique<cloud_data_io>(cloud_io->local(), bucket_name))
  , _location_provider(cloud_io->local().provider(), bucket_name)
  , _schema_registry(schema::registry::make_default(sr_api))
  , _catalog_factory(std::move(catalog_factory))
  // TODO: The two following cache configs were arbitrarily choosen. Figure out
  // a more reasoned size and allocate a share of the datalake memory semaphore
  // to this cache.
  , _schema_cache(
      std::make_unique<chunked_schema_cache>(
        chunked_schema_cache::cache_t::config{
          .cache_size = 50, .small_size = 10}))
  , _resolved_type_cache(
      std::make_unique<chunked_resolved_type_cache>(
        chunked_resolved_type_cache::cache_t::config{
          .cache_size = 50, .small_size = 10}))
  , _as(as)
  , _sg(sg)
  , _iceberg_invalid_record_action(
      config::shard_local_cfg().iceberg_invalid_record_action.bind())
  , _writer_scratch_space(config::node().datalake_staging_path())
  , _disk_manager(std::make_unique<core_0_disk_manager>())
  , _scheduler(
      memory_limit,
      config::shard_local_cfg().datalake_scheduler_block_size_bytes(),
      translation::scheduling::scheduling_policy::make_default(
        config::shard_local_cfg()
          .datalake_scheduler_max_concurrent_translations.bind(),
        std::chrono::duration_cast<translation::scheduling::clock::duration>(
          config::shard_local_cfg().datalake_scheduler_time_slice_ms())),
      *_disk_manager)
  , _queue(
      sg,
      [](const std::exception_ptr& ex) {
          vlog(
            datalake_log.error,
            "unexpected error in managing translator: {}",
            ex);
      })
  , _core0_disk_bytes_reservable{0, "datalake::core0_disk_bytes_reservable"}
  , _disk_space_manager_enable(
      config::shard_local_cfg().datalake_disk_space_monitor_enable.bind())
  , _scratch_space_size_bytes(
      config::shard_local_cfg().datalake_scratch_space_size_bytes.bind())
  , _scratch_space_soft_limit_size_percent(
      config::shard_local_cfg()
        .datalake_scratch_space_soft_limit_size_percent.bind()) {}
datalake_manager::~datalake_manager() = default;

size_t datalake_manager::total_translation_backlog() const {
    size_t total_backlog = 0;
    for (const auto& [_, translator] : _scheduler.all_translators()) {
        total_backlog += translator.status().translation_backlog.value_or(0);
    }
    return total_backlog;
}

ss::lw_shared_ptr<class translation_probe>
datalake_manager::get_or_create_probe(const model::ntp& ntp) {
    auto [it, inserted] = _translation_probe_by_ntp.try_emplace(ntp, nullptr);
    if (inserted) {
        it->second = ss::make_lw_shared<class translation_probe>(ntp);
    }
    return it->second;
}

ss::future<> datalake_manager::start() {
    _disk_manager->set_manager_reference(container());
    _catalog = co_await _catalog_factory->create_catalog(_as->local());
    _schema_mgr = std::make_unique<catalog_schema_manager>(
      *_catalog, &_features->local());
    // partition managed notification, this is particularly
    // relevant for cross core movements without a term change.
    _partition_notifications_id
      = _partition_notifications->register_partition_notifications(
        [this](
          cluster::partition_change_notifier::notification_type,
          const model::ntp& ntp,
          std::optional<cluster::partition_change_notifier::partition_state>
            partition) {
            _queue.submit(
              [this, ntp = ntp, partition = std::move(partition)]() mutable {
                  return handle_translator_state_change(
                    std::move(ntp), std::move(partition));
              });
        });
    _iceberg_invalid_record_action.watch([this] {
        for (auto& [ntp, _] : _scheduler.all_translators()) {
            auto partition = _partition_mgr->local().get(ntp);
            if (!partition) {
                continue;
            }
            cluster::partition_change_notifier::partition_state pstate = {
              partition->term(),
              partition->is_leader(),
              _topic_table->local().get_topic_cfg(
                model::topic_namespace_view{ntp})};
            _queue.submit(
              [this, ntp = ntp, pstate = std::move(pstate)]() mutable {
                  return handle_translator_state_change(
                    std::move(ntp), std::move(pstate));
              });
        }
    });

    _schema_cache->start();
    _resolved_type_cache->start();
    _backlog_controller = std::make_unique<backlog_controller>(
      [this] { return total_translation_backlog(); }, _sg);
    co_await _backlog_controller->start();

    /*
     * Start the global disk space usage monitor loop
     */
    if (ss::this_shard_id() == core_0_disk_manager::manager_shard) {
        // setup config bindings to trigger reconfiguration
        _disk_space_manager_enable.watch([this] { update_disk_limits(); });
        _scratch_space_size_bytes.watch([this] { update_disk_limits(); });
        _scratch_space_soft_limit_size_percent.watch(
          [this] { update_disk_limits(); });

        // initialize
        update_disk_limits();

        ssx::spawn_with_gate(_gate, [this] { return disk_space_monitor(); });
    }
}

ss::future<> datalake_manager::disk_space_monitor() {
    while (!_gate.is_closed()) {
        co_await _disk_space_monitor_cv.wait([this] {
            return config::shard_local_cfg()
                     .datalake_disk_space_monitor_enable()
                   && disk_space_soft_limit_exceeded();
        });

        try {
            co_await check_and_manage_disk_space();
        } catch (...) {
            vlog(
              datalake_log.info,
              "Recoverable error checking datalake disk space: {}",
              std::current_exception());
        }

        /*
         * even when we the system is driven to high disk space utilization
         * frequently, we don't want to go too hard with the reclaim loop.
         */
        co_await ss::sleep_abortable(1s, _as->local());
    }
}

ss::future<> datalake_manager::check_and_manage_disk_space() {
    using translator_id = translation::scheduling::translator_id;
    using translator_info = std::pair<ss::shard_id, translator_id>;
    using index_type = absl::btree_multimap<size_t, translator_info>;

    /*
     * it may be the case that there is plenty of free space, but each scheduler
     * is holding on to unused units. so the first step is to go harvest excess
     * units and then take additional action if we are really out of space.
     */
    auto excess_units = co_await container().map_reduce0(
      [](datalake_manager& mgr) {
          return mgr._scheduler.release_unused_disk_units();
      },
      size_t{0},
      [](size_t acc, size_t units) { return acc + units; });

    // if schedulers miss their disk units, they'll come say hi
    if (excess_units > 0) {
        _core0_disk_bytes_reservable.signal(excess_units);
    }

    vlog(
      datalake_log.debug,
      "Collected {} unused disk reservation units, remaining {}",
      human::bytes(excess_units),
      human::bytes(_core0_disk_bytes_reservable.current()));

    // low disk problem solved?
    if (!disk_space_soft_limit_exceeded()) {
        co_return;
    }

    /*
     * Collect disk usage from all translators managed by the scheduler, and
     * combine these usages from across all cores to create a global set of
     * translators ordered by their disk usage.
     */
    auto usage = co_await container().map_reduce0(
      [](datalake_manager& mgr) {
          index_type usage;
          for (const auto& it : mgr._scheduler.all_translators()) {
              auto status = it.second.status();
              auto size = status.disk_bytes_flushed.value_or(0)
                          + status.memory_bytes_reserved.value_or(0);
              usage.emplace(
                size, std::make_tuple(ss::this_shard_id(), it.first));
          }
          return usage;
      },
      index_type{},
      [](index_type acc, index_type usage) {
          acc.merge(usage);
          return acc;
      });

    const auto total_bytes = std::reduce(
      usage.begin(),
      usage.end(),
      size_t(0),
      [](const auto acc, const auto& elem) { return acc + elem.first; });

    // check again after scheduling point
    if (!disk_space_soft_limit_exceeded()) {
        co_return;
    }

    // amount to free to get back to the soft limit
    const auto soft_limit_excess = disk_space_soft_limit()
                                   - _core0_disk_bytes_reservable.current();

    /*
     * do nothing if we are over the limit, but only by a "small" amount, which
     * increases the chances of having meaningful work to do and avoid some
     * thrashing scenarios. this is the same strategy used in space management
     * to avoid thrashing (see resource_mgm/storage.cc).
     */
    const size_t min_size_threshold = 64_MiB;
    if (soft_limit_excess <= min_size_threshold) {
        vlog(
          datalake_log.trace,
          "Disk monitor total {} available {} soft limit {} excess {}",
          human::bytes(total_bytes),
          human::bytes(_core0_disk_bytes_reservable.current()),
          human::bytes(disk_space_soft_limit()),
          human::bytes(soft_limit_excess));
        co_return;
    }

    const double coeff
      = config::shard_local_cfg().datalake_disk_usage_overage_coeff();

    const auto adjusted_target_excess = static_cast<size_t>(
      soft_limit_excess * coeff);

    /*
     * Generate a schedule of translators that should be finished immediately so
     * that their on disk data is uploaded and deleted locally. Iteration is
     * from largest usage to smallest usage, and that order is preserved in the
     * per-core vector constructed for `scheduler::request_immediate_finish`.
     */
    size_t num_translators = 0;
    size_t schedule_total_bytes = 0;
    absl::flat_hash_map<
      ss::shard_id,
      chunked_vector<translation::scheduling::scheduler::finish_request>>
      schedule;
    for (auto& it : std::ranges::reverse_view(usage)) {
        if (schedule_total_bytes >= adjusted_target_excess) {
            break;
        }
        // it.first is the data usage by the translator. if the scheduling
        // policy can make use of it, then it coudl be passed in here to avoid
        // recalulation of the same value.
        schedule[it.second.first].emplace_back(
          it.second.second,
          translation::scheduling::translator::stop_reason::out_of_disk);
        schedule_total_bytes += it.first;
        num_translators++;
    }

    vlog(
      datalake_log.debug,
      "Requesting {} translators to reclaim {}. Current total {} soft limit {} "
      "excess {} adjusted {}",
      num_translators,
      human::bytes(schedule_total_bytes),
      human::bytes(total_bytes),
      human::bytes(disk_space_soft_limit()),
      human::bytes(soft_limit_excess),
      human::bytes(adjusted_target_excess));

    /*
     * Make the request to each core with translators in the schedule.
     */
    co_await ss::parallel_for_each(
      schedule.begin(), schedule.end(), [this](auto& it) {
          return container().invoke_on(
            it.first,
            [translators = std::move(it.second)](
              datalake_manager& mgr) mutable {
                mgr._scheduler.request_immediate_finish(std::move(translators));
            });
      });
}

size_t datalake_manager::disk_space_soft_limit() {
    return _disk_bytes_reservable_soft_limit;
}

bool datalake_manager::disk_space_soft_limit_exceeded() {
    // we use the available_units semaphore interface which is adjusted to
    // reflect any deficits (cores owe us units), and might be negative.
    const auto used = static_cast<ssize_t>(_disk_bytes_reservable_total)
                      - _core0_disk_bytes_reservable.available_units();
    return used > static_cast<ssize_t>(disk_space_soft_limit());
}

ss::future<size_t> datalake_manager::reserve_disk(ss::shard_id shard) {
    /*
     * figure out how many units we'll return to the caller. if after consuming
     * these units we've exceeded the configured soft limit then kick the disk
     * space monitor which will attempt to bring usage back down below the soft
     * limit.
     */
    const auto units = std::min(
      _core0_disk_bytes_reservable.current(),
      config::shard_local_cfg()
        .datalake_scheduler_disk_reservation_block_size());
    if (units > 0) {
        _core0_disk_bytes_reservable.consume(units);
    }

    if (disk_space_soft_limit_exceeded()) {
        _disk_space_monitor_cv.signal();
    }

    vlog(
      datalake_log.debug,
      "Allocated {} disk reservation for shard {} from global pool. Total "
      "available {}",
      human::bytes(units),
      shard,
      human::bytes(_core0_disk_bytes_reservable.current()));

    co_return units;
}

ss::future<>
datalake_manager::prepare_staging_directory(std::filesystem::path path) {
    try {
        co_await ss::make_directory(path.string());
    } catch (const std::filesystem::filesystem_error& e) {
        if (e.code() != std::errc::file_exists) {
            vlog(
              datalake_log.error,
              "Could not create datalake staging directory: {}: {}",
              config::node().datalake_staging_path(),
              e);
            throw;
        }
    }

    chunked_vector<std::filesystem::path> files;
    co_await directory_walker::walk(
      path.string(), [&files, path](const ss::directory_entry& de) {
          if (de.type == ss::directory_entry_type::regular) {
              files.push_back(path / std::filesystem::path(de.name));
          }
          return ss::now();
      });

    uint64_t total = 0;
    co_await ss::max_concurrent_for_each(
      files.begin(),
      files.end(),
      config::shard_local_cfg().space_management_max_log_concurrency(),
      [&total](const std::filesystem::path& path) {
          return ss::file_size(path.string())
            .then([&total](uint64_t size) { total += size; })
            .finally([path] { return ss::remove_file(path.string()); })
            .handle_exception([path](std::exception_ptr e) {
                vlog(
                  datalake_log.warn,
                  "Error clearing datalake staging file {}: {}",
                  path,
                  e);
            });
      });

    if (total) {
        vlog(
          datalake_log.info,
          "Cleared datalake staging directory: {}",
          human::bytes(total));
    }
}

ss::future<> datalake_manager::shutdown() {
    vlog(datalake_log.debug, "Stopping datalake manager...");
    _disk_space_monitor_cv.broken();
    auto f = _gate.close();
    co_await _queue.shutdown();
    if (_backlog_controller) {
        co_await _backlog_controller->stop();
    }
    if (_schema_mgr) {
        co_await _schema_mgr->stop();
    }
    if (_catalog) {
        co_await _catalog->stop();
    }
    _partition_notifications->unregister_partition_notifications(
      _partition_notifications_id);
    co_await _scheduler.stop();
    co_await std::move(f);
    _schema_cache->stop();
    _resolved_type_cache->stop();
    vlog(datalake_log.debug, "Stopped datalake manager...");
}

ss::future<> datalake_manager::handle_translator_state_change(
  model::ntp ntp,
  std::optional<cluster::partition_change_notifier::partition_state>
    partition) {
    if (_gate.is_closed() || !model::is_user_topic(ntp)) {
        co_return;
    }
    vlog(datalake_log.debug, "Translator state change for {} detected.", ntp);
    const auto& translators = _scheduler.all_translators();
    auto translator_it = translators.find(ntp);
    auto translator_exists = translator_it != translators.end();
    if (translator_exists) {
        // stop the existing translator first and restart if needed
        // The newer incarnation will pickup any changes to the iceberg
        // mode or properties.
        // This is ok because the changes to properties are very rare.
        auto remove_f = co_await ss::coroutine::as_future(
          _scheduler.remove_translator(ntp));
        if (remove_f.failed()) {
            auto ex = remove_f.get_exception();
            vlog(
              datalake_log.warn,
              "removing existing translator for {} failed {}, retrying in 10s.",
              ntp,
              ex);
            if (!_gate.is_closed()) {
                _queue.submit_delayed(
                  10s,
                  [this,
                   ntp = std::move(ntp),
                   partition = std::move(partition)]() mutable {
                      return handle_translator_state_change(
                        std::move(ntp), std::move(partition));
                  });
            }
            co_return;
        }
    }

    auto requires_active_translator
      = partition               // valid partition on the shard
        && partition->is_leader // currently the leader
        // iceberg is enabled in the topic configuration
        && partition->topic_cfg
        && partition->topic_cfg->properties.iceberg_mode
             != model::iceberg_mode::disabled;

    if (!requires_active_translator) {
        // no active translator needed, so nothing to do
        co_return;
    }

    // otherwise we need to set up a translator

    auto mode = partition->topic_cfg->properties.iceberg_mode;
    auto type_resolver = make_type_resolver(
      mode,
      ntp.tp.topic,
      *_schema_registry,
      *_schema_cache,
      *_resolved_type_cache);
    auto record_translator = make_record_translator(mode);
    auto table_creator = translation::make_default_table_creator(
      _coordinator_frontend->local());

    auto& reservations = _scheduler.reservations();
    //  make a new translator
    auto coordinator
      = translation::coordinator_api::make_default_coordinator_api(
        _coordinator_frontend->local());

    auto partition_ptr = _partition_mgr->local().get(ntp);
    if (!partition_ptr) {
        co_return;
    }
    auto data_src = translation::data_source::make_default_data_source(
      partition_ptr);
    auto translation_ctx
      = translation::translation_context::make_default_translation_context(
        local_path{_writer_scratch_space},
        data_src->ntp(),
        data_src->topic_revision(),
        *_cloud_data_io,
        *_schema_mgr,
        std::move(type_resolver),
        std::move(record_translator),
        std::move(table_creator),
        _location_provider,
        *reservations,
        _topic_table,
        _features,
        get_or_create_probe(ntp));
    auto lag_tracker
      = translation::translation_lag_tracker::make_default_lag_tracker(
        partition_ptr, _topic_table->local());

    auto translator = std::make_unique<translation::partition_translator>(
      _sg,
      std::move(coordinator),
      std::move(data_src),
      std::move(translation_ctx),
      std::move(lag_tracker),
      simple_time_jitter<ss::lowres_clock, std::chrono::milliseconds>{
        translation_jitter_base, translation_jitter});

    auto add_f = co_await ss::coroutine::as_future(
      _scheduler.add_translator(std::move(translator)));

    if (add_f.failed() || !add_f.get()) {
        add_f.ignore_ready_future();
        vlog(
          datalake_log.warn,
          "adding translator for {} failed, retrying in a bit",
          ntp);
        if (!_gate.is_closed()) {
            _queue.submit_delayed(
              10s,
              [this,
               ntp = std::move(ntp),
               partition = std::move(partition)]() mutable {
                  return handle_translator_state_change(
                    std::move(ntp), std::move(partition));
              });
            co_return;
        }
    }
}

ss::future<uint64_t> datalake_manager::disk_usage() {
    const auto path = config::node().datalake_staging_path();

    if (!co_await ss::file_exists(path.string())) {
        co_return 0;
    }

    chunked_vector<std::filesystem::path> files;
    co_await directory_walker::walk(
      path.string(), [&files, path](const ss::directory_entry& de) {
          if (de.type == ss::directory_entry_type::regular) {
              files.push_back(path / std::filesystem::path(de.name));
          }
          return ss::now();
      });

    uint64_t total = 0;
    co_await ss::max_concurrent_for_each(
      files.begin(),
      files.end(),
      config::shard_local_cfg().space_management_max_log_concurrency(),
      [&total](const std::filesystem::path& path) {
          return ss::file_size(path.string())
            .then([&total](uint64_t size) { total += size; })
            .handle_exception_type(
              [path](const std::filesystem::filesystem_error& e) {
                  if (e.code() == std::errc::no_such_file_or_directory) {
                      vlog(
                        datalake_log.debug,
                        "Stat failed for path: {}: {}",
                        path,
                        e.code());
                  }
                  return ss::make_exception_future<>(e);
              })
            .handle_exception([path](std::exception_ptr eptr) {
                vlog(
                  datalake_log.warn,
                  "Stat failed for path: {}: {}",
                  path,
                  eptr);
            });
      });

    co_return total;
}

bool datalake_manager::max_shares_assigned() const {
    return _backlog_controller->max_shares_assigned();
}

size_t datalake_manager::overdue_translation_partition_count() const {
    auto now = translation::scheduling::clock::now();
    return std::ranges::count_if(
      _scheduler.all_translators(), [now](const auto& entry) {
          return entry.second.status().next_checkpoint_deadline < now;
      });
}
/**
 * Returns count of partitions that translation is blocked. This value
 * should be 0 in normal conditions.
 */
size_t datalake_manager::partitions_with_translation_blocked() const {
    // TODO: Return blocked if partition wasn't translated for a long time f.e.
    // moret
    return 0;
}

void datalake_manager::update_disk_limits() {
    // the requests
    auto new_total_size = _scratch_space_size_bytes();
    if (!_disk_space_manager_enable()) {
        // when we disable the manager we want to effectively stop enforcing
        // limits. so make the total allowable size massive, but not so big that
        // we need to worry about any kind of integer overflow.
        new_total_size = 50_TiB;
    }

    const auto new_soft_percent = _scratch_space_soft_limit_size_percent();

    // current settings
    const auto prev_total = _disk_bytes_reservable_total;
    const auto prev_soft_limit = _disk_bytes_reservable_soft_limit;
    const auto prev_available = _core0_disk_bytes_reservable.available_units();

    _disk_bytes_reservable_soft_limit = static_cast<size_t>(
      static_cast<double>(new_total_size) * (new_soft_percent / 100.0));

    if (new_total_size > _disk_bytes_reservable_total) {
        auto units = new_total_size - _disk_bytes_reservable_total;
        _disk_bytes_reservable_total = new_total_size;
        _core0_disk_bytes_reservable.signal(units);

    } else if (new_total_size < _disk_bytes_reservable_total) {
        auto units = _disk_bytes_reservable_total - new_total_size;
        _disk_bytes_reservable_total = new_total_size;
        _core0_disk_bytes_reservable.consume(units);
    }

    /*
     * let the monitor see if anything needs to be done based on the changes
     */
    _disk_space_monitor_cv.signal();

    /*
     * when shrinking the size of the scratch space the semaphore tracking
     * available units on core 0 may become negative because all the units are
     * currently handed out to other cores.
     */
    auto format_reservable = [](auto v) {
        if (v >= 0) {
            return fmt::format("{}", human::bytes(v));
        }
        return fmt::format("{} ({})", human::bytes(0), v);
    };

    vlog(
      datalake_log.info,
      "Setting scratch space total {} soft {} available {} prev ({}, {}, {})",
      human::bytes(_disk_bytes_reservable_total),
      human::bytes(_disk_bytes_reservable_soft_limit),
      format_reservable(_core0_disk_bytes_reservable.available_units()),
      human::bytes(prev_total),
      human::bytes(prev_soft_limit),
      format_reservable(prev_available));
}

} // namespace datalake

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

#include <memory>

// TODO(iceberg): Make me configurable
static constexpr auto scheduler_block_size_default = 4_MiB;
static constexpr auto scheduler_concurrent_translations = 4;
static constexpr auto scheduler_time_slice = 30s;
static constexpr std::string_view iceberg_data_path_prefix = "data";

namespace datalake {

namespace {

static std::unique_ptr<type_resolver> make_type_resolver(
  model::iceberg_mode mode, schema::registry& sr, schema_cache& cache) {
    switch (mode) {
    case model::iceberg_mode::disabled:
        vassert(
          false,
          "Cannot make record translator when iceberg is disabled, logic bug.");
    case model::iceberg_mode::key_value:
        return std::make_unique<binary_type_resolver>();
    case model::iceberg_mode::value_schema_id_prefix:
        return std::make_unique<record_schema_resolver>(sr, cache);
    }
}

static std::unique_ptr<record_translator>
make_record_translator(model::iceberg_mode mode) {
    switch (mode) {
    case model::iceberg_mode::disabled:
        vassert(
          false,
          "Cannot make record translator when iceberg is disabled, logic bug.");
    case model::iceberg_mode::key_value:
        return std::make_unique<key_value_translator>();
    case model::iceberg_mode::value_schema_id_prefix:
        return std::make_unique<structured_data_translator>();
    }
}
} // namespace

datalake_manager::datalake_manager(
  model::node_id self,
  ss::sharded<raft::group_manager>* group_mgr,
  ss::sharded<cluster::partition_manager>* partition_mgr,
  ss::sharded<cluster::topic_table>* topic_table,
  ss::sharded<cluster::topics_frontend>* topics_frontend,
  ss::sharded<cluster::partition_leaders_table>* leaders,
  ss::sharded<cluster::shard_table>* shards,
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
  , _group_mgr(group_mgr)
  , _partition_mgr(partition_mgr)
  , _topic_table(topic_table)
  , _topics_frontend(topics_frontend)
  , _leaders(leaders)
  , _shards(shards)
  , _features(features)
  , _coordinator_frontend(frontend)
  , _cloud_data_io(
      std::make_unique<cloud_data_io>(cloud_io->local(), bucket_name))
  , _location_provider(cloud_io->local().provider(), bucket_name)
  , _schema_registry(schema::registry::make_default(sr_api))
  , _catalog_factory(std::move(catalog_factory))
  , _type_resolver(std::make_unique<record_schema_resolver>(*_schema_registry))
  // TODO: The cache size is currently arbitrary. Figure out a more reasoned
  // size and allocate a share of the datalake memory semaphore to this cache.
  , _schema_cache(std::make_unique<chunked_schema_cache>(
      chunked_schema_cache::cache_t::config{
        .cache_size = 50, .small_size = 10}))
  , _as(as)
  , _sg(sg)
  , _iceberg_invalid_record_action(
      config::shard_local_cfg().iceberg_invalid_record_action.bind())
  , _writer_scratch_space(config::node().datalake_staging_path())
  , _scheduler(
      memory_limit,
      scheduler_block_size_default,
      translation::scheduling::scheduling_policy::make_default(
        scheduler_concurrent_translations,
        std::chrono::duration_cast<translation::scheduling::clock::duration>(
          scheduler_time_slice)))
  , _queue(sg, [](const std::exception_ptr& ex) {
      vlog(
        datalake_log.error, "unexpected error in managing translator: {}", ex);
  }) {}
datalake_manager::~datalake_manager() = default;

double datalake_manager::average_translation_backlog() {
    size_t total_lag = 0;
    size_t translators_with_backlog = 0;
    const auto& translators = _scheduler.all_translators();
    for (const auto& [_, translator] : translators) {
        auto backlog_size = translator.status().translation_backlog;
        // skip over translators that are not yet ready to report anything
        if (!backlog_size) {
            continue;
        }
        total_lag += backlog_size.value();
        translators_with_backlog++;
    }

    if (translators_with_backlog == 0) {
        return 0;
    }

    return total_lag / translators_with_backlog;
}

ss::future<> datalake_manager::start() {
    /*
     * Ensure that datalake scratch space directory exists. This is run on each
     * core (as opposed to only on core-0) because shard initialization happens
     * in parallel, but the race is handled by ignoring EEXIST.
     */
    try {
        co_await ss::make_directory(
          config::node().datalake_staging_path().string());
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

    _catalog = co_await _catalog_factory->create_catalog();
    _schema_mgr = std::make_unique<catalog_schema_manager>(*_catalog);
    // partition managed notification, this is particularly
    // relevant for cross core movements without a term change.
    auto partition_managed_notification
      = _partition_mgr->local().register_manage_notification(
        model::kafka_namespace,
        [this](ss::lw_shared_ptr<cluster::partition> new_partition) {
            _queue.submit([this, ntp = new_partition->ntp()]() {
                return handle_translator_state_change(ntp);
            });
        });
    auto partition_unmanaged_notification
      = _partition_mgr->local().register_unmanage_notification(
        model::kafka_namespace, [this](model::topic_partition_view tp) {
            model::ntp ntp{model::kafka_namespace, tp.topic, tp.partition};
            _queue.submit([this, ntp = std::move(ntp)]() {
                return handle_translator_state_change(ntp);
            });
        });
    // Handle leadership changes
    auto leadership_registration
      = _group_mgr->local().register_leadership_notification(
        [this](
          raft::group_id group,
          ::model::term_id,
          std::optional<::model::node_id>) {
            auto partition = _partition_mgr->local().partition_for(group);
            if (partition) {
                _queue.submit([this, ntp = partition->ntp()]() {
                    return handle_translator_state_change(ntp);
                });
            }
        });

    // Handle topic properties changes (iceberg_mode,
    // iceberg_invalid_record_action)
    auto topic_properties_registration
      = _topic_table->local().register_ntp_delta_notification(
        [this](cluster::topic_table::ntp_delta_range_t range) {
            for (auto& entry : range) {
                if (
                  entry.type
                  == cluster::topic_table_ntp_delta_type::properties_updated) {
                    _queue.submit([this, ntp = entry.ntp]() {
                        return handle_translator_state_change(ntp);
                    });
                }
            }
        });

    _deregistrations.reserve(4);
    _deregistrations.emplace_back([this, partition_managed_notification] {
        _partition_mgr->local().unregister_manage_notification(
          partition_managed_notification);
    });
    _deregistrations.emplace_back([this, partition_unmanaged_notification] {
        _partition_mgr->local().unregister_unmanage_notification(
          partition_unmanaged_notification);
    });
    _deregistrations.emplace_back([this, leadership_registration] {
        _group_mgr->local().unregister_leadership_notification(
          leadership_registration);
    });
    _deregistrations.emplace_back([this, topic_properties_registration] {
        _topic_table->local().unregister_ntp_delta_notification(
          topic_properties_registration);
    });
    _iceberg_invalid_record_action.watch([this] {
        for (auto& [_, entry] : _scheduler.all_translators()) {
            entry.translator_ptr()->reconcile_properties();
        }
    });

    if (!_features->local().is_active(features::feature::datalake_iceberg_ga)) {
        ssx::spawn_with_gate(_gate, [this] {
            return _features->local()
              .await_feature(
                features::feature::datalake_iceberg_ga, _as->local())
              .then([this] {
                  for (const auto& [ntp, _] : _scheduler.all_translators()) {
                      _queue.submit([this, ntp]() {
                          return handle_translator_state_change(ntp);
                      });
                  }
              });
        });
    }

    _schema_cache->start();
    _backlog_controller = std::make_unique<backlog_controller>(
      [this] { return average_translation_backlog(); }, _sg);
    co_await _backlog_controller->start();
}

ss::future<> datalake_manager::stop() {
    auto f = _gate.close();
    co_await _queue.shutdown();
    co_await _backlog_controller->stop();
    _deregistrations.clear();
    co_await _scheduler.stop();
    co_await std::move(f);
    _schema_cache->stop();
}

ss::future<>
datalake_manager::handle_translator_state_change(const model::ntp& ntp) {
    if (_gate.is_closed() || !model::is_user_topic(ntp)) {
        co_return;
    }
    auto partition = _partition_mgr->local().get(ntp);
    auto is_leader = partition && partition->raft()->is_leader();
    const auto& topic_cfg = _topic_table->local().get_topic_cfg(
      model::topic_namespace_view{ntp});
    const auto& translators = _scheduler.all_translators();
    auto translator_it = translators.find(ntp);
    auto translator_exists = translator_it != translators.end();
    auto iceberg_disabled = topic_cfg
                            && topic_cfg->properties.iceberg_mode
                                 == model::iceberg_mode::disabled;
    auto requires_active_translator = partition && topic_cfg
                                      && !iceberg_disabled && is_leader;
    if (translator_exists && !requires_active_translator) {
        co_await _scheduler.remove_translator(ntp);
    } else if (!translator_exists && requires_active_translator) {
        auto mode = topic_cfg->properties.iceberg_mode;
        auto type_resolver = make_type_resolver(
          mode, *_schema_registry, *_schema_cache);
        auto record_translator = make_record_translator(mode);
        auto table_creator = translation::make_default_table_creator(
          _coordinator_frontend->local());
        auto term = partition->term();
        auto path = remote_path{
          fmt::format("{}/{}/{}", iceberg_data_path_prefix, ntp.path(), term)};
        auto& reservations = _scheduler.reservations();
        //  make a new translator
        auto coordinator
          = translation::coordinator_api::make_default_coordinator_api(
            _coordinator_frontend->local());
        auto data_src = translation::data_source::make_default_data_source(
          partition);
        auto translation_ctx
          = translation::translation_context::make_default_translation_context(
            local_path{_writer_scratch_space},
            partition->ntp(),
            partition->get_revision_id(),
            *_cloud_data_io,
            *_schema_mgr,
            std::move(type_resolver),
            std::move(record_translator),
            std::move(table_creator),
            _location_provider,
            std::move(path),
            *reservations,
            _topic_table,
            _features);

        auto translator = std::make_unique<translation::partition_translator>(
          _sg,
          std::move(coordinator),
          std::move(data_src),
          std::move(translation_ctx));

        auto add_f = co_await ss::coroutine::as_future(
          _scheduler.add_translator(std::move(translator)));

        if (add_f.failed() || !add_f.get()) {
            add_f.ignore_ready_future();
            vlog(
              datalake_log.warn, "adding translator failed, retrying in a bit");
            if (!_gate.is_closed()) {
                _queue.submit_delayed(10s, [this, ntp]() {
                    return handle_translator_state_change(ntp);
                });
            }
        }
    } else if (translator_exists) {
        // TODO: add more tests that exercise this code path (to ensure new
        // property updates are getting picked up correctly)
        translator_it->second.translator_ptr()->reconcile_properties();
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

} // namespace datalake

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
#include "datalake/catalog_schema_manager.h"
#include "datalake/cloud_data_io.h"
#include "datalake/coordinator/catalog_factory.h"
#include "datalake/coordinator/frontend.h"
#include "datalake/logger.h"
#include "datalake/record_schema_resolver.h"
#include "datalake/record_translator.h"
#include "raft/group_manager.h"
#include "schema/registry.h"

#include <memory>

namespace datalake {

namespace {

static std::unique_ptr<type_resolver>
make_type_resolver(model::iceberg_mode mode, schema::registry& sr) {
    switch (mode) {
    case model::iceberg_mode::disabled:
        vassert(
          false,
          "Cannot make record translator when iceberg is disabled, logic bug.");
    case model::iceberg_mode::key_value:
        return std::make_unique<binary_type_resolver>();
    case model::iceberg_mode::value_schema_id_prefix:
        return std::make_unique<record_schema_resolver>(sr);
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
  , _cloud_data_io(std::make_unique<cloud_data_io>(
      cloud_io->local(), std::move(bucket_name)))
  , _schema_registry(schema::registry::make_default(sr_api))
  , _catalog_factory(std::move(catalog_factory))
  , _type_resolver(std::make_unique<record_schema_resolver>(*_schema_registry))
  , _as(as)
  , _sg(sg)
  , _effective_max_translator_buffered_data(
      std::min(memory_limit, max_translator_buffered_data))
  , _parallel_translations(std::make_unique<ssx::semaphore>(
      size_t(
        std::floor(memory_limit / _effective_max_translator_buffered_data)),
      "datalake_parallel_translations"))
  , _iceberg_commit_interval(
      config::shard_local_cfg().iceberg_catalog_commit_interval_ms.bind()) {}
datalake_manager::~datalake_manager() = default;

ss::future<> datalake_manager::start() {
    _catalog = co_await _catalog_factory->create_catalog();
    _schema_mgr = std::make_unique<catalog_schema_manager>(*_catalog);
    // partition managed notification, this is particularly
    // relevant for cross core movements without a term change.
    auto partition_managed_notification
      = _partition_mgr->local().register_manage_notification(
        model::kafka_namespace,
        [this](ss::lw_shared_ptr<cluster::partition> new_partition) {
            on_group_notification(new_partition->ntp());
        });
    auto partition_unmanaged_notification
      = _partition_mgr->local().register_unmanage_notification(
        model::kafka_namespace, [this](model::topic_partition_view tp) {
            model::ntp ntp{model::kafka_namespace, tp.topic, tp.partition};
            ssx::spawn_with_gate(_gate, [this, ntp = std::move(ntp)] {
                return stop_translator(ntp);
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
                on_group_notification(partition->ntp());
            }
        });

    // Handle topic properties changes (iceberg_mode)
    auto topic_properties_registration
      = _topic_table->local().register_ntp_delta_notification(
        [this](cluster::topic_table::ntp_delta_range_t range) {
            for (auto& entry : range) {
                if (
                  entry.type
                  == cluster::topic_table_ntp_delta_type::properties_updated) {
                    on_group_notification(entry.ntp);
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
    _iceberg_commit_interval.watch([this] {
        ssx::spawn_with_gate(_gate, [this]() {
            for (const auto& [group, _] : _translators) {
                on_group_notification(group);
            }
        });
    });
}

ss::future<> datalake_manager::stop() {
    auto f = _gate.close();
    _deregistrations.clear();
    co_await ss::max_concurrent_for_each(
      _translators, 32, [](auto& entry) mutable {
          return entry.second->stop();
      });
    co_await std::move(f);
}

std::chrono::milliseconds datalake_manager::translation_interval_ms() const {
    // This aims to have multiple translations within a single commit interval
    // window. A minimum interval is in place to disallow frequent translations
    // and hence tiny parquet files. This is generally optimized for higher
    // throughputs that accumulate enough data within a commit interval window.
    static constexpr std::chrono::milliseconds min_translation_interval{5s};
    return std::max(min_translation_interval, _iceberg_commit_interval() / 3);
}

void datalake_manager::on_group_notification(const model::ntp& ntp) {
    auto partition = _partition_mgr->local().get(ntp);
    if (!partition || !model::is_user_topic(ntp)) {
        return;
    }
    const auto& topic_cfg = _topic_table->local().get_topic_cfg(
      model::topic_namespace_view{ntp});
    if (!topic_cfg) {
        return;
    }
    auto it = _translators.find(ntp);
    // todo(iceberg) handle topic / partition disabling
    auto iceberg_disabled = topic_cfg->properties.iceberg_mode
                            == model::iceberg_mode::disabled;
    if (!partition->is_leader() || iceberg_disabled) {
        if (it != _translators.end()) {
            ssx::spawn_with_gate(_gate, [this, partition] {
                return stop_translator(partition->ntp());
            });
        }
        return;
    }
    // By now we know the partition is a leader and iceberg is enabled, so
    // there has to be a translator, spin one up if it doesn't already exist.
    if (it == _translators.end()) {
        start_translator(partition, topic_cfg->properties.iceberg_mode);
    } else {
        // check if translation interval changed.
        auto target_interval = translation_interval_ms();
        if (it->second->translation_interval() != target_interval) {
            it->second->reset_translation_interval(target_interval);
        }
    }
}

void datalake_manager::start_translator(
  ss::lw_shared_ptr<cluster::partition> partition, model::iceberg_mode mode) {
    auto it = _translators.find(partition->ntp());
    vassert(
      it == _translators.end(),
      "Attempt to start a translator for ntp {} in term {} while another "
      "instance already exists",
      partition->ntp(),
      partition->term());
    auto translator = std::make_unique<translation::partition_translator>(
      partition,
      _coordinator_frontend,
      _features,
      &_cloud_data_io,
      _schema_mgr.get(),
      make_type_resolver(mode, *_schema_registry),
      make_record_translator(mode),
      translation_interval_ms(),
      _sg,
      _effective_max_translator_buffered_data,
      &_parallel_translations);
    _translators.emplace(partition->ntp(), std::move(translator));
}

ss::future<> datalake_manager::stop_translator(const model::ntp& ntp) {
    auto it = _translators.find(ntp);
    if (it == _translators.end()) {
        co_return;
    }
    auto translator = std::move(it->second);
    _translators.erase(it);
    co_await translator->stop();
}

} // namespace datalake

/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#include "datalake/coordinator/coordinator_manager.h"

#include "cluster/partition_manager.h"
#include "cluster/topics_frontend.h"
#include "config/configuration.h"
#include "datalake/catalog_schema_manager.h"
#include "datalake/coordinator/catalog_factory.h"
#include "datalake/coordinator/coordinator.h"
#include "datalake/coordinator/iceberg_file_committer.h"
#include "datalake/coordinator/state_machine.h"
#include "datalake/logger.h"
#include "datalake/record_schema_resolver.h"
#include "datalake/table_creator.h"
#include "iceberg/manifest_io.h"
#include "model/fundamental.h"
#include "schema/registry.h"

#include <seastar/core/shared_ptr.hh>

namespace datalake::coordinator {

coordinator_manager::coordinator_manager(
  model::node_id self,
  ss::sharded<raft::group_manager>& gm,
  ss::sharded<cluster::partition_manager>& pm,
  ss::sharded<cluster::topic_table>& topics,
  ss::sharded<cluster::topics_frontend>& topics_fe,
  pandaproxy::schema_registry::api* sr_api,
  std::unique_ptr<catalog_factory> catalog_factory,
  ss::sharded<cloud_io::remote>& io,
  cloud_storage_clients::bucket_name bucket)
  : self_(self)
  , gm_(gm.local())
  , pm_(pm.local())
  , topics_(topics.local())
  , topics_fe_(topics_fe)
  , schema_registry_(schema::registry::make_default(sr_api))
  , manifest_io_(io.local(), bucket)
  , catalog_factory_(std::move(catalog_factory))
  , type_resolver_(
      std::make_unique<record_schema_resolver>(*schema_registry_)) {}

coordinator_manager::~coordinator_manager() = default;

ss::future<> coordinator_manager::start() {
    catalog_ = co_await catalog_factory_->create_catalog();
    schema_mgr_ = std::make_unique<catalog_schema_manager>(*catalog_);
    table_creator_ = std::make_unique<direct_table_creator>(
      *type_resolver_, *schema_mgr_);
    file_committer_ = std::make_unique<iceberg_file_committer>(
      *catalog_, manifest_io_);

    manage_notifications_ = pm_.register_manage_notification(
      model::datalake_coordinator_nt.ns,
      [this](ss::lw_shared_ptr<cluster::partition> p) { start_managing(*p); });
    unmanage_notifications_ = pm_.register_unmanage_notification(
      model::datalake_coordinator_nt.ns,
      [this](model::topic_partition_view tp) {
          if (tp.topic != model::datalake_coordinator_topic) {
              return;
          }
          auto ntp = model::ntp{
            model::datalake_coordinator_nt.ns,
            model::datalake_coordinator_nt.tp,
            tp.partition};
          stop_managing(ntp);
      });
    leadership_notifications_ = gm_.register_leadership_notification(
      [this](
        raft::group_id group,
        model::term_id term,
        std::optional<model::node_id> leader_id) {
          notify_leadership_change(group, term, leader_id);
      });
}

ss::future<> coordinator_manager::stop() {
    if (manage_notifications_) {
        pm_.unregister_manage_notification(*manage_notifications_);
    }
    if (unmanage_notifications_) {
        pm_.unregister_unmanage_notification(*unmanage_notifications_);
    }
    if (leadership_notifications_) {
        gm_.unregister_leadership_notification(*leadership_notifications_);
    }
    auto gate_close = gate_.close();
    for (auto& [_, crd] : coordinators_) {
        co_await crd->stop_and_wait();
    }
    co_await std::move(gate_close);
}

void coordinator_manager::start_managing(cluster::partition& p) {
    if (gate_.is_closed()) {
        return;
    }
    auto ntp = p.get_ntp_config().ntp();
    if (
      ntp.ns != model::datalake_coordinator_nt.ns
      || ntp.tp.topic != model::datalake_coordinator_topic) {
        return;
    }
    if (coordinators_.contains(ntp)) {
        return;
    }
    auto stm = p.raft()->stm_manager()->get<coordinator_stm>();
    if (stm == nullptr) {
        return;
    }
    auto crd = ss::make_lw_shared<coordinator>(
      std::move(stm),
      topics_,
      *table_creator_,
      [this](const model::topic& t, model::revision_id rev) {
          return remove_tombstone(t, rev);
      },
      *file_committer_,
      config::shard_local_cfg().iceberg_catalog_commit_interval_ms.bind());
    if (p.is_leader()) {
        crd->notify_leadership(self_);
    }
    crd->start();
    coordinators_.emplace(ntp, std::move(crd));
}

void coordinator_manager::stop_managing(const model::ntp& ntp) {
    if (gate_.is_closed()) {
        // Cleanup should happen in class stop method.
        return;
    }
    auto it = coordinators_.find(ntp);
    if (it == coordinators_.end()) {
        return;
    }
    auto crd = std::move(it->second);
    coordinators_.erase(it);
    ssx::background = crd->stop_and_wait().finally(
      [c = crd, g = gate_.hold()] {});
}

ss::lw_shared_ptr<coordinator>
coordinator_manager::get(const model::ntp& ntp) const {
    if (gate_.is_closed()) {
        return nullptr;
    }
    auto it = coordinators_.find(ntp);
    if (it == coordinators_.end()) {
        return nullptr;
    }
    return it->second;
}

void coordinator_manager::notify_leadership_change(
  raft::group_id group,
  model::term_id,
  std::optional<model::node_id> leader_id) {
    if (gate_.is_closed()) {
        return;
    }
    auto p = pm_.partition_for(group);
    if (p == nullptr) {
        return;
    }
    const auto& ntp = p->ntp();
    auto crd = get(ntp);
    if (crd == nullptr) {
        return;
    }
    crd->notify_leadership(leader_id);
}

ss::future<checked<std::nullopt_t, coordinator::errc>>
coordinator_manager::remove_tombstone(
  const model::topic& topic, model::revision_id rev) {
    auto topic_res = co_await topics_fe_.local().purged_topic(
      cluster::nt_revision{
        .nt = model::topic_namespace{model::kafka_namespace, topic},
        .initial_revision_id = model::initial_revision_id{rev}},
      cluster::topic_purge_domain::iceberg,
      5s);
    switch (topic_res.ec) {
    case cluster::errc::success:
    case cluster::errc::topic_not_exists:
        co_return std::nullopt;
    case cluster::errc::shutting_down:
        co_return coordinator::errc::shutting_down;
    case cluster::errc::timeout:
        co_return coordinator::errc::timedout;
    default:
        vlog(
          datalake_log.warn,
          "failed to remove iceberg tombstone, topic {}, rev {}: {}",
          topic,
          rev,
          topic_res.ec);
        co_return coordinator::errc::failed;
    }
}

} // namespace datalake::coordinator

/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "kafka/server/group_initializer.h"

#include "cluster/controller_api.h"
#include "cluster/topics_frontend.h"
#include "kafka/protocol/logger.h"
/*
 * This class creates consumer offsets topic
 */
namespace kafka {
group_initializer::group_initializer(
  coordinator_ntp_mapper& coordinator_ntp_mapper,
  ss::sharded<cluster::topics_frontend>& topics_frontend,
  ss::sharded<cluster::members_table>& members_table,
  ss::sharded<cluster::controller_api>& controller_api)
  : _coordinator_ntp_mapper(coordinator_ntp_mapper)
  , _topics_frontend(topics_frontend)
  , _members_table(members_table)
  , _controller_api(controller_api) {}

ss::future<bool> group_initializer::assure_topic_exists(
  bool wait_for_reconciliation,
  model::timeout_clock::time_point reconciliation_deadline) {
    if (_coordinator_ntp_mapper.topic_exists()) {
        co_return true;
    }
    vlog(klog.trace, "can't find consumer group topic, creating");
    if (!co_await try_create_consumer_group_topic()) {
        co_return false;
    }
    if (wait_for_reconciliation) {
        if (!_controller_api.local_is_initialized()) {
            vlog(
              klog.warn,
              "can not create consumer offsets topic topic - controller_api "
              "not initialized");
            co_return false;
        }
        auto ec = co_await _controller_api.local().wait_for_topic(
          model::kafka_consumer_offsets_nt, reconciliation_deadline);
        if (ec) {
            vlog(
              klog.warn,
              "Failed to wait for topic {} reconciliation: {}",
              model::kafka_consumer_offsets_nt,
              ec);
            co_return false;
        }
    }
    co_return true;
}

/*
 * create the internal metadata topic for group membership
 */
ss::future<bool> group_initializer::try_create_consumer_group_topic() {
    // Attempt to use internal topic replication factor, if enough nodes found.
    auto replication_factor
      = (int16_t)config::shard_local_cfg().internal_topic_replication_factor();
    if (!_members_table.local_is_initialized()) {
        vlog(
          klog.warn,
          "can not create consumer offsets topic topic - members_table not "
          "initialized");
        return ssx::now(false);
    }
    if (
      static_cast<size_t>(replication_factor)
      > _members_table.local().node_count()) {
        replication_factor = 1;
    }

    // the new internal metadata topic for group membership
    cluster::topic_configuration topic{
      _coordinator_ntp_mapper.ns(),
      _coordinator_ntp_mapper.topic(),
      config::shard_local_cfg().group_topic_partitions(),
      replication_factor};

    topic.properties.cleanup_policy_bitflags
      = model::cleanup_policy_bitflags::compaction;
    // allow cretation even if a consumer group migration is in progress
    topic.is_migrated = true;

    if (!_topics_frontend.local_is_initialized()) {
        vlog(
          klog.warn,
          "can not create consumer offsets topic topic - topic_frontend not "
          "initialized");
        return ssx::now(false);
    }
    return _topics_frontend.local()
      .autocreate_topics(
        {std::move(topic)},
        config::shard_local_cfg().internal_rpc_request_timeout_ms())
      .then([this](std::vector<cluster::topic_result> res) {
          /*
           * kindly ask client to retry on error
           */
          vassert(res.size() == 1, "expected exactly one result");
          if (
            res[0].ec != cluster::errc::success
            && res[0].ec != cluster::errc::topic_already_exists) {
              vlog(
                klog.warn,
                "can not create {}/{} topic - error: {}",
                _coordinator_ntp_mapper.ns()(),
                _coordinator_ntp_mapper.topic()(),
                cluster::make_error_code(res[0].ec).message());
              return false;
          }
          return true;
      })
      .handle_exception([this](const std::exception_ptr& e) {
          vlog(
            klog.warn,
            "can not create {}/{} topic - exception: {}",
            _coordinator_ntp_mapper.ns()(),
            _coordinator_ntp_mapper.topic()(),
            e);
          // various errors may returned such as a timeout, or if the
          // controller group doesn't have a leader. client will retry.
          return false;
      });
}

} // namespace kafka

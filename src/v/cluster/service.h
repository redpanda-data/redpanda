/*
 * Copyright 2020 Vectorized, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once
#include "cluster/controller_service.h"
#include "cluster/fwd.h"
#include "cluster/types.h"
#include "rpc/types.h"

#include <seastar/core/sharded.hh>

#include <vector>

namespace cluster {
class service : public controller_service {
public:
    service(
      ss::scheduling_group,
      ss::smp_service_group,
      ss::sharded<topics_frontend>&,
      ss::sharded<members_manager>&,
      ss::sharded<metadata_cache>&,
      ss::sharded<security_frontend>&,
      ss::sharded<controller_api>&,
      ss::sharded<members_frontend>&,
      ss::sharded<config_frontend>&,
      ss::sharded<feature_manager>&,
      ss::sharded<feature_table>&,
      ss::sharded<health_monitor_frontend>&);

    virtual ss::future<join_reply>
    join(join_request&&, rpc::streaming_context&) override;

    virtual ss::future<join_node_reply>
    join_node(join_node_request&&, rpc::streaming_context&) override;

    virtual ss::future<create_topics_reply>
    create_topics(create_topics_request&&, rpc::streaming_context&) override;

    ss::future<create_non_replicable_topics_reply> create_non_replicable_topics(
      create_non_replicable_topics_request&&, rpc::streaming_context&) final;

    ss::future<configuration_update_reply> update_node_configuration(
      configuration_update_request&&, rpc::streaming_context&) final;

    ss::future<finish_partition_update_reply> finish_partition_update(
      finish_partition_update_request&&, rpc::streaming_context&) final;

    ss::future<update_topic_properties_reply> update_topic_properties(
      update_topic_properties_request&&, rpc::streaming_context&) final;
    ss::future<reconciliation_state_reply> get_reconciliation_state(
      reconciliation_state_request&&, rpc::streaming_context&) final;

    ss::future<create_acls_reply>
    create_acls(create_acls_request&&, rpc::streaming_context&) final;

    ss::future<delete_acls_reply>
    delete_acls(delete_acls_request&&, rpc::streaming_context&) final;

    ss::future<decommission_node_reply> decommission_node(
      decommission_node_request&&, rpc::streaming_context&) final;

    ss::future<recommission_node_reply> recommission_node(
      recommission_node_request&&, rpc::streaming_context&) final;

    ss::future<finish_reallocation_reply> finish_reallocation(
      finish_reallocation_request&&, rpc::streaming_context&) final;

    ss::future<config_status_reply>
    config_status(config_status_request&&, rpc::streaming_context&) final;

    ss::future<config_update_reply>
    config_update(config_update_request&&, rpc::streaming_context&) final;

    ss::future<get_node_health_reply> collect_node_health_report(
      get_node_health_request&&, rpc::streaming_context&) final;

    ss::future<get_cluster_health_reply> get_cluster_health_report(
      get_cluster_health_request&&, rpc::streaming_context&) final;

    ss::future<feature_action_response>
    feature_action(feature_action_request&& req, rpc::streaming_context&) final;

    ss::future<feature_barrier_response>
    feature_barrier(feature_barrier_request&&, rpc::streaming_context&) final;

private:
    std::
      pair<std::vector<model::topic_metadata>, std::vector<topic_configuration>>
      fetch_metadata_and_cfg(const std::vector<topic_result>&);

    ss::future<finish_partition_update_reply>
    do_finish_partition_update(finish_partition_update_request&&);

    ss::future<update_topic_properties_reply>
    do_update_topic_properties(update_topic_properties_request&&);

    ss::future<reconciliation_state_reply>
      do_get_reconciliation_state(reconciliation_state_request);

    ss::future<finish_reallocation_reply>
      do_finish_reallocation(finish_reallocation_request);

    ss::future<get_node_health_reply>
      do_collect_node_health_report(get_node_health_request);

    ss::future<get_cluster_health_reply>
      do_get_cluster_health_report(get_cluster_health_request);

    ss::sharded<topics_frontend>& _topics_frontend;
    ss::sharded<members_manager>& _members_manager;
    ss::sharded<metadata_cache>& _md_cache;
    ss::sharded<security_frontend>& _security_frontend;
    ss::sharded<controller_api>& _api;
    ss::sharded<members_frontend>& _members_frontend;
    ss::sharded<config_frontend>& _config_frontend;
    ss::sharded<feature_manager>& _feature_manager;
    ss::sharded<feature_table>& _feature_table;
    ss::sharded<health_monitor_frontend>& _hm_frontend;
};
} // namespace cluster

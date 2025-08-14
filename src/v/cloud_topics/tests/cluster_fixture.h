/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#pragma once

#include "cloud_io/tests/s3_imposter.h"
#include "cloud_topics/app.h"
#include "cloud_topics/level_one/metastore/simple_stm.h"
#include "cluster/tests/cluster_test_fixture.h"
#include "model/fundamental.h"
#include "model/namespace.h"
#include "raft/state_machine_manager.h"

namespace experimental::cloud_topics {

class cluster_fixture
  : public cluster_test_fixture
  , public s3_imposter_fixture {
public:
    cluster_fixture() { set_expectations_and_listen({}); }

    ~cluster_fixture() {
        for (auto id : instance_ids()) {
            remove_node_application(id);
        }
    }
    void add_node() {
        static constexpr int kafka_port_base = 9092;
        static constexpr int rpc_port_base = 11000;
        static constexpr int proxy_port_base = 8082;
        static constexpr int schema_reg_port_base = 8081;
        auto [s3_conf, a_conf, cs_conf] = get_cloud_storage_configurations(
          httpd_host_name, httpd_port_number());
        create_node_application(
          next_node_id(),
          kafka_port_base,
          rpc_port_base,
          proxy_port_base,
          schema_reg_port_base,
          configure_node_id::yes,
          empty_seed_starts_cluster::yes,
          s3_conf,
          std::move(*a_conf),
          cs_conf,
          false,
          false,
          /*cloud_topics_enabled=*/true);
    }

    cloud_topics::app& get_ct_app(model::node_id id) {
        return *instance(id)->app.cloud_topics_app;
    }

    ss::shared_ptr<l1::simple_stm> get_l1_stm(model::partition_id pid) {
        auto domain_ntp = model::ntp{
          model::kafka_internal_namespace, model::l1_metastore_topic, pid};
        auto [leader_fx, leader_p] = get_leader(domain_ntp);
        if (leader_fx == nullptr) {
            return nullptr;
        }
        return leader_p->raft()->stm_manager()->get<l1::simple_stm>();
    }
};

} // namespace experimental::cloud_topics

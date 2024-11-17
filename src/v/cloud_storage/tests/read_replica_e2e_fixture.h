/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */
#pragma once
#include "cloud_storage/tests/s3_imposter.h"
#include "redpanda/tests/fixture.h"
#include "test_utils/scoped_config.h"

class read_replica_e2e_fixture
  : public s3_imposter_fixture
  , public redpanda_thread_fixture
  , public enable_cloud_storage_fixture {
public:
    read_replica_e2e_fixture()
      : redpanda_thread_fixture(
          redpanda_thread_fixture::init_cloud_storage_tag{},
          httpd_port_number()) {
        // No expectations: tests will PUT and GET organically.
        set_expectations_and_listen({});
        wait_for_controller_leadership().get();

        // Disable metrics to speed things up.
        test_local_cfg.get("enable_metrics_reporter").set_value(false);
        test_local_cfg.get("disable_metrics").set_value(true);
        test_local_cfg.get("disable_public_metrics").set_value(true);

        // Avoid background work since we'll control uploads ourselves.
        test_local_cfg.get("cloud_storage_enable_segment_merging")
          .set_value(false);
        test_local_cfg.get("cloud_storage_disable_upload_loop_for_tests")
          .set_value(true);
        test_local_cfg.get("cloud_storage_disable_read_replica_loop_for_tests")
          .set_value(true);
    }

    std::unique_ptr<redpanda_thread_fixture> start_read_replica_fixture() {
        return std::make_unique<redpanda_thread_fixture>(
          model::node_id(2),
          9092 + 10,
          33145 + 10,
          8082 + 10,
          8081 + 10,
          std::vector<config::seed_server>{},
          ssx::sformat("test.dir_read_replica{}", time(0)),
          app.sched_groups,
          true,
          get_s3_config(httpd_port_number()),
          get_archival_config(),
          get_cloud_config(httpd_port_number()));
    }
    scoped_config test_local_cfg;
};

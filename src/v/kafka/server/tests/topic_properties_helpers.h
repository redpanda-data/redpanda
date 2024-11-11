// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "config_frontend.h"
#include "redpanda/tests/fixture.h"

class topic_properties_test_fixture : public redpanda_thread_fixture {
public:
    topic_properties_test_fixture() { wait_for_controller_leadership().get(); }

    void revoke_license() {
        app.controller->get_feature_table()
          .invoke_on_all([](auto& ft) { return ft.revoke_license(); })
          .get();
    }

    void reinstall_license() {
        app.controller->get_feature_table()
          .invoke_on_all([](auto& ft) {
              return ft.set_builtin_trial_license(model::timestamp::now());
          })
          .get();
    }

    void update_cluster_config(std::string_view k, std::string_view v) {
        app.controller->get_config_frontend()
          .local()
          .patch(
            cluster::config_update_request{
              .upsert{{ss::sstring{k}, ss::sstring{v}}},
            },
            model::timeout_clock::now() + 5s)
          .get();
    }
};

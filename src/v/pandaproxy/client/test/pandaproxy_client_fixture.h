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

#include "kafka/requests/metadata_request.h"
#include "pandaproxy/client/client.h"
#include "redpanda/tests/fixture.h"

namespace ppc = pandaproxy::client;

class ppc_test_fixture : public redpanda_thread_fixture {
public:
    void restart() {
        app.shutdown();
        ss::smp::invoke_on_all([this] {
            auto& config = config::shard_local_cfg();
            config.get("disable_metrics").set_value(false);
        }).get0();
        app.initialize();
        app.check_environment();
        app.configure_admin_server();
        app.wire_up_services();
        app.start();
    }

    ppc::client make_client() {
        return ppc::client(std::vector<unresolved_address>{
          config::shard_local_cfg().kafka_api()});
    }
    ppc::client make_connected_client() {
        auto client = make_client();
        client.connect().get();
        return client;
    }

    kafka::metadata_request make_list_topics_req() {
        return kafka::metadata_request{.list_all_topics = true};
    }
};

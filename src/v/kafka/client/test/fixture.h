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

#include "kafka/client/client.h"
#include "kafka/protocol/metadata.h"
#include "redpanda/tests/fixture.h"

namespace kc = kafka::client;

class kafka_client_fixture : public redpanda_thread_fixture {
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

    kc::client make_client() {
        return kc::client(std::vector<unresolved_address>{
          config::shard_local_cfg().kafka_api()[0].address});
    }
    kc::client make_connected_client() {
        auto client = make_client();
        client.connect().get();
        return client;
    }

    auto make_list_topics_req() {
        return
          []() { return kafka::metadata_request{.list_all_topics = true}; };
    }

    model::topic_namespace
    make_data(model::revision_id rev, int partitions = 1, int topic = 0) {
        auto topic_name = fmt::format("my_topic_{}", topic);
        auto tp_ns = model::topic_namespace(
          model::kafka_namespace, model::topic{topic_name});

        for (int p = 0; p < partitions; ++p) {
            model::ntp ntp(tp_ns.ns, tp_ns.tp, model::partition_id(p));

            storage::ntp_config ntp_cfg(
              ntp, lconf().data_directory().as_sstring(), nullptr, rev);

            storage::disk_log_builder builder(make_default_config());
            using namespace storage; // NOLINT
            builder | start(std::move(ntp_cfg)) | add_segment(model::offset(0))
              | add_random_batches(
                model::offset(0), 20, maybe_compress_batches::no)
              | stop();
        }
        add_topic(tp_ns, partitions).get();
        return tp_ns;
    }
};

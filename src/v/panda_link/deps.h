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

#pragma once

#include "container/fragmented_vector.h"
#include "kafka/client/configuration.h"
#include "kafka/protocol/describe_configs.h"
#include "kafka/protocol/metadata.h"
#include "model/fundamental.h"

namespace panda_link {
class remote_cluster_connection {
public:
    remote_cluster_connection() = default;
    remote_cluster_connection(const remote_cluster_connection&) = delete;
    remote_cluster_connection& operator=(const remote_cluster_connection&)
      = delete;
    remote_cluster_connection(remote_cluster_connection&&) = delete;
    remote_cluster_connection& operator=(remote_cluster_connection&&) = delete;
    virtual ~remote_cluster_connection() = default;

    virtual ss::future<std::error_code> connect() = 0;
    virtual ss::future<std::error_code> disconnect() = 0;

    virtual ss::future<kafka::metadata_response>
      get_metadata(kafka::metadata_request) = 0;

    virtual ss::future<kafka::describe_configs_response>
      describe_topics(fragmented_vector<model::topic_view>) = 0;
};

class remote_cluster_connection_factory {
public:
    explicit remote_cluster_connection_factory(
      std::unique_ptr<kafka::client::configuration> cfg)
      : _cfg(std::move(cfg)) {}
    remote_cluster_connection_factory(const remote_cluster_connection_factory&)
      = delete;
    remote_cluster_connection_factory&
    operator=(const remote_cluster_connection_factory&)
      = delete;
    remote_cluster_connection_factory(remote_cluster_connection_factory&&)
      = delete;
    remote_cluster_connection_factory&
    operator=(remote_cluster_connection_factory&&)
      = delete;

    virtual ~remote_cluster_connection_factory() = default;
    virtual std::unique_ptr<remote_cluster_connection> make() = 0;

    void set_config(std::unique_ptr<kafka::client::configuration> cfg) {
        _cfg = std::move(cfg);
    }

    const kafka::client::configuration& get_config() { return *_cfg.get(); }

private:
    std::unique_ptr<kafka::client::configuration> _cfg{nullptr};
};
} // namespace panda_link

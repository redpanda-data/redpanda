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

#include "cluster_link/manager.h"
#include "cluster_link/tests/source_cluster.h"

namespace cluster_link::tests {
class test_connection : public remote_cluster_connection {
public:
    template<typename T>
    using result = remote_cluster_connection::result<T>;

    explicit test_connection(source_cluster* source_cluster)
      : _source_cluster(source_cluster) {}

    virtual ss::future<result<void>> connect() override;

    virtual ss::future<result<void>> disconnect() override;

    virtual ss::future<result<kafka::metadata_response>>
    fetch_metadata(kafka::metadata_request req) override;

    virtual ss::future<result<kafka::describe_configs_response>>
      describe_topic_configs(
        chunked_vector<::model::topic>,
        std::optional<chunked_vector<ss::sstring>>) override;

private:
    small_fragment_vector<kafka::metadata_response_topic>
    create_metadata_topics_response(const kafka::metadata_request&);

    large_fragment_vector<kafka::metadata_response_partition>
    create_metadata_partitions_response(const source_topic& t);

private:
    bool _is_connected{false};
    source_cluster* _source_cluster;
};

class test_connection_factory : public remote_cluster_connection_factory {
public:
    explicit test_connection_factory(source_cluster*);

    std::unique_ptr<remote_cluster_connection>
    create_remote_cluster_connection() override;

private:
    source_cluster* _source_cluster;
};
} // namespace cluster_link::tests

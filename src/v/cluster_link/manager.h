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

#include "base/outcome.h"
#include "cluster_link/link.h"
#include "cluster_link/model/types.h"
#include "container/fragmented_vector.h"
#include "kafka/protocol/describe_configs.h"
#include "kafka/protocol/metadata.h"
#include "model/fundamental.h"
#include "ssx/work_queue.h"

#include <absl/container/flat_hash_map.h>

namespace cluster_link {

/// Indicates if the current node is the leader for a given NTP
using ntp_leader = ss::bool_class<struct is_ntp_leader_tag>;

/**
 * @brief Abstract class that provides accessors to panda link table
 *
 */
class link_registry {
public:
    link_registry() = default;
    link_registry(const link_registry&) = delete;
    link_registry(link_registry&&) = delete;
    link_registry& operator=(const link_registry&) = delete;
    link_registry& operator=(link_registry&&) = delete;
    virtual ~link_registry() = default;

    virtual std::optional<std::reference_wrapper<const model::metadata>>
      find_link_by_id(model::id_t) const = 0;

    virtual std::optional<std::reference_wrapper<const model::metadata>>
    find_link_by_name(const model::name_t&) const = 0;

    virtual chunked_vector<model::id_t> get_all_link_ids() const = 0;
};

/**
 * @brief Abstract class that represents a Kafka connection to a remote cluster
 *
 */
class remote_cluster_connection {
public:
    remote_cluster_connection() = default;
    remote_cluster_connection(const remote_cluster_connection&) = delete;
    remote_cluster_connection& operator=(const remote_cluster_connection&)
      = delete;
    remote_cluster_connection(remote_cluster_connection&&) = delete;
    remote_cluster_connection& operator=(remote_cluster_connection&&) = delete;
    virtual ~remote_cluster_connection() = default;

    template<typename T>
    using result = result<T, std::exception_ptr>;

    /// Connects to the remote cluster
    virtual ss::future<result<void>> connect() = 0;
    /// Disconnects from the remote cluster
    virtual ss::future<result<void>> disconnect() = 0;
    /// Fetches the metadata from the remote cluster
    virtual ss::future<result<kafka::metadata_response>>
      fetch_metadata(kafka::metadata_request) = 0;
    /// Describes the configurations for a list of topics
    virtual ss::future<result<kafka::describe_configs_response>>
      describe_topic_configs(
        chunked_vector<::model::topic>,
        std::optional<chunked_vector<ss::sstring>>)
      = 0;
};

/**
 * @brief Factory used to create remote cluster connections
 *
 */
class remote_cluster_connection_factory {
public:
    remote_cluster_connection_factory() = default;
    remote_cluster_connection_factory(const remote_cluster_connection_factory&)
      = delete;
    remote_cluster_connection_factory(remote_cluster_connection_factory&&)
      = delete;
    remote_cluster_connection_factory&
    operator=(const remote_cluster_connection_factory&)
      = delete;
    remote_cluster_connection_factory&
    operator=(remote_cluster_connection_factory&&)
      = delete;
    virtual ~remote_cluster_connection_factory() = default;

    virtual std::unique_ptr<remote_cluster_connection>
    create_remote_cluster_connection() = 0;
};

/**
 * @brief Factory abstract class to create new links
 *
 */
class link_factory {
public:
    link_factory() = default;
    link_factory(const link_factory&) = delete;
    link_factory(link_factory&&) = delete;
    link_factory& operator=(const link_factory&) = delete;
    link_factory& operator=(link_factory&&) = delete;
    virtual ~link_factory() = default;

    virtual std::unique_ptr<link> create_link(model::metadata config) = 0;
};

/**
 * @brief Class used to manage panda links
 *
 * This class will be notified of changes in Redpanda to create, modify, or
 * remove panda links and to handle leadership changes of partitions.
 */
class manager {
public:
    manager(
      ::model::node_id self,
      std::unique_ptr<link_registry> registry,
      std::unique_ptr<link_factory> link_factory);
    manager(const manager&) = delete;
    manager(manager&&) = delete;
    manager& operator=(const manager&) = delete;
    manager& operator=(manager&&) = delete;
    virtual ~manager() = default;

    ss::future<> start();
    ss::future<> stop();

    /// Used to notify that a panda link has been updated
    void on_link_change(model::id_t id);
    /// Used to notify manager in a change of NTP leadership
    void on_leadership_change(::model::ntp ntp, ntp_leader is_ntp_leader);
    /// Handles creation and start of a link
    ss::future<> handle_on_link_change(model::id_t id);

private:
    ::model::node_id _self;
    std::unique_ptr<link_registry> _registry;
    std::unique_ptr<link_factory> _link_factory;
    ssx::work_queue _queue;

    absl::flat_hash_map<model::id_t, std::unique_ptr<link>> _links;
};
} // namespace cluster_link

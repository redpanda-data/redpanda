/**
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/dev/licenses/rcl.md
 *
 */

#pragma once

#include "cluster_link/model/types.h"
#include "cluster_link/task.h"
#include "kafka/protocol/describe_acls.h"
#include "kafka/protocol/types.h"

namespace cluster_link {

/**
 * @brief This task is responsible for syncing security settings from the source
 * cluster
 *
 * Currently only ACLs are supported, future will be SCRAM creds and roles
 */
class security_migrator : public controller_locked_task {
public:
    static constexpr auto task_name = "Security Migrator Task";
    static constexpr kafka::cluster_authorized_operations required_permissions
      = kafka::cluster_authorized_operations{0x100}; // DESCRIBE

    security_migrator(link* link, const model::metadata& config);
    security_migrator(const security_migrator&) = delete;
    security_migrator(security_migrator&&) = delete;
    security_migrator& operator=(const security_migrator&) = delete;
    security_migrator& operator=(security_migrator&&) = delete;
    ~security_migrator() override = default;

    void update_config(const model::metadata& config) override;

protected:
    ss::future<> run_impl() override;

private:
    ss::future<chunked_vector<kafka::describe_acls_resource>>
    fetch_acls(kafka::api_version describe_acls_version);

private:
    model::security_settings_sync_config _config;
};

/**
 * @brief Factory class used to create security_migrator's
 */
class security_migrator_factory : public task_factory {
public:
    std::string_view created_task_name() const noexcept final;
    std::unique_ptr<task> create_task(link* link) final;
};
} // namespace cluster_link

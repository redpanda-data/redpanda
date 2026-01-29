/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#pragma once

#include "iceberg/catalog.h"
#include "ssx/mutex.h"

namespace datalake {
class credential_manager;
} // namespace datalake

namespace iceberg {
namespace rest_client {
class catalog_client;
}

struct create_table_request;
/**
 * Iceberg REST catalog implementation, the catalog manages lifecycle of a rest
 * provided catalog client and manages concurency control.
 */
class rest_catalog final : public catalog {
public:
    explicit rest_catalog(
      std::unique_ptr<rest_client::catalog_client>,
      config::binding<std::chrono::milliseconds> request_timeout,
      std::optional<ss::sstring> base_location = std::nullopt);

    rest_catalog(const rest_catalog&) = delete;
    rest_catalog(rest_catalog&&) noexcept = default;
    rest_catalog& operator=(const rest_catalog&) = delete;
    rest_catalog& operator=(rest_catalog&&) noexcept = delete;

    ss::future<checked<table_metadata, errc>> create_table(
      const table_identifier& table_ident,
      const schema& schema,
      const partition_spec& spec) final;

    ss::future<checked<table_metadata, errc>>
    load_table(const table_identifier& table_ident) final;

    ss::future<checked<void, errc>>
    drop_table(const table_identifier& table_ident, bool purge) final;

    ss::future<checked<table_metadata, errc>>
    commit_txn(const table_identifier& table_ident, transaction) final;

    ~rest_catalog() final = default;

    ss::future<> stop() final;

private:
    retry_chain_node create_rtc();
    void set_table_location_if_needed(
      create_table_request& request, const table_identifier& t_id) const;
    std::unique_ptr<rest_client::catalog_client> client_;
    config::binding<std::chrono::milliseconds> request_timeout_;
    std::optional<ss::sstring> base_location_;
    // currently we use very simple concurrency control i.e. we only allow one
    // REST request at a time
    ssx::mutex lock_;
    ss::abort_source as_;
};
}; // namespace iceberg

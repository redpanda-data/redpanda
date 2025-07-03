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
#include "cloud_io/remote.h"
#include "cloud_roles/apply_credentials.h"
#include "iceberg/rest_client/credentials.h"
#include "iceberg/rest_client/oauth_token.h"

namespace datalake {
class credential_manager;
} // namespace datalake

namespace iceberg {
class catalog;
class filesystem_catalog;
class rest_catalog;
} // namespace iceberg
namespace iceberg::rest_client {
class client_probe;
} // namespace iceberg::rest_client

namespace datalake::coordinator {

class catalog_factory {
public:
    virtual ~catalog_factory() = default;
    virtual ss::future<std::unique_ptr<iceberg::catalog>>
    create_catalog(ss::abort_source&) = 0;
};

/**
 * Rest catalog factory, the catalog properties are set based on the
 * configuration provided.
 */
class rest_catalog_factory : public catalog_factory {
public:
    explicit rest_catalog_factory(
      config::configuration& config,
      ss::metrics::label_instance,
      datalake::credential_manager& cred_mgr);
    ~rest_catalog_factory() override;

    ss::future<std::unique_ptr<iceberg::catalog>>
    create_catalog(ss::abort_source&) final;

private:
    struct credentials_and_token {
        std::optional<iceberg::rest_client::credentials> credentials{
          std::nullopt};
        std::optional<iceberg::rest_client::oauth_token> token{std::nullopt};
    };

    credentials_and_token make_credentials_or_token();

    config::configuration* config_;
    ss::shared_ptr<iceberg::rest_client::client_probe> client_probe_;
    datalake::credential_manager& credential_manager_;
};
/**
 * Filesystem catalog factory, the will use provided cloud_io::remote and bucket
 */

class filesystem_catalog_factory : public catalog_factory {
public:
    filesystem_catalog_factory(
      config::configuration& config,
      cloud_io::remote& remote,
      const cloud_storage_clients::bucket_name& bucket);

    ss::future<std::unique_ptr<iceberg::catalog>>
    create_catalog(ss::abort_source&) final;

private:
    config::configuration* config_;
    cloud_io::remote* remote_;
    cloud_storage_clients::bucket_name bucket_;
};

/**
 * Returns a catalog factory based on the configuration provided.
 * The given metrics label instance should distinguish metrics from different
 * instances of catalog factory instances.
 */
std::unique_ptr<catalog_factory> get_catalog_factory(
  config::configuration& config,
  cloud_io::remote& remote,
  const cloud_storage_clients::bucket_name& bucket,
  ss::metrics::label_instance label,
  datalake::credential_manager& cred_mgr);

} // namespace datalake::coordinator

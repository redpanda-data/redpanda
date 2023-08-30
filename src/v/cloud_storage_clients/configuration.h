/*
 * Copyright 2022 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#pragma once

#include "cloud_roles/types.h"
#include "cloud_storage_clients/client_probe.h"
#include "cloud_storage_clients/types.h"
#include "net/transport.h"
#include "net/types.h"

namespace cloud_storage_clients {

/// List of default overrides that can be used to workaround issues
/// that can arise when we want to deal with different S3 API implementations
/// and different OS issues (like different truststore locations on different
/// Linux distributions).
struct default_overrides {
    std::optional<endpoint_url> endpoint = std::nullopt;
    std::optional<uint16_t> port = std::nullopt;
    std::optional<ca_trust_file> trust_file = std::nullopt;
    std::optional<ss::lowres_clock::duration> max_idle_time = std::nullopt;
    bool disable_tls = false;
};

/// Configuration options common accross cloud storage clients
struct common_configuration : net::base_transport::configuration {
    /// URI of the access point
    access_point_uri uri;
    /// Max time that connection can spend idle
    ss::lowres_clock::duration max_idle_time;
    /// Metrics probe (should be created for every aws account on every shard)
    ss::shared_ptr<client_probe> _probe;

    bool requires_self_configuration{false};
};

struct s3_configuration : common_configuration {
    /// AWS region
    cloud_roles::aws_region_name region;
    /// AWS access key, optional if configuration uses temporary credentials
    std::optional<cloud_roles::public_key_str> access_key;
    /// AWS secret key, optional if configuration uses temporary credentials
    std::optional<cloud_roles::private_key_str> secret_key;

    /// \brief opinionated configuraiton initialization
    /// Generates uri field from region, initializes credentials for the
    /// transport, resolves the uri to get the server_addr.
    ///
    /// \param pkey is an AWS access key
    /// \param skey is an AWS secret key
    /// \param region is an AWS region code
    /// \param overrides contains a bunch of property overrides like
    ///        non-standard SSL port and alternative location of the
    ///        truststore
    /// \return future that returns initialized configuration
    static ss::future<s3_configuration> make_configuration(
      const std::optional<cloud_roles::public_key_str>& pkey,
      const std::optional<cloud_roles::private_key_str>& skey,
      const cloud_roles::aws_region_name& region,
      const default_overrides& overrides = {},
      net::metrics_disabled disable_metrics = net::metrics_disabled::yes,
      net::public_metrics_disabled disable_public_metrics
      = net::public_metrics_disabled::yes);

    friend std::ostream& operator<<(std::ostream& o, const s3_configuration& c);
};

struct abs_configuration : common_configuration {
    cloud_roles::storage_account storage_account_name;
    std::optional<cloud_roles::private_key_str> shared_key;
    bool is_hns_enabled{false};

    abs_configuration make_adls_configuration() const;

    static ss::future<abs_configuration> make_configuration(
      const std::optional<cloud_roles::private_key_str>& shared_key,
      const cloud_roles::storage_account& storage_account_name,
      const default_overrides& overrides = {},
      net::metrics_disabled disable_metrics = net::metrics_disabled::yes,
      net::public_metrics_disabled disable_public_metrics
      = net::public_metrics_disabled::yes);

    friend std::ostream&
    operator<<(std::ostream& o, const abs_configuration& c);
};

template<typename T>
concept storage_client_configuration
  = std::is_base_of_v<common_configuration, T>;

template<storage_client_configuration... Ts>
using client_configuration_variant = std::variant<Ts...>;

using client_configuration
  = client_configuration_variant<abs_configuration, s3_configuration>;

std::ostream& operator<<(std::ostream&, const client_configuration&);

struct abs_self_configuration_result {
    bool is_hns_enabled;
};

struct s3_self_configuration_result {};

using client_self_configuration_output
  = std::variant<abs_self_configuration_result, s3_self_configuration_result>;

void apply_self_configuration_result(
  client_configuration&, const client_self_configuration_output&);

std::ostream& operator<<(std::ostream&, const abs_self_configuration_result&);
std::ostream& operator<<(std::ostream&, const s3_self_configuration_result&);
std::ostream&
operator<<(std::ostream&, const client_self_configuration_output&);

model::cloud_storage_backend infer_backend_from_configuration(
  const client_configuration& client_config,
  model::cloud_credentials_source cloud_storage_credentials_source);

} // namespace cloud_storage_clients

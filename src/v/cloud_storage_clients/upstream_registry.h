/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#pragma once

#include "base/format_to.h"
#include "cloud_storage_clients/configuration.h"
#include "cloud_storage_clients/detail/registry.h"
#include "cloud_storage_clients/upstream.h"

#include <seastar/core/sharded.hh>

namespace cloud_storage_clients {

/// A reasonable upper limit on the number of upstreams.
constexpr size_t max_upstreams = 10;

/// Key identifying an upstream service instance.
struct upstream_key {
    auto operator<=>(const upstream_key&) const = default;

    fmt::iterator format_to(fmt::iterator it) const {
        return fmt::format_to(it, "upstream_key{{}}");
    }
};

/// Registry for upstream cloud storage clients.
class upstream_registry final
  : public detail::basic_registry<upstream, upstream_key, upstream_registry>
  , public ss::peering_sharded_service<upstream_registry> {
public:
    explicit upstream_registry(client_configuration config);

    ss::future<> start();

    ss::shared_ptr<client_probe> probe() const noexcept { return _probe; }

protected:
    ss::future<>
    start_svc(sharded_constructor& ctor, const upstream_key&) final;

private:
    client_configuration _config;
    ss::shared_ptr<ss::tls::certificate_credentials> _tls_credentials;
    ss::shared_ptr<client_probe> _probe;
};

extern template class detail::
  basic_registry<upstream, upstream_key, upstream_registry>;

} // namespace cloud_storage_clients

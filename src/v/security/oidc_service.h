/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#pragma once

#include "base/outcome.h"
#include "config/property.h"
#include "security/fwd.h"

#include <chrono>
#include <memory>
#include <optional>
#include <string_view>

namespace security::oidc {

/// \brief Manage interaction with an OIDC Identity Provider
class service {
public:
    service(
      config::binding<std::vector<ss::sstring>> sasl_mechanisms,
      config::binding<std::vector<ss::sstring>> http_authentication,
      config::binding<ss::sstring> discovery_url,
      config::binding<ss::sstring> token_audience,
      config::binding<std::chrono::seconds> clock_skew_tolerance,
      config::binding<ss::sstring> mapping,
      config::binding<std::chrono::seconds> jwks_refresh_interval);
    service(service&&) = delete;
    service& operator=(service&&) = delete;
    service(const service&) = delete;
    service& operator=(const service&) = delete;
    ~service() noexcept;

    ss::future<> start();
    ss::future<> stop();

    const verifier& get_verifier() const;
    const principal_mapping_rule& get_principal_mapping_rule() const;
    std::string_view audience() const;
    result<std::string_view> issuer() const;
    std::chrono::seconds clock_skew_tolerance() const;

    ss::future<> refresh_keys();

private:
    struct impl;
    std::unique_ptr<impl> _impl;
};

} // namespace security::oidc

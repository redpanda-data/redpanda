/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "bytes/bytes.h"
#include "config/tls_config.h"
#include "metrics/metrics.h"
#include "seastarx.h"

#include <seastar/core/lowres_clock.hh>
#include <seastar/net/tls.hh>

#include <chrono>
#include <exception>
#include <iosfwd>

namespace net {

class tls_certificate_probe {
    using tls_serial_number
      = named_type<uint32_t, struct tls_serial_number_tag>;

public:
    using clock_type = ss::lowres_system_clock;
    tls_certificate_probe() { reset(); }
    tls_certificate_probe(const tls_certificate_probe&) = delete;
    tls_certificate_probe& operator=(const tls_certificate_probe&) = delete;
    tls_certificate_probe(tls_certificate_probe&&) = delete;
    tls_certificate_probe& operator=(tls_certificate_probe&&) = delete;
    ~tls_certificate_probe() = default;

    void loaded(
      const ss::tls::certificate_credentials& creds, std::exception_ptr ex);

    void setup_metrics(std::string_view area, std::string_view detail);

private:
    metrics::internal_metric_groups _metrics;
    metrics::public_metric_groups _public_metrics;
    clock_type::time_point _load_time{};
    clock_type::time_point _cert_expiry_time{clock_type::time_point::max()};
    clock_type::time_point _ca_expiry_time{clock_type::time_point::max()};
    tls_serial_number _cert_serial;
    tls_serial_number _ca_serial;
    bool _cert_loaded{false};

    bool cert_valid() const {
        auto now = clock_type::now();
        return _cert_loaded && now < _ca_expiry_time && now < _cert_expiry_time;
    }

    void reset() {
        _cert_loaded = false;
        _cert_expiry_time = clock_type::time_point::max();
        _ca_expiry_time = clock_type::time_point::max();
        _cert_serial = {};
        _ca_serial = {};
    }

    friend std::ostream&
    operator<<(std::ostream& o, const tls_certificate_probe& p);
};

ss::future<ss::shared_ptr<ss::tls::server_credentials>>
build_reloadable_server_credentials_with_probe(
  config::tls_config tls_config,
  ss::sstring service,
  ss::sstring listener_name,
  ss::tls::reload_callback cb = {});

template<typename T>
concept TLSCreds = std::is_base_of<ss::tls::certificate_credentials, T>::value;

template<TLSCreds T>
ss::future<ss::shared_ptr<T>> build_reloadable_credentials_with_probe(
  ss::tls::credentials_builder builder,
  ss::sstring area,
  ss::sstring detail,
  ss::tls::reload_callback cb = {});

}; // namespace net

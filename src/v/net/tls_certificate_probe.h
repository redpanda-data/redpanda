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

#include "base/seastarx.h"
#include "bytes/bytes.h"
#include "config/tls_config.h"
#include "metrics/metrics.h"
#include "utils/named_type.h"

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
      const ss::tls::certificate_credentials& creds,
      std::exception_ptr ex,
      std::optional<ss::tls::blob> trust_file_contents);

    void setup_metrics(std::string_view area, std::string_view detail);

private:
    struct cert {
        const clock_type::time_point expiry{clock_type::time_point::min()};
        const tls_serial_number serial{};
        bool expired(clock_type::time_point now) const { return expiry <= now; }
    };
    metrics::internal_metric_groups _metrics;
    metrics::public_metric_groups _public_metrics;
    clock_type::time_point _load_time{};
    std::optional<cert> _cert;
    std::optional<cert> _ca;
    bool _cert_loaded{false};
    uint32_t _trust_file_crc32c;

    bool cert_valid() const {
        auto now = clock_type::now();
        return (
          _cert_loaded //
          && (!_cert.value_or(cert{}).expired(now))
          && (!_ca.value_or(cert{}).expired(now)));
    }

    void reset() {
        _cert_loaded = false;
        _trust_file_crc32c = 0;
        _cert.reset();
        _ca.reset();
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

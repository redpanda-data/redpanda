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
#include "security/acl.h"
#include "security/gssapi_principal_mapper.h"
#include "security/sasl_authentication.h"

#include <seastar/core/lowres_clock.hh>

namespace ssx {
class singleton_thread_worker;
}

namespace security {

class gssapi_authenticator final : public sasl_mechanism {
    using clock_type = ss::lowres_clock;

public:
    enum class state { init = 0, more, ssfcap, ssfreq, complete, failed };
    static constexpr const char* name = "GSSAPI";

    gssapi_authenticator(
      ssx::singleton_thread_worker& thread_worker,
      std::vector<gssapi_rule> rules,
      ss::sstring principal,
      ss::sstring keytab);
    ~gssapi_authenticator() override;

    ss::future<result<bytes>> authenticate(bytes) override;

    bool complete() const override { return _state == state::complete; }
    bool failed() const override { return _state == state::failed; }

    const security::acl_principal& principal() const override {
        return _principal;
    }

    std::optional<std::chrono::milliseconds>
    credential_expires_in_ms() const override {
        if (_session_expiry.has_value()) {
            return std::chrono::duration_cast<std::chrono::milliseconds>(
              _session_expiry.value() - clock_type::now());
        }
        return std::nullopt;
    }

    const audit::user& audit_user() const override { return _audit_user; }

    const char* mechanism_name() const override { return name; }

private:
    friend std::ostream&
    operator<<(std::ostream& os, const gssapi_authenticator::state s);

    ssx::singleton_thread_worker& _worker;
    security::acl_principal _principal;
    std::optional<clock_type::time_point> _session_expiry;
    audit::user _audit_user;
    state _state{state::init};
    class impl;
    std::unique_ptr<impl> _impl;
};

} // namespace security

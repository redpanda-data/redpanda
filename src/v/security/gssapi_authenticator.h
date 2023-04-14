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
#include "ssx/fwd.h"

namespace security {

class gssapi_authenticator final : public sasl_mechanism {
public:
    enum class state { init = 0, more, ssfcap, ssfreq, complete, failed };
    static constexpr const char* name = "GSSAPI";

    gssapi_authenticator(
      ssx::thread_worker& thread_worker,
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

private:
    friend std::ostream&
    operator<<(std::ostream& os, gssapi_authenticator::state const s);

    ssx::thread_worker& _worker;
    security::acl_principal _principal;
    state _state{state::init};
    class impl;
    std::unique_ptr<impl> _impl;
};

} // namespace security

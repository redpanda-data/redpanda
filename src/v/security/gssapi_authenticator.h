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
#include "security/credential_store.h"
#include "security/gssapi.h"
#include "security/gssapi_principal_mapper.h"
#include "security/logger.h"
#include "security/sasl_authentication.h"
#include "security/scram_algorithm.h"
#include "security/types.h"
#include "ssx/thread_worker.h"
#include "utils/mutex.h"

namespace security {

class gssapi_authenticator final : public sasl_mechanism {
    enum class state { init = 0, more, ssfcap, ssfreq, complete, failed };
    friend std::ostream&
    operator<<(std::ostream& os, gssapi_authenticator::state const s);

public:
    static constexpr const char* name = "GSSAPI";

    gssapi_authenticator(
      ssx::thread_worker& thread_worker,
      config::binding<std::vector<ss::sstring>> cb)
      : _worker{thread_worker}
      , _gssapi_principal_mapper(std::move(cb)) {}

    ss::future<result<bytes>> authenticate(bytes) override;

    bool complete() const override { return _state == state::complete; }
    bool failed() const override { return _state == state::failed; }

    const security::acl_principal& principal() const override {
        return _principal;
    }

private:
    result<void> init(ss::sstring principal, ss::sstring keytab);
    result<bytes> more(bytes_view);
    result<bytes> ssfcap(bytes_view);
    result<bytes> ssfreq(bytes_view);
    result<void> check();
    void finish();
    void
    fail_impl(OM_uint32 maj_stat, OM_uint32 min_stat, std::string_view msg);
    template<typename... Args>
    void fail(
      OM_uint32 maj_stat,
      OM_uint32 min_stat,
      fmt::format_string<Args...> format_str,
      Args&&... args) {
        auto msg = ssx::sformat(format_str, std::forward<Args>(args)...);
        fail_impl(maj_stat, min_stat, msg);
    }
    acl_principal get_principal_from_name(std::string_view source_name);

    ssx::thread_worker& _worker;
    security::gssapi_principal_mapper _gssapi_principal_mapper;
    security::acl_principal _principal;
    state _state{state::init};
    gss::cred_id _server_creds;
    gss::ctx_id _context;
};

} // namespace security

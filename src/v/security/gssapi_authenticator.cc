/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#include "security/gssapi_authenticator.h"

#include "bytes/bytes.h"
#include "config/configuration.h"
#include "kafka/protocol/request_reader.h"
#include "security/acl.h"
#include "security/errc.h"
#include "security/gssapi.h"
#include "security/logger.h"
#include "vlog.h"

#include <boost/outcome/basic_outcome.hpp>
#include <boost/outcome/success_failure.hpp>
#include <fmt/ranges.h>
#include <gssapi/gssapi.h>
#include <gssapi/gssapi_ext.h>

#include <array>
#include <sstream>
#include <string_view>

namespace security {

std::ostream&
operator<<(std::ostream& os, gssapi_authenticator::state const s) {
    using state = gssapi_authenticator::state;
    switch (s) {
    case state::init:
        return os << "init";
    case state::more:
        return os << "more";
    case state::ssfcap:
        return os << "ssfcap";
    case state::ssfreq:
        return os << "ssfreq";
    case state::complete:
        return os << "complete";
    case state::failed:
        return os << "failed";
    }
}

static void display_status_1(std::string_view m, OM_uint32 code, int type) {
    while (true) {
        OM_uint32 msg_ctx{};
        OM_uint32 min_stat{};
        gss::buffer msg;
        auto maj_stat = gss_display_status(
          &min_stat, code, type, GSS_C_NO_OID, &msg_ctx, &msg);
        if (maj_stat != GSS_S_COMPLETE) {
            vlog(seclog.info, "gss status from {}", m);
            break;
        } else {
            vlog(seclog.info, "GSS_API error {}: {}", m, msg);
        }

        if (!msg_ctx) {
            break;
        }
    }
}

std::string display_ctx_flags(OM_uint32 flags) {
    std::stringstream ss;

    if (flags & GSS_C_DELEG_FLAG) ss << "GSS_C_DELEG_FLAG, ";
    if (flags & GSS_C_MUTUAL_FLAG) ss << "GSS_C_MUTUAL_FLAG, ";
    if (flags & GSS_C_REPLAY_FLAG) ss << "GSS_C_REPLAY_FLAG, ";
    if (flags & GSS_C_SEQUENCE_FLAG) ss << "GSS_C_SEQUENCE_FLAG, ";
    if (flags & GSS_C_CONF_FLAG) ss << "GSS_C_CONF_FLAG, ";
    if (flags & GSS_C_INTEG_FLAG) ss << "GSS_C_INTEG_FLAG, ";
    auto str = ss.str();
    if (str.length() > 2) {
        str.resize(str.length() - 2);
    }
    return str;
}

/*
 * Function: display_status
 *
 * Purpose: displays GSS-API messages
 *
 * Arguments:
 *
 *      msg             a string to be displayed with the message
 *      maj_stat        the GSS-API major status code
 *      min_stat        the GSS-API minor status code
 *
 * Effects:
 *
 * The GSS-API messages associated with maj_stat and min_stat are
 * displayed on stderr, each preceeded by "GSS-API error <msg>:
" and
 * followed by a newline.
 */
void display_status(
  std::string_view msg, OM_uint32 maj_stat, OM_uint32 min_stat) {
    display_status_1(msg, maj_stat, GSS_C_GSS_CODE);
    display_status_1(msg, min_stat, GSS_C_MECH_CODE);
}

ss::future<result<bytes>> gssapi_authenticator::authenticate(bytes auth_bytes) {
    vlog(
      seclog.trace,
      "gss {} authenticate received {} bytes",
      _state,
      auth_bytes.size());

    switch (_state) {
    case state::init: {
        auto principal = config::shard_local_cfg().sasl_kerberos_principal();
        auto keytab = config::shard_local_cfg().sasl_kerberos_keytab();

        auto res = co_await _worker.submit(
          [this,
           principal{std::move(principal)},
           keytab{std::move(keytab)}]() mutable {
              return init(std::move(principal), std::move(keytab));
          });
        if (res.has_error()) {
            co_return res.assume_error();
        }
    }
        [[fallthrough]];
    case state::more: {
        co_return co_await _worker.submit(
          [this, auth_bytes]() { return more(auth_bytes); });
    }
    case state::ssfcap: {
        co_return co_await _worker.submit(
          [this, auth_bytes]() { return ssfcap(auth_bytes); });
    }
    case state::ssfreq: {
        co_return co_await _worker.submit(
          [this, auth_bytes]() { return ssfreq(auth_bytes); });
    }
    case state::complete:
    case state::failed:
        break;
    }

    fail(0, 0, "gss {} authenticate failed", _state);
    co_return errc::invalid_gssapi_state;
}

result<void>
gssapi_authenticator::init(ss::sstring principal, ss::sstring keytab) {
    OM_uint32 minor_status{};
    gss::buffer_view service_name{principal};
    gss::name server_name{};

    auto major_status = ::gss_import_name(
      &minor_status, &service_name, GSS_C_NT_HOSTBASED_SERVICE, &server_name);

    if (major_status != GSS_S_COMPLETE) {
        fail(
          major_status,
          minor_status,
          "gss {} failed to import service principal {}",
          _state,
          principal);
        return errc::invalid_credentials;
    }

    gss_key_value_element_desc elem{.key = "keytab", .value = keytab.c_str()};
    gss_key_value_set_desc store{.count = 1, .elements = &elem};

    major_status = ::gss_acquire_cred_from(
      &minor_status,
      server_name,
      0,
      GSS_C_NO_OID_SET,
      GSS_C_ACCEPT,
      &store,
      &_server_creds,
      NULL,
      NULL);

    if (major_status != GSS_S_COMPLETE) {
        fail(
          major_status,
          minor_status,
          "gss {} failed to acquire credentials for principal {} in keytab {}",
          _state,
          principal,
          keytab);
        return errc::invalid_credentials;
    }

    _state = state::more;
    return outcome::success();
}

result<bytes> gssapi_authenticator::more(bytes_view auth_bytes) {
    gss::buffer_view recv_tok{auth_bytes};
    gss::buffer send_tok;
    OM_uint32 minor_status{};
    OM_uint32 ret_flags{};
    gss_OID oid;
    gss::name client_name;

    auto major_status = ::gss_accept_sec_context(
      &minor_status,
      &_context,
      _server_creds,
      &recv_tok,
      GSS_C_NO_CHANNEL_BINDINGS,
      &client_name,
      &oid,
      &send_tok,
      &ret_flags,
      nullptr,  /* ignore time_rec */
      nullptr); /* ignore del_cred_handle */

    if (
      major_status != GSS_S_COMPLETE && major_status != GSS_S_CONTINUE_NEEDED) {
        fail(
          major_status,
          minor_status,
          "gss {} failed to accept security context",
          _state);
        return errc::invalid_credentials;
    }

    bytes ret{bytes_view{send_tok}};
    vlog(seclog.trace, "gss {} sending {} bytes", _state, ret.size());

    if (major_status == GSS_S_COMPLETE) {
        _state = state::ssfcap;
    } else if (major_status == GSS_S_CONTINUE_NEEDED) {
        _state = state::more;
    }

    return ret;
}

result<bytes> gssapi_authenticator::ssfcap(bytes_view auth_bytes) {
    if (!auth_bytes.empty()) {
        fail(
          0,
          0,
          "gss {} expected empty response but got {} bytes",
          _state,
          auth_bytes.size());
        return errc::invalid_credentials;
    }

    OM_uint32 minor_status{};
    gss::buffer_set bufset;
    auto major_status = gss_inquire_sec_context_by_oid(
      &minor_status, _context, GSS_C_SEC_CONTEXT_SASL_SSF, &bufset);
    if (major_status != GSS_S_COMPLETE) {
        fail(
          major_status, minor_status, "gss {} failed to inquire ssf", _state);
        return errc::invalid_credentials;
    }
    if ((bufset.size() != 1) || (bufset[0].size() != 4)) {
        fail(0, 0, "gss {} unexpected data in ssf", _state);
        return errc::invalid_credentials;
    }

    uint32_t ssf{};
    memcpy(&ssf, bufset[0].value(), sizeof(ssf));
    auto mech_ssf = ntohl(ssf);
    vlog(seclog.trace, "gss {} mech_ssf: {}", _state, mech_ssf);

    bytes sasl_data{0x1, 0x0, 0x0, 0xff};
    gss::buffer_view input{sasl_data};
    gss::buffer output_token;
    major_status = ::gss_wrap(
      &minor_status,
      _context,
      1,
      GSS_C_QOP_DEFAULT,
      &input,
      nullptr,
      &output_token);
    if (major_status != GSS_S_COMPLETE) {
        fail(
          major_status,
          minor_status,
          "gss {} failed to wrap ssf",
          major_status,
          minor_status);
        return errc::invalid_credentials;
    }

    vlog(seclog.trace, "gss {} sending {} bytes", _state, output_token.size());
    _state = state::ssfreq;
    return bytes{bytes_view{output_token}};
}

result<bytes> gssapi_authenticator::ssfreq(bytes_view auth_bytes) {
    gss::buffer_view input_token{auth_bytes};
    gss::buffer output_token{};
    OM_uint32 minor_status{};
    auto major_status = gss_unwrap(
      &minor_status, _context, &input_token, &output_token, nullptr, nullptr);
    if (major_status != GSS_S_COMPLETE) {
        fail(
          major_status,
          minor_status,
          "gss {} failed to unwrap ssf of {} bytes",
          _state,
          auth_bytes.size());
        return errc::invalid_credentials;
    }

    if (output_token.size() < 4) {
        fail(
          0,
          0,
          "gss {} unexpected data in unwrapped result of {} bytes",
          _state,
          output_token.size());
    }

    if (auto res = check(); res.has_error()) {
        return res.assume_error();
    }

    bytes ret{};
    vlog(seclog.trace, "gss {} sending {} bytes", _state, ret.size());
    finish();
    return ret;
}

result<void> gssapi_authenticator::check() {
    OM_uint32 major_status{};
    OM_uint32 minor_status{};
    OM_uint32 lifetime_rec{};
    gss::name source;
    gss::name target;
    gss_OID mech{};
    OM_uint32 ctx_flags{};
    int open{};

    major_status = ::gss_inquire_context(
      &minor_status,
      _context,
      &source,
      &target,
      &lifetime_rec,
      &mech,
      &ctx_flags,
      nullptr,
      &open);
    if (major_status != GSS_S_COMPLETE) {
        fail(
          major_status,
          minor_status,
          "gss {} failed to inquire context",
          _state);
        return errc::invalid_scram_state;
    }

    auto target_buf = target.display_name_buffer();
    std::string_view target_name{target_buf};
    if (target_name.empty()) {
        fail(0, 0, "gss {} failed to get service principal", _state);
        return errc::invalid_scram_state;
    }

    auto source_buf = source.display_name_buffer();
    std::string_view source_name{source_buf};
    if (source_name.empty()) {
        fail(0, 0, "gss {} failed to get client principal", _state);
        return errc::invalid_scram_state;
    }

    vlog(
      seclog.debug,
      "gss_inquire_context: source: {}, target: {}, lifetime_rec: {}, "
      "ctx_flags: {}, open: {}",
      source_name,
      target_name,
      lifetime_rec,
      display_ctx_flags(ctx_flags),
      open);

    // None of this is needed - left here temporarily for posterity
    // if ((ctx_flags & GSS_C_INTEG_FLAG) == 0) {
    //     // No integrity
    //     _state = state::failed;
    //     return errc::invalid_credentials;
    // } else if ((ctx_flags & GSS_C_CONF_FLAG) == 0) {
    //     // No confidentiality
    //     _state = state::failed;
    //     return errc::invalid_credentials;
    // }

    _principal = acl_principal{principal_type::user, ss::sstring{source_name}};
    return outcome::success();
}

void gssapi_authenticator::finish() {
    _context.reset();
    _server_creds.reset();
    _state = state::complete;
}

void gssapi_authenticator::fail_impl(
  OM_uint32 maj_stat, OM_uint32 min_stat, std::string_view msg) {
    if (maj_stat != 0 || min_stat != 0) {
        display_status(msg, maj_stat, min_stat);
    } else {
        vlog(seclog.info, "{}", msg);
    }
    _state = state::failed;
}

} // namespace security

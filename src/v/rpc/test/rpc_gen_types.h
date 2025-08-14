/*
 * Copyright 2020 Redpanda Data, Inc.
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
#include "reflection/adl.h"
#include "rpc/parse_utils.h"
#include "serde/envelope.h"
#include "serde/rw/enum.h"
#include "serde/rw/envelope.h"
#include "serde/rw/rw.h"
#include "serde/rw/scalar.h"
#include "serde/rw/sstring.h"

#include <seastar/core/sstring.hh>

#include <cstdint>

namespace cycling {
struct ultimate_cf_slx
  : serde::
      envelope<ultimate_cf_slx, serde::version<0>, serde::compat_version<0>> {
    ultimate_cf_slx() = default;
    ultimate_cf_slx(int x)
      : x(x) {}
    int x = 42;
    auto serde_fields() { return std::tie(x); }
};
struct nairo_quintana
  : serde::
      envelope<nairo_quintana, serde::version<0>, serde::compat_version<0>> {
    nairo_quintana() = default;
    nairo_quintana(int x)
      : x(x) {}
    int x = 43;
    auto serde_fields() { return std::tie(x); }
};
struct san_francisco
  : serde::
      envelope<san_francisco, serde::version<0>, serde::compat_version<0>> {
    san_francisco() = default;
    san_francisco(int x)
      : x(x) {}
    int x = 44;
    auto serde_fields() { return std::tie(x); }
};
struct mount_tamalpais
  : serde::
      envelope<mount_tamalpais, serde::version<0>, serde::compat_version<0>> {
    mount_tamalpais() = default;
    mount_tamalpais(int x)
      : x(x) {}
    int x = 45;
    auto serde_fields() { return std::tie(x); }
};
} // namespace cycling

namespace echo {
struct echo_req
  : serde::envelope<echo_req, serde::version<0>, serde::compat_version<0>> {
    ss::sstring str;
    auto serde_fields() { return std::tie(str); }
};

struct echo_resp
  : serde::envelope<echo_req, serde::version<0>, serde::compat_version<0>> {
    ss::sstring str;
    auto serde_fields() { return std::tie(str); }
};

struct cnt_req
  : serde::envelope<echo_req, serde::version<0>, serde::compat_version<0>> {
    uint64_t expected;
    auto serde_fields() { return std::tie(expected); }
};

struct cnt_resp
  : serde::envelope<echo_req, serde::version<0>, serde::compat_version<0>> {
    uint64_t expected;
    uint64_t current;
    auto serde_fields() { return std::tie(expected, current); }
};

struct sleep_req
  : serde::envelope<echo_req, serde::version<0>, serde::compat_version<0>> {
    uint64_t secs;
    auto serde_fields() { return std::tie(secs); }
};

struct sleep_resp
  : serde::envelope<echo_req, serde::version<0>, serde::compat_version<0>> {
    ss::sstring str;
    auto serde_fields() { return std::tie(str); }
};

enum class failure_type { throw_exception, exceptional_future, none };

struct throw_req
  : serde::envelope<echo_req, serde::version<0>, serde::compat_version<0>> {
    failure_type type;
    auto serde_fields() { return std::tie(type); }
};

struct throw_resp
  : serde::envelope<echo_req, serde::version<0>, serde::compat_version<0>> {
    ss::sstring reply;

    auto serde_fields() { return std::tie(reply); }
};

struct echo_req_serde_only
  : serde::envelope<
      echo_req_serde_only,
      serde::version<1>,
      serde::compat_version<1>> {
    ss::sstring str;

    void serde_write(iobuf& out) const {
        // serialize with serde a serde-only type
        using serde::write;
        write(out, str + "_to_sso");
    }

    void serde_read(iobuf_parser& in, const serde::header& h) {
        // deserialize with serde a serde-only type
        using serde::read_nested;
        str = read_nested<ss::sstring>(in, h._bytes_left_limit);
        str += "_from_sso";
    }
};

struct echo_resp_serde_only
  : serde::envelope<
      echo_resp_serde_only,
      serde::version<1>,
      serde::compat_version<1>> {
    ss::sstring str;

    void serde_write(iobuf& out) const {
        // serialize with serde a serde-only type
        using serde::write;
        write(out, str + "_to_sso");
    }

    void serde_read(iobuf_parser& in, const serde::header& h) {
        // deserialize with serde a serde-only type
        using serde::read_nested;
        str = read_nested<ss::sstring>(in, h._bytes_left_limit);
        str += "_from_sso";
    }
};

} // namespace echo

namespace echo_v2 {

/// This type is meant to be the evolution of the echo_req_serde_only type
/// defined in `rpc_gen_types.h`, the issue being that a redefinition
/// of a new type with a different parent class and new fields cannot be done
/// within the same binary/library.
///
/// To get around this, this new type is defined which contains all of the
/// desired changes desired for the type evolution of echo_req_serde_only, and
/// tests will use raw `send_typed<req, resp>` when making requests to rpc
/// servers
struct echo_req
  : serde::envelope<echo_req, serde::version<2>, serde::compat_version<1>> {
    ss::sstring str;
    ss::sstring str_two;

    void serde_write(iobuf& out) const {
        // serialize with serde a serde-only type
        using serde::write;
        write(out, str + "_to_sso_v2");
        write(out, str_two + "_to_sso_v2");
    }

    void serde_read(iobuf_parser& in, const serde::header& h) {
        // deserialize with serde a serde-only type
        using serde::read_nested;
        str = read_nested<ss::sstring>(in, h._bytes_left_limit);
        str += "_from_sso_v2";
        if (h._version >= static_cast<serde::version_t>(2)) {
            str_two = read_nested<ss::sstring>(in, h._bytes_left_limit);
            str_two += "_from_sso_v2";
        }
    }
};

struct echo_resp
  : serde::envelope<echo_resp, serde::version<2>, serde::compat_version<1>> {
    ss::sstring str;
    ss::sstring str_two;

    void serde_write(iobuf& out) const {
        // serialize with serde a serde-only type
        using serde::write;
        write(out, str + "_to_sso_v2");
        write(out, str_two + "_to_sso_v2");
    }

    void serde_read(iobuf_parser& in, const serde::header& h) {
        // deserialize with serde a serde-only type
        using serde::read_nested;
        str = read_nested<ss::sstring>(in, h._bytes_left_limit);
        str += "_from_sso_v2";
        if (h._version >= static_cast<serde::version_t>(2)) {
            str_two = read_nested<ss::sstring>(in, h._bytes_left_limit);
            str_two += "_from_sso_v2";
        }
    }
};

} // namespace echo_v2

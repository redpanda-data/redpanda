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

#include "reflection/adl.h"
#include "rpc/parse_utils.h"
#include "seastarx.h"
#include "serde/envelope.h"
#include "serde/serde.h"

#include <seastar/core/sstring.hh>

#include <cstdint>

namespace cycling {
struct ultimate_cf_slx {
    using rpc_serde_exempt = std::true_type;
    int x = 42;
};
struct nairo_quintana {
    using rpc_serde_exempt = std::true_type;
    int x = 43;
};
struct san_francisco {
    using rpc_serde_exempt = std::true_type;
    int x = 44;
};
struct mount_tamalpais {
    using rpc_serde_exempt = std::true_type;
    int x = 45;
};
} // namespace cycling

namespace echo {
struct echo_req
  : serde::envelope<echo_req, serde::version<0>, serde::compat_version<0>> {
    using rpc_adl_exempt = std::true_type;
    ss::sstring str;
};

struct echo_resp
  : serde::envelope<echo_req, serde::version<0>, serde::compat_version<0>> {
    using rpc_adl_exempt = std::true_type;
    ss::sstring str;
};

static_assert(serde::is_serde_compatible_v<echo_req>);
static_assert(serde::is_serde_compatible_v<echo_resp>);
static_assert(rpc::is_rpc_adl_exempt<echo_req>);
static_assert(rpc::is_rpc_adl_exempt<echo_resp>);

struct cnt_req
  : serde::envelope<echo_req, serde::version<0>, serde::compat_version<0>> {
    using rpc_adl_exempt = std::true_type;
    uint64_t expected;
};

struct cnt_resp
  : serde::envelope<echo_req, serde::version<0>, serde::compat_version<0>> {
    using rpc_adl_exempt = std::true_type;
    uint64_t expected;
    uint64_t current;
};

struct sleep_req
  : serde::envelope<echo_req, serde::version<0>, serde::compat_version<0>> {
    using rpc_adl_exempt = std::true_type;
    uint64_t secs;
};

struct sleep_resp
  : serde::envelope<echo_req, serde::version<0>, serde::compat_version<0>> {
    using rpc_adl_exempt = std::true_type;
    ss::sstring str;
};

enum class failure_type { throw_exception, exceptional_future, none };

struct throw_req
  : serde::envelope<echo_req, serde::version<0>, serde::compat_version<0>> {
    using rpc_adl_exempt = std::true_type;
    failure_type type;
};

struct throw_resp
  : serde::envelope<echo_req, serde::version<0>, serde::compat_version<0>> {
    using rpc_adl_exempt = std::true_type;
    ss::sstring reply;
};

struct echo_req_serde_only
  : serde::envelope<
      echo_req_serde_only,
      serde::version<1>,
      serde::compat_version<1>> {
    using rpc_adl_exempt = std::true_type;
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
    using rpc_adl_exempt = std::true_type;
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

// serde-only type needs to have serde support
static_assert(serde::is_serde_compatible_v<echo_req_serde_only>);
static_assert(serde::is_serde_compatible_v<echo_resp_serde_only>);

// serde-only type needs to be example from adl
static_assert(rpc::is_rpc_adl_exempt<echo_req_serde_only>);
static_assert(rpc::is_rpc_adl_exempt<echo_resp_serde_only>);

} // namespace echo

namespace echo_v2 {

/// This type is meant to be the evolution of the echo_req_serde_only type
/// defined in `rpc/test/rpc_gen_types.h`, the issue being that a redefinition
/// of a new type with a different parent class and new fields cannot be done
/// within the same binary/library.
///
/// To get around this, this new type is defined which contains all of the
/// desired changes desired for the type evolution of echo_req_serde_only, and
/// tests will use raw `send_typed<req, resp>` when making requests to rpc
/// servers
struct echo_req
  : serde::envelope<echo_req, serde::version<2>, serde::compat_version<1>> {
    using rpc_adl_exempt = std::true_type;
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
    using rpc_adl_exempt = std::true_type;
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

static_assert(serde::is_serde_compatible_v<echo_req>);
static_assert(rpc::is_rpc_adl_exempt<echo_req>);

static_assert(rpc::is_rpc_adl_exempt<echo_req>);
static_assert(rpc::is_rpc_adl_exempt<echo_resp>);

} // namespace echo_v2

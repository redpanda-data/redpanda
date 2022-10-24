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
struct echo_req {
    using rpc_serde_exempt = std::true_type;
    ss::sstring str;
};

struct echo_resp {
    using rpc_serde_exempt = std::true_type;
    ss::sstring str;
};

struct cnt_req {
    using rpc_serde_exempt = std::true_type;
    uint64_t expected;
};

struct cnt_resp {
    using rpc_serde_exempt = std::true_type;
    uint64_t expected;
    uint64_t current;
};

struct sleep_req {
    using rpc_serde_exempt = std::true_type;
    uint64_t secs;
};

struct sleep_resp {
    using rpc_serde_exempt = std::true_type;
    ss::sstring str;
};

enum class failure_type { throw_exception, exceptional_future, none };

struct throw_req {
    using rpc_serde_exempt = std::true_type;
    failure_type type;
};

struct throw_resp {
    using rpc_serde_exempt = std::true_type;
    ss::sstring reply;
};

/*
 * echo methods with req/resp that support encodings:
 * - adl only
 * - serde only
 * - serde and adl
 */
struct echo_req_adl_only {
    using rpc_serde_exempt = std::true_type;
    ss::sstring str;
};

struct echo_resp_adl_only {
    using rpc_serde_exempt = std::true_type;
    ss::sstring str;
};

// an adl-only type should not have serde support
static_assert(!serde::is_serde_compatible_v<echo_req_adl_only>);
static_assert(!serde::is_serde_compatible_v<echo_resp_adl_only>);

// an adl-only type should not be exempt from adl support
static_assert(!rpc::is_rpc_adl_exempt<echo_req_adl_only>);
static_assert(!rpc::is_rpc_adl_exempt<echo_resp_adl_only>);

struct echo_req_adl_serde
  : serde::envelope<echo_req_adl_serde, serde::version<1>> {
    ss::sstring str;

    void serde_write(iobuf& out) const {
        // serialize with serde an adl-serde type
        using serde::write;
        write(out, str + "_to_sas");
    }

    void serde_read(iobuf_parser& in, const serde::header& h) {
        // deserialize with serde an adl-serde type
        using serde::read_nested;
        str = read_nested<ss::sstring>(in, h._bytes_left_limit);
        str += "_from_sas";
    }
};

struct echo_resp_adl_serde
  : serde::envelope<echo_resp_adl_serde, serde::version<1>> {
    ss::sstring str;

    void serde_write(iobuf& out) const {
        // serialize with serde an adl-serde type
        using serde::write;
        write(out, str + "_to_sas");
    }

    void serde_read(iobuf_parser& in, const serde::header& h) {
        // deserialize with serde an adl-serde type
        using serde::read_nested;
        str = read_nested<ss::sstring>(in, h._bytes_left_limit);
        str += "_from_sas";
    }
};

static_assert(serde::is_serde_compatible_v<echo_req_adl_serde>);
static_assert(serde::is_serde_compatible_v<echo_resp_adl_serde>);
static_assert(!rpc::is_rpc_adl_exempt<echo_req_adl_serde>);
static_assert(!rpc::is_rpc_adl_exempt<echo_resp_adl_serde>);

struct echo_req_serde_only
  : serde::envelope<echo_req_serde_only, serde::version<1>> {
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
  : serde::envelope<echo_resp_serde_only, serde::version<1>> {
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

namespace reflection {
template<>
struct adl<echo::echo_req_adl_only> {
    void to(iobuf& out, echo::echo_req_adl_only&& r) {
        // serialize with adl an adl-only type
        reflection::serialize(out, r.str + "_to_aao");
    }
    echo::echo_req_adl_only from(iobuf_parser& in) {
        // deserialize with adl an adl-only type
        return echo::echo_req_adl_only{
          .str = adl<ss::sstring>{}.from(in) + "_from_aao",
        };
    }
};

template<>
struct adl<echo::echo_resp_adl_only> {
    void to(iobuf& out, echo::echo_resp_adl_only&& r) {
        // serialize with adl an adl-only type
        reflection::serialize(out, r.str + "_to_aao");
    }
    echo::echo_resp_adl_only from(iobuf_parser& in) {
        // deserialize with adl an adl-only type
        return echo::echo_resp_adl_only{
          .str = adl<ss::sstring>{}.from(in) + "_from_aao",
        };
    }
};

template<>
struct adl<echo::echo_req_adl_serde> {
    void to(iobuf& out, echo::echo_req_adl_serde&& r) {
        // serialize with adl an adl-serde type
        reflection::serialize(out, r.str + "_to_aas");
    }
    echo::echo_req_adl_serde from(iobuf_parser& in) {
        // deserialize with adl an adl-serde type
        return echo::echo_req_adl_serde{
          .str = adl<ss::sstring>{}.from(in) + "_from_aas",
        };
    }
};

template<>
struct adl<echo::echo_resp_adl_serde> {
    void to(iobuf& out, echo::echo_resp_adl_serde&& r) {
        // serialize with adl an adl-serde type
        reflection::serialize(out, r.str + "_to_aas");
    }
    echo::echo_resp_adl_serde from(iobuf_parser& in) {
        // deserialize with adl an adl-serde type
        return echo::echo_resp_adl_serde{
          .str = adl<ss::sstring>{}.from(in) + "_from_aas",
        };
    }
};
} // namespace reflection

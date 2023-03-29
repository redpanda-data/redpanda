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

#include "model/fundamental.h"
#include "serde/envelope.h"

#include <chrono>

namespace cluster {

struct snc_quota_balancer_lend_request
  : serde::envelope<
      snc_quota_balancer_lend_request,
      serde::version<0>,
      serde::compat_version<0>> {
    using rpc_adl_exempt = std::true_type;

    model::thoughput_bps quota_ask{0};

    auto serde_fields() { return std::tie(quota_ask); }

    friend std::ostream&
    operator<<(std::ostream& o, const snc_quota_balancer_lend_request&);
};

struct snc_quota_balancer_lend_reply
  : serde::envelope<
      snc_quota_balancer_lend_reply,
      serde::version<0>,
      serde::compat_version<0>> {
    using rpc_adl_exempt = std::true_type;
    using clock = std::chrono::system_clock;

    // unique id of the lease created at the lender node, used for debugging
    // purposes only => overflow is not a concern
    uint32_t lease_id{0};

    // quota amount leased. If the lender has not created a new lease, it is
    // zero. Other fields like `lease_id` are zero as well in this case,
    // however this one is the determinant
    model::thoughput_bps lease_amount{0};

    // non-extended duration of the lease created
    clock::duration lease_time;

    // extension time for all leases sourced from the lender. This field is
    // effective regardless of whether a new lease has been created or not
    clock::duration extension_time;

    auto serde_fields() {
        return std::tie(lease_id, lease_amount, lease_time, extension_time);
    }

    friend std::ostream&
    operator<<(std::ostream& o, const snc_quota_balancer_lend_reply&);
};

} // namespace cluster

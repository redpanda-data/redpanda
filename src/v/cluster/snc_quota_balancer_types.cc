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
#include "snc_quota_balancer_types.h"

#include "utils/human_chrono.h"

namespace cluster {

std::ostream&
operator<<(std::ostream& o, const snc_quota_balancer_lend_request& v) {
    fmt::print(o, "{{quota_ask: {}}}", v.quota_ask);
    return o;
}

std::ostream&
operator<<(std::ostream& o, const snc_quota_balancer_lend_reply& v) {
    fmt::print(
      o,
      "{{lease_id: {}, lease_amount: {}, lease_time: {}, extension_time: {}}}",
      v.lease_id,
      v.lease_amount,
      human::seconds_decimal(v.lease_time),
      human::seconds_decimal(v.extension_time));
    return o;
}

} // namespace cluster

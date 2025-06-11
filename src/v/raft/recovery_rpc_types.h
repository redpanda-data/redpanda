/*
 * Copyright 2025 Redpanda Data, Inc.
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
#include "serde/rw/bool_class.h"
#include "serde/rw/envelope.h"
#include "serde/rw/scalar.h"

#include <seastar/util/bool_class.hh>

#include <fmt/ostream.h>

#include <ostream>
#include <tuple>
#include <type_traits>

namespace raft {

struct reset_learner_state_request
  : serde::envelope<
      reset_learner_state_request,
      serde::version<0>,
      serde::compat_version<0>> {
    using rpc_adl_exempt = std::true_type;

    friend std::ostream&
    operator<<(std::ostream& o, const reset_learner_state_request& r) {
        fmt::print(o, "ntp: {}", r.ntp);
        return o;
    }

    auto serde_fields() { return std::tie(ntp); }

    model::ntp ntp;
};

struct reset_learner_state_reply
  : serde::envelope<
      reset_learner_state_reply,
      serde::version<0>,
      serde::compat_version<0>> {
    using rpc_adl_exempt = std::true_type;
    using is_success = ss::bool_class<struct reset_learner_state_tag>;

    friend std::ostream&
    operator<<(std::ostream& o, const reset_learner_state_reply& r) {
        fmt::print(o, "success: {}", r.success);
        return o;
    }

    auto serde_fields() { return std::tie(success); }

    is_success success = is_success::no;
};

} // namespace raft

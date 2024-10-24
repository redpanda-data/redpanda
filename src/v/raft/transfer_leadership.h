/*
 * Copyright 2024 Redpanda Data, Inc.
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
#include "raft/errc.h"
#include "raft/fundamental.h"
#include "serde/rw/chrono.h"
#include "serde/rw/enum.h"
#include "serde/rw/envelope.h"
#include "serde/rw/optional.h"
#include "serde/rw/scalar.h"

namespace raft {

// if not target is specified then the most up-to-date node will be selected
struct transfer_leadership_request
  : serde::envelope<
      transfer_leadership_request,
      serde::version<1>,
      serde::compat_version<0>> {
    using rpc_adl_exempt = std::true_type;
    group_id group;
    std::optional<model::node_id> target;
    std::optional<std::chrono::milliseconds> timeout;

    raft::group_id target_group() const { return group; }

    friend bool operator==(
      const transfer_leadership_request&, const transfer_leadership_request&)
      = default;

    auto serde_fields() { return std::tie(group, target, timeout); }

    friend std::ostream&
    operator<<(std::ostream& o, const transfer_leadership_request& r);
};

struct transfer_leadership_reply
  : serde::envelope<
      transfer_leadership_reply,
      serde::version<0>,
      serde::compat_version<0>> {
    using rpc_adl_exempt = std::true_type;
    bool success{false};
    raft::errc result;

    friend bool operator==(
      const transfer_leadership_reply&, const transfer_leadership_reply&)
      = default;

    auto serde_fields() { return std::tie(success, result); }

    friend std::ostream&
    operator<<(std::ostream& o, const transfer_leadership_reply& r) {
        fmt::print(o, "success {} result {}", r.success, r.result);
        return o;
    }
};

} // namespace raft

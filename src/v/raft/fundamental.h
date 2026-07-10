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

#include "base/format_to.h"
#include "base/seastarx.h"
#include "model/fundamental.h"
#include "serde/envelope.h"
#include "utils/named_type.h"

#include <seastar/core/lowres_clock.hh>

namespace raft {

using clock_type = ss::lowres_clock;
using duration_type = typename clock_type::duration;
using timer_type = ss::timer<clock_type>;
inline constexpr clock_type::time_point no_timeout
  = clock_type::time_point::max();

using group_id = named_type<int64_t, struct raft_group_id_type>;

enum class reply_result : uint8_t {
    success,
    failure,
    group_unavailable,
    follower_busy
};

inline fmt::iterator format_to(reply_result r, fmt::iterator out) {
    switch (r) {
    case reply_result::success:
        return fmt::format_to(out, "success");
    case reply_result::failure:
        return fmt::format_to(out, "failure");
    case reply_result::group_unavailable:
        return fmt::format_to(out, "group_unavailable");
    case reply_result::follower_busy:
        return fmt::format_to(out, "follower_busy");
    }
    __builtin_unreachable();
}

/**
 * Class representing single incarnation of a node being a member of Raft group.
 * This class allows Raft to recognize members with the same id coming from
 * different reconfiguration epochs.
 */
class vnode
  : public serde::envelope<vnode, serde::version<0>, serde::compat_version<0>> {
public:
    constexpr vnode() = default;

    constexpr vnode(model::node_id nid, model::revision_id rev)
      : _node_id(nid)
      , _revision(rev) {}

    bool operator==(const vnode& other) const = default;
    bool operator!=(const vnode& other) const = default;

    fmt::iterator format_to(fmt::iterator it) const {
        return fmt::format_to(
          it, "{{id: {}, revision: {}}}", _node_id, _revision);
    }

    template<typename H>
    friend H AbslHashValue(H h, const vnode& node) {
        return H::combine(std::move(h), node._node_id, node._revision);
    }

    constexpr model::node_id id() const { return _node_id; }
    constexpr model::revision_id revision() const { return _revision; }

    void serde_write(iobuf&) const;
    void serde_read(iobuf_parser&, const serde::header&);

private:
    model::node_id _node_id;
    model::revision_id _revision;
};

} // namespace raft

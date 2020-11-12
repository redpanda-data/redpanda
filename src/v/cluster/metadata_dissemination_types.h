/*
 * Copyright 2020 Vectorized, Inc.
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
#include "model/metadata.h"

#include <ostream>

namespace cluster {

struct ntp_leader {
    model::ntp ntp;
    model::term_id term;
    std::optional<model::node_id> leader_id;
};

using ntp_leaders = std::vector<ntp_leader>;

struct update_leadership_request {
    ntp_leaders leaders;
};

struct update_leadership_reply {};

struct get_leadership_request {};

struct get_leadership_reply {
    ntp_leaders leaders;
};

inline std::ostream& operator<<(std::ostream& o, const ntp_leader& l) {
    o << "{ " << l.ntp << ", term: " << l.term
      << ", leader_id: " << (l.leader_id ? l.leader_id.value()() : -1) << " }";
    return o;
}

} // namespace cluster
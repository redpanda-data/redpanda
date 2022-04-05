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
#include "bytes/iobuf_parser.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "reflection/adl.h"

#include <fmt/ostream.h>

#include <ostream>

namespace cluster {

struct ntp_leader {
    model::ntp ntp;
    model::term_id term;
    std::optional<model::node_id> leader_id;
};

struct ntp_leader_revision {
    model::ntp ntp;
    model::term_id term;
    std::optional<model::node_id> leader_id;
    model::revision_id revision;
    friend std::ostream&
    operator<<(std::ostream& o, const ntp_leader_revision& r) {
        fmt::print(
          o,
          "{{ntp: {}, term: {}, leader: {}, revision: {}}}",
          r.ntp,
          r.term,
          r.leader_id,
          r.revision);
        return o;
    }
};

using ntp_leaders = std::vector<ntp_leader>;

struct update_leadership_request {
    ntp_leaders leaders;
};

struct update_leadership_request_v2 {
    static constexpr int8_t version = 0;
    std::vector<ntp_leader_revision> leaders;
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

namespace reflection {
template<>
struct adl<cluster::update_leadership_request_v2> {
    void to(iobuf& out, cluster::update_leadership_request_v2&& req) {
        serialize(out, req.version, req.leaders);
    }
    cluster::update_leadership_request_v2 from(iobuf_parser& in) {
        // decode version
        adl<int8_t>{}.from(in);
        auto leaders = adl<std::vector<cluster::ntp_leader_revision>>{}.from(
          in);
        return cluster::update_leadership_request_v2{
          .leaders = std::move(leaders)};
    }
};
} // namespace reflection

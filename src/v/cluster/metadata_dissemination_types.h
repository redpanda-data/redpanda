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
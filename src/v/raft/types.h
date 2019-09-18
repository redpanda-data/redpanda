#pragma once

#include "model/fundamental.h"
#include "model/metadata.h"
#include "rpc/deserialize.h"
#include "rpc/serialize.h"
#include "utils/fragbuf.h"
#include "utils/named_type.h"

#include <seastar/net/socket_defs.hh>

#include <cstdint>

namespace raft {
using clock_type = lowres_clock;
using duration_type = typename clock_type::duration;
using timer_type = timer<clock_type>;

using group_id = named_type<int64_t, struct raft_group_id_type>;
using term_id = named_type<int64_t, struct raft_term_id_type>;

/// special case. it uses underlying type because it is the most used type
/// by using the underlying::type we save 8 continuations per deserialization
struct [[gnu::packed]] protocol_metadata {
    unaligned<group_id::type> group;
    /// \brief used to compare completeness
    unaligned<model::offset::type> commit_index;
    /// \brief current term
    unaligned<term_id::type> term;
    /// \brief used to compare completeness
    unaligned<term_id::type> prev_log_term;
};

struct group_configuration {
    model::node_id node_id;
    std::vector<model::broker> nodes;
    std::vector<model::broker> learners;
};

struct configuration_reply {};

struct append_entries_request {
    model::node_id node_id;
    protocol_metadata meta;
    std::vector<fragbuf> entries;
};

struct [[gnu::packed]] append_entries_reply {
    /// \brief callee's term, for the caller to upate itself
    unaligned<term_id::type> term;
    /// \brief The recipient's last log index after it applied changes to
    /// the log. This is used to speed up finding the correct value for the
    /// nextIndex with a follower that is far behind a leader
    unaligned<term_id::type> last_log_index;
};

/// \brief special use of underlying::type to save continuations on the
/// deserialization step
struct [[gnu::packed]] vote_request {
    unaligned<model::node_id::type> node_id;
    unaligned<group_id::type> group;
    /// \brief current term
    unaligned<term_id::type> term;
    /// \brief used to compare completeness
    unaligned<term_id::type> prev_log_term;
};

struct [[gnu::packed]] vote_reply {
    /// \brief callee's term, for the caller to upate itself
    unaligned<term_id::type> term;

    /// True if the follower granted the candidate it's vote, false otherwise
    unaligned<bool> granted;

    /// set to true if the caller's log is as up to date as the recipient's
    /// - extension on raft. see Diego's phd dissertation, section 9.6
    /// - "Preventing disruptions when a server rejoins the cluster"
    unaligned<bool> log_ok;
};

} // namespace raft

namespace rpc {
template<>
void serialize(bytes_ostream&, model::broker&&);
template<>
future<model::offset> deserialize(source&);
template<>
future<model::broker> deserialize(source&);
} // namespace rpc

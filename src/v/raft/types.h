#pragma once

#include "model/fundamental.h"
#include "model/metadata.h"
#include "model/record_batch_reader.h"
#include "rpc/models.h"
#include "utils/named_type.h"

#include <seastar/net/socket_defs.hh>

#include <cstdint>

namespace raft {
using clock_type = lowres_clock;
using duration_type = typename clock_type::duration;
using timer_type = timer<clock_type>;
static constexpr clock_type::time_point no_timeout
  = clock_type::time_point::max();

using group_id = named_type<int64_t, struct raft_group_id_type>;

static constexpr const model::record_batch_type configuration_batch_type{2};
static constexpr const model::record_batch_type data_batch_type{1};

/// special case. it uses underlying type because it is the most used type
/// by using the underlying::type we save 8 continuations per deserialization
struct [[gnu::packed]] protocol_metadata {
    unaligned<group_id::type> group = -1;
    unaligned<model::offset::type> commit_index = 0;
    unaligned<model::term_id::type> term = -1;

    /// \brief used for completeness
    unaligned<model::offset::type> prev_log_index = 0;
    unaligned<model::term_id::type> prev_log_term = -1;
};

struct group_configuration {
    model::node_id leader_id;
    std::vector<model::broker> nodes;
    std::vector<model::broker> learners;
    group_configuration() = default;
    group_configuration(group_configuration&&) noexcept = default;
    group_configuration& operator=(group_configuration&&) noexcept = default;

    bool has_voters() const { return !nodes.empty(); }

    bool has_learners() const { return !learners.empty(); }

    size_t majority() const { return (nodes.size() / 2) + 1; }

    std::optional<model::broker> find_in_nodes(model::node_id id) const;
    std::optional<model::broker> find_in_learners(model::node_id id) const;
    bool contains_machine(model::node_id id) const;
};

struct follower_index_metadata {
    model::node_id node_id;
    model::term_id term;
    model::offset commit_index;
};

/// \brief a *collection* of record_batch. In other words
/// and array of array. This is done because the majority of
/// batches will come from the Kafka API which is already batched
/// Main constraint is that _all_ records and batches must be of the same type
class entry final {
public:
    explicit entry(model::record_batch_type t, model::record_batch_reader r)
      : _t(t)
      , _rdr(std::move(r)) {}
    entry(const entry&) = delete;
    entry& operator=(const entry&) = delete;
    entry(entry&&) noexcept = default;
    entry& operator=(entry&&) noexcept = default;
    model::record_batch_type entry_type() const { return _t; }
    model::record_batch_reader& reader() { return _rdr; }

private:
    model::record_batch_type _t;
    model::record_batch_reader _rdr;
};

struct append_entries_request {
    append_entries_request() = default;
    append_entries_request(const append_entries_request&) = delete;
    append_entries_request& operator=(const append_entries_request&) = delete;
    append_entries_request(append_entries_request&&) noexcept = default;
    append_entries_request& operator=(append_entries_request&&) noexcept
      = default;

    model::node_id node_id;
    protocol_metadata meta;
    std::vector<entry> entries;
};

struct [[gnu::packed]] append_entries_reply {
    /// \brief callee's node_id; work-around for batched heartbeats
    unaligned<model::node_id::type> node_id = -1;
    unaligned<group_id::type> group = -1;
    /// \brief callee's term, for the caller to upate itself
    unaligned<model::term_id::type> term = -1;
    /// \brief The recipient's last log index after it applied changes to
    /// the log. This is used to speed up finding the correct value for the
    /// nextIndex with a follower that is far behind a leader
    unaligned<model::offset::type> last_log_index = 0;
    /// \brief did the rpc succeed or not
    unaligned<bool> success = false;
};

/// \brief this is our _biggest_ modification to how raft works
/// to accomodate for millions of raft groups in a cluster.
/// internally, the receiving side will simply iterate and dispatch one
/// at a time, as well as the receiving side will trigger the
/// individual raft responses one at a time - for example to start replaying the
/// log at some offset
struct heartbeat_request {
    model::node_id node_id;
    std::vector<protocol_metadata> meta;
};
struct heartbeat_reply {
    std::vector<append_entries_reply> meta;
};

/// \brief special use of underlying::type to save continuations on the
/// deserialization step
struct [[gnu::packed]] vote_request {
    unaligned<model::node_id::type> node_id = 0;
    unaligned<group_id::type> group = -1;
    /// \brief current term
    unaligned<model::term_id::type> term = -1;
    /// \brief used to compare completeness
    unaligned<model::offset::type> prev_log_index = 0;
    unaligned<model::term_id::type> prev_log_term = -1;
};

struct [[gnu::packed]] vote_reply {
    /// \brief callee's term, for the caller to upate itself
    unaligned<model::term_id::type> term = -1;

    /// True if the follower granted the candidate it's vote, false otherwise
    unaligned<bool> granted = false;

    /// set to true if the caller's log is as up to date as the recipient's
    /// - extension on raft. see Diego's phd dissertation, section 9.6
    /// - "Preventing disruptions when a server rejoins the cluster"
    unaligned<bool> log_ok = false;
};

static inline std::ostream&
operator<<(std::ostream& o, const protocol_metadata& m) {
    return o << "{raft_group:" << m.group << ", commit_index:" << m.commit_index
             << ", term:" << m.term << ", prev_log_index:" << m.prev_log_index
             << ", prev_log_term:" << m.prev_log_term << "}";
}
static inline std::ostream& operator<<(std::ostream& o, const vote_reply& r) {
    return o << "{term:" << r.term << ", vote_granted: " << r.granted
             << ", log_ok:" << r.log_ok << "}";
}
static inline std::ostream&
operator<<(std::ostream& o, const append_entries_reply& r) {
    return o << "{node_id: " << r.node_id << ", group: " << r.group
             << ", term:" << r.term << ", last_log_index:" << r.last_log_index
             << ", success: " << r.success << "}";
}
static inline std::ostream&
operator<<(std::ostream& o, const heartbeat_request& r) {
    o << "{node: " << r.node_id << ", meta: [";
    for (auto& m : r.meta) {
        o << m << ",";
    }
    return o << "]}";
}
static inline std::ostream&
operator<<(std::ostream& o, const heartbeat_reply& r) {
    o << "{meta:[";
    for (auto& m : r.meta) {
        o << m << ",";
    }
    return o << "]}";
}

} // namespace raft

namespace rpc {
template<>
void serialize(iobuf&, raft::entry&&);
template<>
future<raft::entry> deserialize(source&);
} // namespace rpc

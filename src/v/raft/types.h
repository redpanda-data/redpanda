#pragma once

#include "model/adl_serde.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "model/record.h"
#include "model/record_batch_reader.h"
#include "model/timeout_clock.h"
#include "reflection/async_adl.h"
#include "utils/named_type.h"

#include <seastar/net/socket_defs.hh>

#include <boost/range/irange.hpp>

#include <cstdint>
#include <exception>

namespace raft {
using clock_type = ss::lowres_clock;
using duration_type = typename clock_type::duration;
using timer_type = ss::timer<clock_type>;
static constexpr clock_type::time_point no_timeout
  = clock_type::time_point::max();

using group_id = named_type<int64_t, struct raft_group_id_type>;

static constexpr const model::record_batch_type configuration_batch_type{2};
static constexpr const model::record_batch_type data_batch_type{1};

struct protocol_metadata {
    group_id::type group = -1;
    model::offset::type commit_index
      = std::numeric_limits<model::offset::type>::min();
    model::term_id::type term
      = std::numeric_limits<model::term_id::type>::min();
    /// \brief used for completeness
    model::offset::type prev_log_index
      = std::numeric_limits<model::offset::type>::min();
    model::term_id::type prev_log_term
      = std::numeric_limits<model::term_id::type>::min();
};

struct group_configuration {
    using brokers_t = std::vector<model::broker>;
    using iterator = brokers_t::iterator;
    using const_iterator = brokers_t::const_iterator;

    brokers_t nodes;
    brokers_t learners;
    group_configuration() = default;
    group_configuration(const group_configuration&) = default;
    group_configuration(group_configuration&&) noexcept = default;
    group_configuration& operator=(group_configuration&&) noexcept = default;

    bool has_voters() const { return !nodes.empty(); }

    bool has_learners() const { return !learners.empty(); }

    size_t majority() const { return (nodes.size() / 2) + 1; }

    iterator find_in_nodes(model::node_id id);
    const_iterator find_in_nodes(model::node_id id) const;

    iterator find_in_learners(model::node_id id);
    const_iterator find_in_learners(model::node_id id) const;

    bool contains_broker(model::node_id id) const;
    brokers_t all_brokers() const;
};

struct follower_index_metadata {
    explicit follower_index_metadata(model::node_id node)
      : node_id(node) {}

    model::node_id node_id;
    // index of last known log for this follower
    model::offset last_log_index;
    // index of log for which leader and follower logs matches
    model::offset match_index;
    // next index to send to this follower
    model::offset next_index;
    // timestamp of last append_entries_rpc call
    clock_type::time_point last_hbeat_timestamp;
    uint64_t failed_appends{0};
    bool is_learner = false;
    bool is_recovering = false;
};

struct append_entries_request {
    append_entries_request(const append_entries_request&) = delete;
    append_entries_request& operator=(const append_entries_request&) = delete;
    append_entries_request(append_entries_request&&) noexcept = default;
    append_entries_request& operator=(append_entries_request&&) noexcept
      = default;

    model::node_id node_id;
    protocol_metadata meta;
    model::record_batch_reader batches;
};

struct append_entries_reply {
    enum class status : uint8_t { success, failure, group_unavailable };

    /// \brief callee's node_id; work-around for batched heartbeats
    model::node_id::type node_id = -1;
    group_id::type group = -1;
    /// \brief callee's term, for the caller to upate itself
    model::term_id::type term
      = std::numeric_limits<model::term_id::type>::min();
    /// \brief The recipient's last log index after it applied changes to
    /// the log. This is used to speed up finding the correct value for the
    /// nextIndex with a follower that is far behind a leader
    model::offset::type last_log_index = 0;
    /// \brief did the rpc succeed or not
    status result = status::failure;
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

struct vote_request {
    model::node_id::type node_id = 0;
    group_id::type group = -1;
    /// \brief current term
    model::term_id::type term
      = std::numeric_limits<model::term_id::type>::min();
    /// \brief used to compare completeness
    model::offset::type prev_log_index = 0;
    model::term_id::type prev_log_term
      = std::numeric_limits<model::term_id::type>::min();
};

struct vote_reply {
    /// \brief callee's term, for the caller to upate itself
    model::term_id::type term
      = std::numeric_limits<model::term_id::type>::min();

    /// True if the follower granted the candidate it's vote, false otherwise
    bool granted = false;

    /// set to true if the caller's log is as up to date as the recipient's
    /// - extension on raft. see Diego's phd dissertation, section 9.6
    /// - "Preventing disruptions when a server rejoins the cluster"
    bool log_ok = false;
};

/// This structure is used by consensus to notify other systems about group
/// leadership changes.
struct leadership_status {
    // Current term
    model::term_id term;
    // Group for which leader have changed
    group_id group;
    // Empty when there is no known leader in the group
    std::optional<model::node_id> current_leader;
};

struct replicate_result {
    /// used by the kafka API to produce a kafka reply to produce request.
    /// see produce_request.cc
    model::offset last_offset;
};

enum class consistency_level { quorum_ack, leader_ack, no_ack };

struct replicate_options {
    explicit replicate_options(consistency_level l)
      : consistency(l) {}

    consistency_level consistency;
};

std::ostream& operator<<(std::ostream& o, const consistency_level& l);
std::ostream& operator<<(std::ostream& o, const protocol_metadata& m);
std::ostream& operator<<(std::ostream& o, const vote_reply& r);
std::ostream& operator<<(std::ostream& o, const append_entries_reply::status&);
std::ostream& operator<<(std::ostream& o, const append_entries_reply& r);
std::ostream& operator<<(std::ostream& o, const vote_request& r);
std::ostream& operator<<(std::ostream& o, const follower_index_metadata& i);
std::ostream& operator<<(std::ostream& o, const heartbeat_request& r);
std::ostream& operator<<(std::ostream& o, const heartbeat_reply& r);
std::ostream& operator<<(std::ostream& o, const group_configuration& c);
} // namespace raft

namespace reflection {
template<>
struct async_adl<raft::append_entries_request> {
    ss::future<> to(iobuf& out, raft::append_entries_request&& request);
    ss::future<raft::append_entries_request> from(iobuf_parser& in);
};
} // namespace reflection

#pragma once
#include "model/fundamental.h"
#include "redpanda/kafka/errors/errors.h"
#include "redpanda/kafka/groups/member.h"
#include "redpanda/kafka/groups/types.h"
#include "redpanda/kafka/requests/join_group_request.h"
#include "seastarx.h"

#include <seastar/core/future.hh>
#include <seastar/core/lowres_clock.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/util/log.hh>

#include <iosfwd>
#include <optional>
#include <unordered_map>
#include <unordered_set>
#include <vector>

namespace kafka {

/**
 * \defgroup kafka-groups Kafka group membership API
 *
 * The Kafka API defines a group membership protocol for distributing and
 * synchronizing state across a set of clients. The primary use case for the
 * group membership API is the implementation of consumer groups which is a
 * feature in Kafka for distributing the work of consuming a topic across the
 * members in the group.
 *
 * The group membership API is generic and can be used by Kafka clients to
 * build other group membership-based features. For instance, in addition to
 * consumer groups, the group membership API is used to implement Kafka Connect
 * that aids in connecting Kafka with external data sources.
 *
 * A note on naming. Some of the names used in the group membership API
 * implementation are not ideal. However, most of the names directly correspond
 * to their counterparts in the Kafka implementation. This equivalence has
 * proven generally useful when comparing implementations.
 *
 * \addtogroup kafka-groups
 * @{
 */

/**
 * \brief Group states.
 */
enum class group_state {
    /// The group currently has no members.
    empty,

    /// The group is preparing to rebalance.
    preparing_rebalance,

    /// The group is waiting on the leader to provide assignments.
    completing_rebalance,

    /// The group is stable.
    stable,

    /// Transient state as the group is being removed.
    dead,
};

std::ostream& operator<<(std::ostream&, group_state gs);

/// \brief A Kafka group.
///
/// Container of members.
class group {
public:
    using duration_type = lowres_clock::duration;

    group(kafka::group_id id, group_state s)
      : _id(id)
      , _state(s)
      , _generation(0)
      , _num_members_joining(0) {
    }

    /// Get the group id.
    const kafka::group_id& id() const {
        return _id;
    }

    /// Return the group state.
    group_state state() const {
        return _state;
    }

    /// Check if the group is in a given state.
    bool in_state(group_state s) const {
        return _state == s;
    }

    /// Transition the group to a new state.
    void set_state(group_state s);

    /// Return the generation of the group.
    kafka::generation_id generation() const {
        return _generation;
    }

    /**
     * \brief Access a group member.
     *
     * \throws std::out_of_range if member is not found.
     */
    member_ptr get_member(const kafka::member_id& id) const {
        return _members.at(id);
    }

    /// Check if the group contains a member.
    bool contains_member(const kafka::member_id& member_id) const {
        return _members.find(member_id) != _members.end();
    }

    /// Check if the group has members.
    bool has_members() const {
        return !_members.empty();
    }

    /// Check if all members have joined.
    bool all_members_joined() const {
        return (_members.size() == _num_members_joining)
               && _pending_members.empty();
    }

    /// Add a member to the group in a pending state.
    void add_pending_member(const kafka::member_id& member_id) {
        _pending_members.emplace(member_id);
    }

    /// Check if the group contains a pending member.
    bool contains_pending_member(const kafka::member_id& member) const {
        return _pending_members.find(member) != _pending_members.end();
    }

    void remove_pending_member(const kafka::member_id& member_id) {
        _pending_members.erase(member_id);
    }

    /// Check if a member id refers to the group leader.
    bool is_leader(const kafka::member_id& member_id) const {
        return _leader && _leader.value() == member_id;
    }

    /// Get the group's configured protocol type (if any).
    const std::optional<kafka::protocol_type>& protocol_type() const {
        return _protocol_type;
    }

    /// Get the group's configured protocol (if any).
    const std::optional<kafka::protocol_name>& protocol() const {
        return _protocol;
    }

    /// Get the group leader (if any).
    const std::optional<member_id>& leader() const {
        return _leader;
    }

    /**
     * \brief check if group supports a member's protocol configuration.
     *
     * if the group is empty, then as long as the member (1) specifies a
     * protocol type and (2) lists at least one protocol, the protocol
     * configuration is supported.
     *
     * if the group is non-empty, then the configuration is supported if
     * the group and member have the same protocol type and the member
     * specifies at least one protocol that is supported by all members of
     * the group.
     */
    bool supports_protocols(const requests::join_group_request& r);

    /**
     * \brief Add a member to the group.
     *
     * If the group is empty, the member will define the group's protocol class
     * and become the group leader.
     *
     * \returns join response promise set at the end of the join phase.
     */
    future<requests::join_group_response> add_member(member_ptr member);

    /**
     * \brief Update the set of protocols supported by a group member.
     *
     * \returns join response promise set at the end of the join phase.
     */
    future<requests::join_group_response> update_member(
      member_ptr member, std::vector<member_protocol>&& new_protocols);

    /**
     * \brief Get the timeout duration for rebalancing.
     *
     * Returns the maximum rebalance timeout across all group members.
     *
     * \throws std::runtime_error if the group has no members.
     */
    duration_type rebalance_timeout() const;

    /**
     * \brief Remove members that have not rejoined the group.
     *
     * When a member is removed any pending heartbeat timers will be cancelled.
     */
    void remove_unjoined_members();

    /**
     * \brief Return member metadata for the group's selected protocol.
     *
     * This is used at the end of the join phase to generate the group leader's
     * response, which includes all of the member metadata associated with the
     * group's selected protocol.
     *
     * Caller must ensure that the group's protocol is set.
     */
    std::vector<requests::join_group_response::member_config>
    member_metadata() const;

    /**
     * \brief Add empty assignments for missing group members.
     *
     * The assignments mapping is updated to include an empty assignment for any
     * group member without an assignment.
     */
    void add_missing_assignments(assignments_type& assignments) const;

    /**
     * \brief Apply the assignments to group members.
     *
     * Each assignment is a (member, bytes) mapping.
     *
     * \throws std::out_of_range if an assignment is for a member that does not
     * belong to the group.
     */
    void set_assignments(assignments_type assignments) const;

    /// Clears all member assignments.
    void clear_assignments() const;

    /**
     * \brief Advance the group to the next generation.
     *
     * When the group has members then a protocol is selected and the group
     * moves to the `completing_rebalance` state. Otherwise, the group is put
     * into the `empty` state.
     */
    void advance_generation();

    /**
     * \brief Select a group protocol.
     *
     * A protocol is selected by a voting process in which each member votes for
     * its preferred protocol from the set of protocols supported by all
     * members. The protocol with the most votes is selected.
     *
     * \throws std::out_of_range if any member fails to cast a vote.
     */
    kafka::protocol_name select_protocol() const;

    /**
     * \brief Fulfill joining members' response promise.
     *
     * Joining members may be delayed waiting for other members to join or to
     * implement join debouncing for efficiency. This method is called after the
     * join process completes and generates join responses for each member.
     */
    void finish_joining_members();

    /**
     * \brief Fufill syncing members' response promise.
     *
     * A syncing member waits on a future that is fulfilled once the group
     * leader has reported member assignments. This method is called by the
     * leader to set the value on the associated promise.
     */
    void finish_syncing_members(error_code error) const;

    /**
     * \brief Get the group's associated partition.
     *
     * TODO:
     *   - integrate with raft persistence.
     */
    const model::ntp& ntp() const {
        return _ntp;
    }

    /// Check if moving to the given state is a valid transition.
    bool valid_previous_state(group_state s) const;

    /**
     * \brief Check if the leader has rejoined or choose new leader.
     *
     * Returns true if either the current leader has rejoined, or a joining
     * member is selected to be the new leader. Otherwise, false is returned.
     */
    bool leader_rejoined();

    /**
     * \brief Generate a new member id.
     *
     * The structure of a member id is "id-{uuid}" where `id` is the group
     * instance id if it exists, or the client id otherwise.
     */
    static kafka::member_id
    generate_member_id(const requests::join_group_request& r);

private:
    using member_map = std::unordered_map<kafka::member_id, member_ptr>;
    using protocol_support = std::unordered_map<kafka::protocol_name, int>;

    kafka::group_id _id;
    group_state _state;
    model::ntp _ntp;
    kafka::generation_id _generation;
    protocol_support _supported_protocols;
    member_map _members;
    size_t _num_members_joining;
    std::unordered_set<kafka::member_id> _pending_members;
    std::optional<kafka::protocol_type> _protocol_type;
    std::optional<kafka::protocol_name> _protocol;
    std::optional<kafka::member_id> _leader;
};

using group_ptr = lw_shared_ptr<group>;

std::ostream& operator<<(std::ostream&, const group&);

/// @}

} // namespace kafka

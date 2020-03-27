#pragma once
#include "bytes/bytes.h"
#include "kafka/requests/join_group_request.h"
#include "kafka/requests/sync_group_request.h"
#include "kafka/types.h"
#include "utils/concepts-enabled.h"

#include <seastar/core/future.hh>
#include <seastar/core/shared_ptr.hh>

#include <algorithm>
#include <chrono>
#include <iosfwd>
#include <iterator>
#include <memory>
#include <optional>
#include <set>
#include <utility>
#include <vector>

namespace kafka {

/// \addtogroup kafka-groups
/// @{

/// \brief A Kafka group member.
class group_member {
public:
    using clock_type = ss::lowres_clock;
    using duration_type = clock_type::duration;

    group_member(
      kafka::member_id member_id,
      kafka::group_id group_id,
      std::optional<kafka::group_instance_id> group_instance_id,
      duration_type session_timeout,
      duration_type rebalance_timeout,
      kafka::protocol_type protocol_type,
      std::vector<member_protocol> protocols)
      : _id(std::move(member_id))
      , _group_id(std::move(group_id))
      , _group_instance_id(std::move(group_instance_id))
      , _session_timeout(session_timeout)
      , _rebalance_timeout(rebalance_timeout)
      , _protocol_type(std::move(protocol_type))
      , _protocols(std::move(protocols))
      , _is_new(false) {}

    /// Get the member id.
    const kafka::member_id& id() const { return _id; }

    /// Get the id of the member's group.
    const kafka::group_id& group_id() const { return _group_id; }

    /// Get the instance id of the member's group.
    const std::optional<kafka::group_instance_id>& group_instance_id() const {
        return _group_instance_id;
    }

    /// Get the member's session timeout.
    duration_type session_timeout() const { return _session_timeout; }

    /// Get the member's rebalance timeout.
    duration_type rebalance_timeout() const { return _rebalance_timeout; }

    /// Get the member's protocol type.
    const kafka::protocol_type& protocol_type() const { return _protocol_type; }

    /// Get the member's assignment.
    const bytes& assignment() const { return _assignment; }

    /// Set the member's assignment.
    void set_assignment(bytes assignment) {
        _assignment = std::move(assignment);
    }

    /// Clear the member's assignment.
    void clear_assignment() { _assignment.reset(); }

    const std::vector<member_protocol>& protocols() const { return _protocols; }

    /// Update the set of protocols supported by the member.
    void set_protocols(std::vector<member_protocol> protocols) {
        _protocols = std::move(protocols);
    }

    /// Update the is_new flag.
    void set_new(bool is_new) { _is_new = is_new; }

    /// Check if the member is waiting to join.
    bool is_joining() const { return bool(_join_promise); }

    /**
     * Get the join response.
     *
     * NOTE: the caller must ensure that the member is not already joining.
     */
    ss::future<join_group_response> get_join_response() {
        _join_promise = std::make_unique<join_promise>();
        return _join_promise->get_future();
    }

    /// Fulfill the join promise.
    void set_join_response(join_group_response&& response) {
        auto pr = std::move(_join_promise);
        pr->set_value(std::move(response));
    }

    /// Check if the member is syncing.
    bool is_syncing() const { return bool(_sync_promise); }

    /**
     * Get the sync response.
     *
     * NOTE: the caller must ensure that the member is not already syncing.
     */
    ss::future<sync_group_response> get_sync_response() {
        _sync_promise = std::make_unique<sync_promise>();
        return _sync_promise->get_future();
    }

    /// Fulfill the sync promise.
    void set_sync_response(sync_group_response&& response) {
        auto pr = std::move(_sync_promise);
        pr->set_value(std::move(response));
    }

    /**
     * \brief Vote for a protocol from the candidates.
     *
     * Returns the highest preference member protocol that is also contained
     * in the set of candidate protocols.
     *
     * \throws std::out_of_range if no candidate is supported.
     */
    const kafka::protocol_name&
    vote_for_protocol(const std::set<protocol_name>& candidates) const;

    /**
     * \brief Get the member's protocol metadata by name.
     *
     * This method has linear cost since the underlying container is a vector,
     * sorted by priority, rather than by protocol name. A member should always
     * have very few protocols in this container.
     *
     * \throws std::out_of_range if the protocol is not found.
     */
    const bytes&
    get_protocol_metadata(const kafka::protocol_name& protocol) const;

    bool should_keep_alive(
      clock_type::time_point deadline, clock_type::duration new_join_timeout) {
        if (is_joining()) {
            return _is_new || (_latest_heartbeat + new_join_timeout) > deadline;
        }

        if (is_syncing()) {
            return (_latest_heartbeat + _session_timeout) > deadline;
        }

        return false;
    }

    void set_latest_heartbeat(clock_type::time_point t) {
        _latest_heartbeat = t;
    }

    ss::timer<clock_type>& expire_timer() { return _expire_timer; }

    ~group_member() noexcept { _expire_timer.cancel(); }

private:
    using join_promise = ss::promise<join_group_response>;
    using sync_promise = ss::promise<sync_group_response>;

    friend std::ostream& operator<<(std::ostream&, const group_member&);

    kafka::member_id _id;
    kafka::group_id _group_id;
    std::optional<kafka::group_instance_id> _group_instance_id;
    duration_type _session_timeout;
    duration_type _rebalance_timeout;
    kafka::protocol_type _protocol_type;
    bytes _assignment;
    std::vector<member_protocol> _protocols;
    bool _is_new;
    clock_type::time_point _latest_heartbeat;
    ss::timer<clock_type> _expire_timer;

    // external shutdown synchronization
    std::unique_ptr<sync_promise> _sync_promise;
    std::unique_ptr<join_promise> _join_promise;
};

/// \brief Shared pointer to a group member.
using member_ptr = ss::lw_shared_ptr<group_member>;

std::ostream& operator<<(std::ostream&, const group_member&);

/// @}

} // namespace kafka


#pragma once
#include <system_error>

namespace cluster {

enum class errc {
    success = 0, // must be 0
    notification_wait_timeout,
    topic_invalid_partitions,
    topic_invalid_replication_factor,
    topic_invalid_config,
    not_leader_controller,
    topic_already_exists,
    replication_error,
    shutting_down,
    no_leader_controller,
    join_request_dispatch_error,
    seed_servers_exhausted,
    auto_create_topics_exception,
    timeout,
    topic_not_exists,
};
struct errc_category final : public std::error_category {
    const char* name() const noexcept final { return "cluster::errc"; }

    std::string message(int c) const final {
        switch (static_cast<errc>(c)) {
        case errc::success:
            return "Success";
        case errc::notification_wait_timeout:
            return "Timeout waiting for append entries notification comming "
                   "from raft 0";
        case errc::topic_invalid_partitions:
            return "Unable to assign topic partitions to current cluster "
                   "resources";
        case errc::topic_invalid_replication_factor:
            return "Unable to allocate topic with given replication factor";
        case errc::topic_invalid_config:
            return "Topic configuration is either bogus or not supported";
        case errc::not_leader_controller:
            return "This node is not raft-0 leader. i.e is not leader "
                   "controller";
        case errc::topic_already_exists:
            return "The topic has already been created";
        case errc::replication_error:
            return "Controller was unable to replicate given state across "
                   "cluster nodes";
        case errc::shutting_down:
            return "Application is shutting down";
        case errc::no_leader_controller:
            return "Currently there is no leader controller elected in the "
                   "cluster";
        case errc::join_request_dispatch_error:
            return "Error occurred when controller tried to join the cluster";
        case errc::seed_servers_exhausted:
            return "There are no seed servers left to try in this round, will "
                   "retry after delay ";
        case errc::auto_create_topics_exception:
            return "An exception was thrown while auto creating topics";
        case errc::timeout:
            return "Timeout occurred while processing request";
        case errc::topic_not_exists:
            return "Topic does not exists";
        default:
            return "cluster::errc::unknown";
        }
    }
};
inline const std::error_category& error_category() noexcept {
    static errc_category e;
    return e;
}
inline std::error_code make_error_code(errc e) noexcept {
    return std::error_code(static_cast<int>(e), error_category());
}
} // namespace cluster
namespace std {
template<>
struct is_error_code_enum<cluster::errc> : true_type {};
} // namespace std
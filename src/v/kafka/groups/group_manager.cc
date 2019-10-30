#include "kafka/groups/group_manager.h"

#include "kafka/requests/delete_groups_request.h"
#include "kafka/requests/describe_groups_request.h"
#include "kafka/requests/offset_commit_request.h"
#include "kafka/requests/offset_fetch_request.h"

namespace kafka {

future<join_group_response> group_manager::join_group(join_group_request&& r) {
    kglog.trace("join request {}", r);

    if (r.group_instance_id) {
        kglog.trace("static group membership is unsupported");
        return make_join_error(
          unknown_member_id, error_code::unsupported_version);
    }

    auto error = validate_group_status(r.group_id, join_group_api::key);
    if (error != error_code::none) {
        kglog.trace("request validation failed with error={}", error);
        return make_join_error(r.member_id, error);
    }

    if (
      r.session_timeout < _conf.group_min_session_timeout_ms()
      || r.session_timeout > _conf.group_max_session_timeout_ms()) {
        kglog.trace(
          "join group request has invalid session timeout min={}/{}/max={}",
          _conf.group_min_session_timeout_ms(),
          r.session_timeout,
          _conf.group_max_session_timeout_ms());
        return make_join_error(
          r.member_id, error_code::invalid_session_timeout);
    }

    auto group = get_group(r.group_id);
    if (!group) {
        // <kafka>only try to create the group if the group is UNKNOWN AND
        // the member id is UNKNOWN, if member is specified but group does
        // not exist we should reject the request.</kafka>
        if (r.member_id != unknown_member_id) {
            kglog.trace(
              "join request rejected for known member and unknown group");
            return make_join_error(r.member_id, error_code::unknown_member_id);
        }
        group = make_lw_shared<kafka::group>(
          r.group_id, group_state::empty, _conf);
        _groups.emplace(r.group_id, group);
        kglog.trace("created new group {}", group);
    }

    return group->handle_join_group(std::move(r));
}

future<sync_group_response> group_manager::sync_group(sync_group_request&& r) {
    kglog.trace("sync request {}", r);

    if (r.group_instance_id) {
        kglog.trace("static group membership is unsupported");
        return make_sync_error(error_code::unsupported_version);
    }

    auto error = validate_group_status(r.group_id, sync_group_api::key);
    if (error != error_code::none) {
        kglog.trace("invalid group status {}", error);
        if (error == error_code::coordinator_load_in_progress) {
            // <kafka>The coordinator is loading, which means we've lost the
            // state of the active rebalance and the group will need to start
            // over at JoinGroup. By returning rebalance in progress, the
            // consumer will attempt to rejoin without needing to rediscover the
            // coordinator. Note that we cannot return
            // COORDINATOR_LOAD_IN_PROGRESS since older clients do not expect
            // the error.</kafka>
            return make_sync_error(error_code::rebalance_in_progress);
        }
        return make_sync_error(error);
    }

    auto group = get_group(r.group_id);
    if (group) {
        return group->handle_sync_group(std::move(r));
    } else {
        kglog.trace("group not found");
        return make_sync_error(error_code::unknown_member_id);
    }
}

future<heartbeat_response> group_manager::heartbeat(heartbeat_request&& r) {
    kglog.trace("heartbeat request {}", r);

    if (r.group_instance_id) {
        kglog.trace("static group membership is unsupported");
        return make_heartbeat_error(error_code::unsupported_version);
    }

    auto error = validate_group_status(r.group_id, heartbeat_api::key);
    if (error != error_code::none) {
        kglog.trace("invalid group status {}", error);
        if (error == error_code::coordinator_load_in_progress) {
            // <kafka>the group is still loading, so respond just
            // blindly</kafka>
            return make_heartbeat_error(error_code::none);
        }
        return make_heartbeat_error(error);
    }

    auto group = get_group(r.group_id);
    if (group) {
        return group->handle_heartbeat(std::move(r));
    }

    kglog.trace("group not found");
    return make_heartbeat_error(error_code::unknown_member_id);
}

bool group_manager::valid_group_id(group_id group, api_key api) {
    switch (api) {
    case offset_commit_api::key:
        [[fallthrough]];
    case offset_fetch_api::key:
        [[fallthrough]];
    case describe_groups_api::key:
        [[fallthrough]];
    case delete_groups_api::key:
        // <kafka> For backwards compatibility, we support the offset commit
        // APIs for the empty groupId, and also in DescribeGroups and
        // DeleteGroups so that users can view and delete state of all
        // groups.</kafka>

        // return true;
        return false; // these apis are not yet implemented
    default:
        return !group().empty();
    }
}

/*
 * TODO
 * - check for group being shutdown
 * - check for group being recovered
 * - check coordinator for correct leader
 */
error_code group_manager::validate_group_status(group_id group, api_key api) {
    if (!valid_group_id(group, api)) {
        return error_code::invalid_group_id;
    }
    return error_code::none;
}

} // namespace kafka

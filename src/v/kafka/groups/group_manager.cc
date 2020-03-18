#include "kafka/groups/group_manager.h"

#include "kafka/requests/delete_groups_request.h"
#include "kafka/requests/describe_groups_request.h"
#include "kafka/requests/offset_commit_request.h"
#include "kafka/requests/offset_fetch_request.h"

namespace kafka {

ss::future<> group_manager::start() {
    /*
     * receive notifications when group-metadata partitions come under
     * management on this core. note that the notify callback will be
     * synchronously invoked for all existing partitions that match the query.
     */
    _manage_notify_handle = _pm.local().register_manage_notification(
      cluster::kafka_internal_namespace,
      cluster::kafka_group_topic,
      [this](ss::lw_shared_ptr<cluster::partition> p) { attach_partition(p); });

    return ss::make_ready_future<>();
}

ss::future<> group_manager::stop() {
    // prevent new partition attachments
    _pm.local().unregister_manage_notification(_manage_notify_handle);

    return _gate.close();
}

void group_manager::attach_partition(ss::lw_shared_ptr<cluster::partition> p) {
    (void)with_gate(_gate, [this, p = std::move(p)]() {
        kglog.debug("attaching group metadata partition {}", p->ntp());
        auto attached = ss::make_lw_shared<attached_partition>(p);
        auto res = _partitions.try_emplace(p->ntp(), attached);
        vassert(res.second, "double ntp registration");
    }).handle_exception_type([p](const ss::gate_closed_exception&) {
        kglog.info("ignored partition attach during shutdown {}", p);
    });
}

ss::future<join_group_response>
group_manager::join_group(join_group_request&& r) {
    kglog.trace("join request {}", r);

    auto error = validate_group_status(r.ntp, r.group_id, join_group_api::key);
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
        group = ss::make_lw_shared<kafka::group>(
          r.group_id, group_state::empty, _conf);
        _groups.emplace(r.group_id, group);
        kglog.trace("created new group {}", group);
    }

    return group->handle_join_group(std::move(r));
}

ss::future<sync_group_response>
group_manager::sync_group(sync_group_request&& r) {
    kglog.trace("sync request {}", r);

    if (r.group_instance_id) {
        kglog.trace("static group membership is unsupported");
        return make_sync_error(error_code::unsupported_version);
    }

    auto error = validate_group_status(r.ntp, r.group_id, sync_group_api::key);
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

ss::future<heartbeat_response> group_manager::heartbeat(heartbeat_request&& r) {
    kglog.trace("heartbeat request {}", r);

    if (r.group_instance_id) {
        kglog.trace("static group membership is unsupported");
        return make_heartbeat_error(error_code::unsupported_version);
    }

    auto error = validate_group_status(r.ntp, r.group_id, heartbeat_api::key);
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

ss::future<leave_group_response>
group_manager::leave_group(leave_group_request&& r) {
    kglog.trace("leave request {}", r);

    auto error = validate_group_status(r.ntp, r.group_id, leave_group_api::key);
    if (error != error_code::none) {
        kglog.trace("invalid group status error={}", error);
        return make_leave_error(error);
    }

    auto group = get_group(r.group_id);
    if (group) {
        return group->handle_leave_group(std::move(r));
    } else {
        kglog.trace("group does not exist");
        return make_leave_error(error_code::unknown_member_id);
    }
}

ss::future<offset_commit_response>
group_manager::offset_commit(offset_commit_request&& r) {
    auto error = validate_group_status(
      r.ntp, r.group_id, offset_commit_api::key);
    if (error != error_code::none) {
        return ss::make_ready_future<offset_commit_response>(
          offset_commit_response(r, error));
    }

    auto group = get_group(r.group_id);
    if (!group) {
        if (r.generation_id < 0) {
            // <kafka>the group is not relying on Kafka for group management, so
            // allow the commit</kafka>
            group = ss::make_lw_shared<kafka::group>(
              r.group_id, group_state::empty, _conf);
            _groups.emplace(r.group_id, group);
        } else {
            // <kafka>or this is a request coming from an older generation.
            // either way, reject the commit</kafka>
            return ss::make_ready_future<offset_commit_response>(
              offset_commit_response(r, error_code::illegal_generation));
        }
    }

    return group->handle_offset_commit(std::move(r));
}

ss::future<offset_fetch_response>
group_manager::offset_fetch(offset_fetch_request&& r) {
    auto error = validate_group_status(
      r.ntp, r.group_id, offset_fetch_api::key);
    if (error != error_code::none) {
        return ss::make_ready_future<offset_fetch_response>(
          offset_fetch_response(error));
    }

    auto group = get_group(r.group_id);
    if (!group) {
        return ss::make_ready_future<offset_fetch_response>(
          offset_fetch_response(r.topics));
    }

    return group->handle_offset_fetch(std::move(r));
}

bool group_manager::valid_group_id(const group_id& group, api_key api) {
    switch (api) {
    case offset_commit_api::key:
        [[fallthrough]];
    case offset_fetch_api::key:
        // <kafka> For backwards compatibility, we support the offset commit
        // APIs for the empty groupId, and also in DescribeGroups and
        // DeleteGroups so that users can view and delete state of all
        // groups.</kafka>
        return true;

    // currently unsupported apis
    case describe_groups_api::key:
        [[fallthrough]];
    case delete_groups_api::key:
        return false;

    // join-group etc... require non-empty group ids
    default:
        return !group().empty();
    }
}

/*
 * TODO
 * - check for group being shutdown
 */
error_code group_manager::validate_group_status(
  const model::ntp& ntp, const group_id& group, api_key api) {
    if (!valid_group_id(group, api)) {
        return error_code::invalid_group_id;
    }

    if (const auto it = _partitions.find(ntp); it != _partitions.end()) {
        if (!it->second->partition->is_leader()) {
            kglog.trace("group partition is not leader {}/{}", group, ntp);
            return error_code::not_coordinator;
        }

        if (it->second->loading) {
            kglog.trace("group is loading {}/{}", group, ntp);
            return error_code::coordinator_load_in_progress;
        }

        return error_code::none;
    }

    kglog.trace("group operation misdirected {}/{}", group, ntp);
    return error_code::not_coordinator;
}

} // namespace kafka

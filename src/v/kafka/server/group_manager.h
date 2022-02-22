/*
 * Copyright 2020 Vectorized, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once
#include "cluster/fwd.h"
#include "kafka/protocol/delete_groups.h"
#include "kafka/protocol/describe_groups.h"
#include "kafka/protocol/errors.h"
#include "kafka/protocol/heartbeat.h"
#include "kafka/protocol/join_group.h"
#include "kafka/protocol/leave_group.h"
#include "kafka/protocol/list_groups.h"
#include "kafka/protocol/offset_commit.h"
#include "kafka/protocol/offset_fetch.h"
#include "kafka/protocol/sync_group.h"
#include "kafka/protocol/txn_offset_commit.h"
#include "kafka/server/group.h"
#include "kafka/server/group_recovery_consumer.h"
#include "kafka/server/group_stm.h"
#include "kafka/server/member.h"
#include "model/namespace.h"
#include "raft/group_manager.h"
#include "seastarx.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/core/future.hh>
#include <seastar/core/sharded.hh>

#include <absl/container/node_hash_map.h>
#include <cluster/partition_manager.h>

namespace kafka {

/*
 * \brief Manages the Kafka group lifecycle.
 *
 * Dynamic partition attachment
 * ============================
 *
 * When a partition belonging to the internal group metadata topic comes under
 * management on a node it is dynamically registered with the group manager. The
 * set of partitions is managed in the group manager in an index:
 *
 *     [ntp -> attached_partition]
 *
 * Where `attached_partition` is a struct with an initial state of:
 *
 *     { loading = true, semaphore(1), abort_source, partition }
 *
 * Leadership changes
 * ==================
 *
 * The group manager handles leadership changes by transitioning the state of
 * attached partitions, either by recovering state or clearing state from cache.
 *
 * On leadership change notification
 *
 *     1. Do nothing if notification is for a non-registered partition
 *     2. While holding the attached partition's semaphore
 *     3. Recover or unload (see below)
 *
 * Leadership change notifications current occur for _all_ partitions on the
 * current {node, core} pair (not just group metadata partitions). Hence, we do
 * nothing for non-registered partitions. See dynamic partition attachment
 * discussion above regarding registration.
 *
 * In order to support (1) parallel partition recovery (important during
 * start-up) and (2) flapping leadership we grab a per-partition semaphore to
 * queue state transitions on the registered partition.
 *
 * The semaphore is used to serialize recovery/unload requests which are
 * themselves an asynchronous fiber. The controller/raft dispatches these
 * requests as leadership changes, and the requests are sync upcalls that can't
 * be handled synchronously without blocking raft.
 *
 * When a new upcall is received, we use the abort source to request that any
 * on-going recovery/unload is stopped promptly.
 *
 * Recovery (background)
 * =====================
 *
 * - Both recovery and partition unload are serialized per-partition
 * - Recovery occurs when the local node is leader, else unload (below)
 *
 * The recovery process reads the entire log and deduplicates entries into the
 * `recovery_batch_consumer` object.
 *
 * After the log is read the deduplicated state is used to re-populate the
 * in-memory cache of groups/commits through.
 *
 * Unload (background)
 * ===================
 *
 * - Both recovery and partition unload are serialized per-partition
 * - Unloading occurs when the local node loses partition leadership
 *
 * This process involves identifying the groups/commits that map to a partition
 * for which the local node is no longer a leader. The in-memory cache will be
 * cleared.
 *
 *     - This is not yet implemented.
 */
class group_manager {
public:
    group_manager(
      ss::sharded<raft::group_manager>& gm,
      ss::sharded<cluster::partition_manager>& pm,
      ss::sharded<cluster::topic_table>&,
      config::configuration& conf);

    ss::future<> start();
    ss::future<> stop();

public:
    /// \brief Handle a JoinGroup request
    ss::future<join_group_response> join_group(join_group_request&& request);

    /// \brief Handle a SyncGroup request
    ss::future<sync_group_response> sync_group(sync_group_request&& request);

    /// \brief Handle a Heartbeat request
    ss::future<heartbeat_response> heartbeat(heartbeat_request&& request);

    /// \brief Handle a LeaveGroup request
    ss::future<leave_group_response> leave_group(leave_group_request&& request);

    /// \brief Handle a OffsetCommit request
    group::offset_commit_stages offset_commit(offset_commit_request&& request);

    ss::future<txn_offset_commit_response>
    txn_offset_commit(txn_offset_commit_request&& request);

    ss::future<cluster::commit_group_tx_reply>
    commit_tx(cluster::commit_group_tx_request&& request);

    ss::future<cluster::begin_group_tx_reply>
    begin_tx(cluster::begin_group_tx_request&&);

    ss::future<cluster::prepare_group_tx_reply>
    prepare_tx(cluster::prepare_group_tx_request&&);

    ss::future<cluster::abort_group_tx_reply>
    abort_tx(cluster::abort_group_tx_request&&);

    /// \brief Handle a OffsetFetch request
    ss::future<offset_fetch_response>
    offset_fetch(offset_fetch_request&& request);

    // returns the set of registered groups, and an error if one occurred while
    // retrieving the group list (e.g. coordinator_load_in_progress).
    std::pair<error_code, std::vector<listed_group>> list_groups() const;

    described_group describe_group(const model::ntp&, const kafka::group_id&);

    ss::future<std::vector<deletable_group_result>>
      delete_groups(std::vector<std::pair<model::ntp, group_id>>);

public:
    error_code validate_group_status(
      const model::ntp& ntp, const group_id& group, api_key api);

    static bool valid_group_id(const group_id& group, api_key api);

private:
    group_ptr get_group(const group_id& group) {
        if (auto it = _groups.find(group); it != _groups.end()) {
            return it->second;
        }
        return nullptr;
    }

    cluster::notification_id_type _manage_notify_handle;
    cluster::notification_id_type _unmanage_notify_handle;
    ss::gate _gate;

    void attach_partition(ss::lw_shared_ptr<cluster::partition>);
    void detach_partition(const model::ntp&);

    struct attached_partition {
        bool loading;
        ss::semaphore sem{1};
        ss::abort_source as;
        ss::lw_shared_ptr<cluster::partition> partition;
        ss::basic_rwlock<> catchup_lock;
        model::term_id term{-1};

        explicit attached_partition(ss::lw_shared_ptr<cluster::partition> p)
          : loading(true)
          , partition(std::move(p)) {}
    };

    cluster::notification_id_type _leader_notify_handle;
    cluster::notification_id_type _topic_table_notify_handle;

    void handle_leader_change(
      model::term_id,
      ss::lw_shared_ptr<cluster::partition>,
      std::optional<model::node_id>);

    void handle_topic_delta(const std::vector<cluster::topic_table_delta>&);

    ss::future<> cleanup_removed_topic_partitions(
      const std::vector<model::topic_partition>&);

    ss::future<> handle_partition_leader_change(
      model::term_id,
      ss::lw_shared_ptr<attached_partition>,
      std::optional<model::node_id> leader_id);

    ss::future<> recover_partition(
      model::term_id,
      ss::lw_shared_ptr<attached_partition>,
      group_recovery_consumer_state);

    ss::future<> gc_partition_state(ss::lw_shared_ptr<attached_partition>);

    ss::future<> inject_noop(
      ss::lw_shared_ptr<cluster::partition> p,
      ss::lowres_clock::time_point timeout);

    ss::lw_shared_ptr<attached_partition>
    get_attached_partition(model::ntp ntp) {
        auto it = _partitions.find(ntp);
        if (it == _partitions.end()) {
            return nullptr;
        }
        return it->second;
    }

    ss::sharded<raft::group_manager>& _gm;
    ss::sharded<cluster::partition_manager>& _pm;
    ss::sharded<cluster::topic_table>& _topic_table;
    config::configuration& _conf;
    absl::node_hash_map<group_id, group_ptr> _groups;
    absl::node_hash_map<model::ntp, ss::lw_shared_ptr<attached_partition>>
      _partitions;
    //

    model::broker _self;
};

} // namespace kafka

namespace reflection {

/*
 * new code + new version: will see extra bytes for client host/id
 * new code + old version: will observe no extra bytes
 * old code + old/new version: will not read new appended fields
 *
 * on the wire we write out a version that is compatible with old code and then
 * append the additional client host/id information for each member. then when
 * deserializing we process the appended data by updating the old versions from
 * disk in the new in-memory structs which contain the extra member metadata.
 * old code skips over the appended data.
 */
template<>
struct adl<kafka::group_log_group_metadata> {
    struct member_state_v0 {
        kafka::member_id id;
        std::chrono::milliseconds session_timeout;
        std::chrono::milliseconds rebalance_timeout;
        std::optional<kafka::group_instance_id> instance_id;
        kafka::protocol_type protocol_type;
        std::vector<kafka::member_protocol> protocols;
        iobuf assignment;
    };

    struct group_log_group_metadata_v0 {
        kafka::protocol_type protocol_type;
        kafka::generation_id generation;
        std::optional<kafka::protocol_name> protocol;
        std::optional<kafka::member_id> leader;
        int32_t state_timestamp;
        std::vector<member_state_v0> members;
    };

    struct client_host_id {
        kafka::client_id id;
        kafka::client_host host;
    };

    void to(iobuf& out, kafka::group_log_group_metadata&& data) {
        // create instance of old version of members
        std::vector<member_state_v0> members_v0;
        members_v0.reserve(data.members.size());
        for (auto& member : data.members) {
            members_v0.push_back(member_state_v0{
              .id = std::move(member.id),
              .session_timeout = member.session_timeout,
              .rebalance_timeout = member.rebalance_timeout,
              .instance_id = std::move(member.instance_id),
              .protocol_type = std::move(member.protocol_type),
              .protocols = std::move(member.protocols),
              .assignment = std::move(member.assignment),
            });
        }

        // create instance of old version of group metadata
        group_log_group_metadata_v0 metadata_v0{
          .protocol_type = std::move(data.protocol_type),
          .generation = data.generation,
          .protocol = std::move(data.protocol),
          .leader = std::move(data.leader),
          .state_timestamp = data.state_timestamp,
          .members = std::move(members_v0),
        };

        // this puts a version on disk that older code can read
        serialize(out, std::move(metadata_v0));

        // now append the client host/id information for new code
        std::vector<client_host_id> client_info;
        client_info.reserve(data.members.size());
        for (auto& member : data.members) {
            client_info.push_back(client_host_id{
              .id = std::move(member.client_id),
              .host = std::move(member.client_host),
            });
        }
        serialize(out, std::move(client_info));
    }

    kafka::group_log_group_metadata from(iobuf_parser& in) {
        auto metadata_v0 = adl<group_log_group_metadata_v0>{}.from(in);

        std::vector<client_host_id> client_info;
        if (in.bytes_left()) {
            client_info = adl<std::vector<client_host_id>>{}.from(in);
            vassert(
              client_info.size() == metadata_v0.members.size(),
              "Expected client info size {} got {}",
              metadata_v0.members.size(),
              client_info.size());
        }

        std::vector<kafka::member_state> members_out;
        members_out.reserve(metadata_v0.members.size());

        for (auto& member : metadata_v0.members) {
            members_out.push_back(kafka::member_state{
              .id = std::move(member.id),
              .session_timeout = member.session_timeout,
              .rebalance_timeout = member.rebalance_timeout,
              .instance_id = std::move(member.instance_id),
              .protocol_type = std::move(member.protocol_type),
              .protocols = std::move(member.protocols),
              .assignment = std::move(member.assignment),
            });
        }

        for (size_t i = 0; i < client_info.size(); i++) {
            members_out[i].client_id = std::move(client_info[i].id);
            members_out[i].client_host = std::move(client_info[i].host);
        }

        kafka::group_log_group_metadata metadata_out{
          .protocol_type = std::move(metadata_v0.protocol_type),
          .generation = metadata_v0.generation,
          .protocol = std::move(metadata_v0.protocol),
          .leader = std::move(metadata_v0.leader),
          .state_timestamp = metadata_v0.state_timestamp,
          .members = std::move(members_out),
        };

        return metadata_out;
    }
};

} // namespace reflection

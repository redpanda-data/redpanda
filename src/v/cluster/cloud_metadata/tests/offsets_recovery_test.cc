/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#include "cloud_storage/remote_file.h"
#include "cloud_storage/remote_segment.h"
#include "cloud_storage/tests/manual_fixture.h"
#include "cloud_storage/tests/produce_utils.h"
#include "cloud_storage/tests/s3_imposter.h"
#include "cloud_storage/types.h"
#include "cluster/archival/archival_metadata_stm.h"
#include "cluster/cloud_metadata/cluster_manifest.h"
#include "cluster/cloud_metadata/error_outcome.h"
#include "cluster/cloud_metadata/offsets_lookup_batcher.h"
#include "cluster/cloud_metadata/offsets_recoverer.h"
#include "cluster/cloud_metadata/offsets_recovery_manager.h"
#include "cluster/cloud_metadata/offsets_recovery_router.h"
#include "cluster/cloud_metadata/offsets_snapshot.h"
#include "cluster/cloud_metadata/offsets_upload_router.h"
#include "cluster/cloud_metadata/offsets_uploader.h"
#include "cluster/cloud_metadata/tests/cluster_metadata_utils.h"
#include "cluster/cloud_metadata/tests/manual_mixin.h"
#include "cluster/cloud_metadata/uploader.h"
#include "cluster/commands.h"
#include "cluster/controller.h"
#include "cluster/types.h"
#include "config/node_config.h"
#include "kafka/client/client.h"
#include "kafka/client/configuration.h"
#include "kafka/client/consumer.h"
#include "kafka/protocol/types.h"
#include "kafka/server/group.h"
#include "kafka/server/group_manager.h"
#include "kafka/server/rm_group_frontend.h"
#include "kafka/server/tests/produce_consume_utils.h"
#include "model/fundamental.h"
#include "model/namespace.h"
#include "redpanda/tests/fixture.h"
#include "storage/types.h"
#include "test_utils/async.h"

#include <seastar/core/io_priority_class.hh>
#include <seastar/util/later.hh>

#include <absl/container/flat_hash_map.h>
#include <boost/test/tools/old/interface.hpp>

namespace {
ss::logger logger("offsets_recovery_test");
static ss::abort_source never_abort;
const model::topic topic_name{"oreo"};
const std::vector<model::ntp> offset_ntps = [] {
    std::vector<model::ntp> ntps;
    for (int i = 0; i < 16; i++) {
        ntps.emplace_back(
          model::kafka_namespace,
          model::kafka_consumer_offsets_topic,
          model::partition_id(i));
    }
    return ntps;
}();

} // anonymous namespace

namespace kc = kafka::client;
using namespace cluster::cloud_metadata;

class offsets_recovery_fixture
  : public cloud_storage_manual_multinode_test_base {
public:
    offsets_recovery_fixture()
      : bucket(cloud_storage_clients::bucket_name("test-bucket")) {
        RPTEST_REQUIRE_EVENTUALLY(5s, [this] {
            return app.storage.local().get_cluster_uuid().has_value();
        });
        cluster_uuid = app.storage.local().get_cluster_uuid().value();
    }
    ss::future<std::vector<cluster::partition*>> make_partitions(int n) {
        cluster::topic_properties props;
        props.shadow_indexing = model::shadow_indexing_mode::full;
        props.retention_local_target_bytes = tristate<size_t>(1);
        props.cleanup_policy_bitflags
          = model::cleanup_policy_bitflags::deletion;
        co_await add_topic({model::kafka_namespace, topic_name}, n, props);
        std::vector<cluster::partition*> ps;
        for (int i = 0; i < n; i++) {
            auto ntp = model::ntp(
              model::kafka_namespace, topic_name, model::partition_id{i});
            co_await wait_for_leader(ntp);
            ps.emplace_back(app.partition_manager.local().get(ntp).get());
        }
        co_return ps;
    }

    kc::client make_client() { return kc::client{proxy_client_config()}; }

    kc::client make_connected_client() {
        auto client = make_client();
        client.config().retry_base_backoff.set_value(10ms);
        client.config().retries.set_value(size_t(10));
        client.connect().get();
        return client;
    }

    // Construct group ids with the given prefix.
    static std::vector<kafka::group_id>
    group_ids(ss::sstring prefix, int num_groups) {
        std::vector<kafka::group_id> groups;
        groups.reserve(num_groups);
        for (int i = 0; i < num_groups; i++) {
            groups.emplace_back(
              kafka::group_id{fmt::format("{}_{}", prefix, i)});
        }
        return groups;
    }

    // Sends out requests creating the given groups, each with one member,
    // returning the member IDs for each.
    ss::future<absl::flat_hash_map<kafka::group_id, kafka::member_id>>
    create_groups(
      kc::client& client,
      std::vector<kafka::group_id> groups,
      redpanda_thread_fixture* second_fixture = nullptr) {
        std::vector<ss::future<kafka::member_id>> ms;
        ms.reserve(groups.size());
        for (const auto& g : groups) {
            ms.emplace_back(client.create_consumer(g));
        }
        auto res = co_await ss::when_all_succeed(ms.begin(), ms.end());
        RPTEST_REQUIRE_EVENTUALLY_CORO(10s, [&, second_fixture] {
            size_t listed_groups
              = app._group_manager.local().list_groups().second.size();
            if (second_fixture) {
                listed_groups += second_fixture->app._group_manager.local()
                                   .list_groups()
                                   .second.size();
            }
            return listed_groups == groups.size();
        });
        absl::flat_hash_map<kafka::group_id, kafka::member_id> ret;
        for (int i = 0; i < groups.size(); i++) {
            ret[groups[i]] = res[i];
            co_await client.subscribe_consumer(groups[i], res[i], {});
        }
        co_return ret;
    }

    // Commits a random offset per member in each group.
    ss::future<absl::flat_hash_map<kafka::group_id, int>> commit_random_offsets(
      kc::client& client,
      const absl::flat_hash_map<kafka::group_id, kafka::member_id>& members) {
        absl::flat_hash_map<kafka::group_id, int> committed_offsets;
        for (const auto& [gid, mid] : members) {
            // Commit an offset for each group.
            auto o = random_generators::get_int(0, 100);
            committed_offsets[gid] = o;
            auto t = kafka::offset_commit_request_topic{
              .name = topic_name,
              .partitions = {
                {.partition_index = model::partition_id{0},
                 .committed_offset = model::offset{o},
                 .committed_metadata{mid()}}}};
            auto res = co_await client.consumer_offset_commit(
              gid, mid, {std::move(t)});

            // Sanity checks the result: only one partition committed per
            // group, no errors.
            BOOST_REQUIRE_EQUAL(res.data.topics.size(), 1);
            auto topic_res = res.data.topics[0];
            BOOST_REQUIRE_EQUAL(topic_res.partitions.size(), 1);
            auto& p = topic_res.partitions[0];
            BOOST_REQUIRE_EQUAL(p.partition_index, 0);
            BOOST_REQUIRE_EQUAL(p.error_code, kafka::error_code::none);
        }
        co_return committed_offsets;
    }

    // Returns the number of groups managed by each offsets ntp. Expects that
    // the group manager for each NTP has already loaded the partition, and
    // that the shard-local partition is leader.
    absl::flat_hash_map<model::ntp, size_t> snap_num_group_per_offsets_ntp() {
        auto& gm = app._group_manager.local();
        absl::flat_hash_map<model::ntp, size_t> groups_per_ntp;
        for (const auto& ntp : offset_ntps) {
            auto res = gm.snapshot_groups(ntp).get();
            BOOST_REQUIRE(res.has_value());
            for (const auto& snap : res.value()) {
                groups_per_ntp[ntp] += snap.groups.size();
            }
        }
        return groups_per_ntp;
    }

    // Creates the specified number of groups and commits one offset per group,
    // returning maps containing the underlying offsets topic partition,
    // committed offsets, and associated groups.
    auto create_groups_and_commit(size_t num_groups) {
        auto client = make_connected_client();
        auto stop_client = ss::defer([&client]() { client.stop().get(); });

        auto groups = group_ids("test_group", num_groups);
        auto members = create_groups(client, groups).get();
        auto committed_offsets = commit_random_offsets(client, members).get();
        auto num_groups_per_ntp = snap_num_group_per_offsets_ntp();
        return std::make_tuple(num_groups_per_ntp, committed_offsets);
    }

    // Repeatedly checks that each offsets NTP manages the exact count of
    // groups as in `groups_per_ntp`, verifying that all reported errors are in
    // `allowed_errors`.
    ss::future<> validate_group_counts_in_loop(
      absl::flat_hash_map<model::ntp, size_t> groups_per_ntp,
      absl::flat_hash_set<cluster::cloud_metadata::error_outcome>
        allowed_errors,
      ss::gate& gate) {
        auto& gm = app._group_manager.local();
        auto holder = gate.hold();
        while (!gate.is_closed()) {
            for (const auto& ntp : offset_ntps) {
                auto res = co_await gm.snapshot_groups(ntp);
                if (res.has_value()) {
                    size_t num_groups = 0;
                    for (const auto& snap : res.value()) {
                        num_groups += snap.groups.size();
                    }
                    BOOST_REQUIRE_EQUAL(groups_per_ntp[ntp], num_groups);
                } else {
                    BOOST_REQUIRE(allowed_errors.contains(res.error()));
                }
                co_await ss::maybe_yield();
            }
            co_await ss::maybe_yield();
        }
    }

    // Returns true if the number of groups of the group manager partitions
    // match those in `groups_per_ntp` exactly.
    ss::future<bool> validate_group_counts_exactly(
      absl::flat_hash_map<model::ntp, size_t> groups_per_ntp) {
        for (const auto& ntp : offset_ntps) {
            auto snaps = co_await app._group_manager.local().snapshot_groups(
              ntp);
            if (!snaps.has_value()) {
                co_return false;
            }
            size_t snapshotted_num_groups = 0;
            for (const auto& snap : snaps.value()) {
                snapshotted_num_groups += snap.groups.size();
            }
            if (snapshotted_num_groups != groups_per_ntp.at(ntp)) {
                co_return false;
            }
        }
        co_return true;
    }

    // Asserts that the group counts per offsets NTP eventually match those
    // given by `groups_per_ntp`.
    ss::future<> validate_group_counts_eventually(
      const absl::flat_hash_map<model::ntp, size_t>& groups_per_ntp) {
        RPTEST_REQUIRE_EVENTUALLY_CORO(
          5s, [&] { return validate_group_counts_exactly(groups_per_ntp); });
    }

    // Uploads the offsets from each offsets topic partition, returning the
    // resulting remote paths.
    ss::future<std::vector<std::vector<cloud_storage::remote_segment_path>>>
    upload_offsets() {
        retry_chain_node retry_node(
          never_abort, ss::lowres_clock::time_point::max(), 10ms);
        offsets_uploader uploader(
          bucket, app._group_manager, app.cloud_storage_api);
        std::vector<std::vector<cloud_storage::remote_segment_path>>
          paths_per_pid;
        paths_per_pid.resize(offset_ntps.size());
        for (const auto& ntp : offset_ntps) {
            std::vector<cloud_storage::remote_segment_path> remote_paths;
            auto res = co_await uploader.upload(
              cluster_uuid, ntp, cluster_metadata_id{0}, retry_node);
            BOOST_REQUIRE(!res.has_error());
            for (auto& p : res.value().paths) {
                remote_paths.emplace_back(std::move(p));
            }
            paths_per_pid[ntp.tp.partition()] = std::move(remote_paths);
        }
        co_return paths_per_pid;
    }

    ss::future<> validate_downloaded_offsets(
      redpanda_thread_fixture& fixture,
      const absl::flat_hash_map<kafka::group_id, int>& committed_offsets,
      const std::vector<cloud_storage::remote_segment_path>& remote_paths) {
        retry_chain_node retry_node(
          never_abort, ss::lowres_clock::time_point::max(), 10ms);
        absl::flat_hash_map<kafka::group_id, int> downloaded_offsets;
        for (const auto& p : remote_paths) {
            cloud_storage::remote_file rf(
              fixture.app.cloud_storage_api.local(),
              fixture.app.shadow_index_cache.local(),
              bucket,
              p,
              retry_node,
              "offsets file");
            auto f = co_await rf.hydrate_readable_file();
            auto f_size = co_await f.size();
            ss::file_input_stream_options options;
            auto input = ss::make_file_input_stream(f, options);
            auto snap_buf_parser = iobuf_parser{
              co_await read_iobuf_exactly(input, f_size)};
            auto snapshot = serde::read<group_offsets_snapshot>(
              snap_buf_parser);
            for (const auto& g : snapshot.groups) {
                const auto gid = kafka::group_id{g.group_id};
                BOOST_REQUIRE_EQUAL(1, g.offsets.size());
                downloaded_offsets[gid] = g.offsets[0].partitions[0].offset();
            }
        }
        BOOST_REQUIRE_EQUAL(committed_offsets, downloaded_offsets);
    }

protected:
    const cloud_storage_clients::bucket_name bucket;
    model::cluster_uuid cluster_uuid;
};

FIXTURE_TEST(test_snapshot_basic, offsets_recovery_fixture) {
    make_partitions(1).get();

    // Create a client and a bunch of groups.
    auto client = make_connected_client();
    auto stop_client = ss::defer([&client]() { client.stop().get(); });
    constexpr int num_groups = 30;
    auto groups = group_ids("test_group", num_groups);
    auto members = create_groups(client, groups).get();
    BOOST_REQUIRE_EQUAL(groups.size(), members.size());

    // Sanity check that snapshotting with no offsets results in expectedly
    // empty snapshots.
    size_t snapped_groups = 0;
    for (const auto& ntp : offset_ntps) {
        auto snap = app._group_manager.local().snapshot_groups(ntp).get();
        BOOST_REQUIRE(snap.has_value());
        BOOST_REQUIRE_EQUAL(snap.value().size(), 1);
        snapped_groups += snap.value()[0].groups.size();

        // All groups are empty since we haven't committed anything.
        for (const auto& g : snap.value()[0].groups) {
            BOOST_REQUIRE_EQUAL(0, g.offsets.size());
        }
    }
    BOOST_REQUIRE_EQUAL(snapped_groups, num_groups);
    auto committed_offsets = commit_random_offsets(client, members).get();

    // Now snapshot again and ensure that the correct offsets were snapshotted.
    snapped_groups = 0;
    for (const auto& ntp : offset_ntps) {
        auto snap = app._group_manager.local().snapshot_groups(ntp).get();
        BOOST_REQUIRE(snap.has_value());
        BOOST_REQUIRE_EQUAL(snap.value().size(), 1);
        snapped_groups += snap.value()[0].groups.size();

        // All groups should have committed one offset.
        for (const auto& g : snap.value()[0].groups) {
            BOOST_REQUIRE_EQUAL(1, g.offsets.size());
            BOOST_REQUIRE_EQUAL(
              g.offsets[0].partitions[0].offset(),
              committed_offsets[kafka::group_id{g.group_id}]);
        }
    }
    BOOST_REQUIRE_EQUAL(snapped_groups, num_groups);
}

FIXTURE_TEST(test_snapshot_leadership_change, offsets_recovery_fixture) {
    make_partitions(1).get();
    auto [groups_per_ntp, _] = create_groups_and_commit(30);

    // Repeatedly snapshot while we undergo some leadership changes.
    ss::gate g;
    auto validate_fut = validate_group_counts_in_loop(
      groups_per_ntp, {cluster::cloud_metadata::error_outcome::not_ready}, g);
    auto finish = ss::defer([&] {
        g.close().get();
        validate_fut.get();
    });

    // Step down from each partition while validation is running.
    for (const auto& ntp : offset_ntps) {
        auto p = app.partition_manager.local().get(ntp);
        p->raft()->step_down("test").get();
    }
    validate_group_counts_eventually(groups_per_ntp).get();
}

FIXTURE_TEST(test_snapshot_group_removal, offsets_recovery_fixture) {
    make_partitions(1).get();

    // Create a client and a bunch of groups.
    auto client = make_connected_client();
    auto stop_client = ss::defer([&client]() { client.stop().get(); });
    constexpr int num_groups = 30;
    auto groups = group_ids("test_group", num_groups);
    auto members = create_groups(client, groups).get();
    BOOST_REQUIRE_EQUAL(groups.size(), members.size());

    auto committed_offsets = commit_random_offsets(client, members).get();
    auto& gm = app._group_manager.local();
    auto groups_per_ntp = snap_num_group_per_offsets_ntp();
    std::vector<std::pair<model::ntp, kafka::group_id>> to_delete;
    for (const auto& ntp : offset_ntps) {
        auto snap = gm.snapshot_groups(ntp).get();
        BOOST_REQUIRE(snap.has_value());
        BOOST_REQUIRE_EQUAL(snap.value().size(), 1);
        auto& groups = snap.value()[0].groups;
        if (groups.empty()) {
            // NTP manages no groups.
            continue;
        }
        if (groups[0].offsets.empty()) {
            // Group contains no offsets.
            continue;
        }

        auto group_to_delete = kafka::group_id{groups[0].group_id};

        // Have the member leave the group so the group can be deleted.
        client.remove_consumer(group_to_delete, members[group_to_delete]).get();
        to_delete.emplace_back(std::make_pair(ntp, group_to_delete));
        groups_per_ntp[ntp]--;
    }
    // Send the deletion request.
    BOOST_REQUIRE(!to_delete.empty());
    auto num_deleted = to_delete.size();
    auto results = gm.delete_groups(std::move(to_delete)).get();
    BOOST_REQUIRE_EQUAL(results.size(), num_deleted);
    for (const auto& res : results) {
        BOOST_REQUIRE_MESSAGE(!res.errored(), res);
    }
    // Assert that we eventually get to the newly subtracted counts.
    validate_group_counts_eventually(groups_per_ntp).get();
}

FIXTURE_TEST(test_upload_offsets, offsets_recovery_fixture) {
    make_partitions(1).get();
    auto [_, committed_offsets] = create_groups_and_commit(30);

    // Upload the offsets.
    auto paths_per_pid = upload_offsets().get();
    std::vector<cloud_storage::remote_segment_path> remote_paths;
    for (const auto& paths : paths_per_pid) {
        remote_paths.insert(remote_paths.end(), paths.begin(), paths.end());
    }

    // Download each snapshot and collect their committed offsets, ensuring
    // they are equivalent to what we started with.
    validate_downloaded_offsets(*this, committed_offsets, remote_paths).get();
}

FIXTURE_TEST(test_upload_offsets_batched, offsets_recovery_fixture) {
    make_partitions(1).get();
    scoped_config cfg;
    cfg.get("cloud_storage_cluster_metadata_num_consumer_groups_per_upload")
      .set_value(size_t(1));
    auto [_, committed_offsets] = create_groups_and_commit(30);

    // Upload the offsets.
    auto paths_per_pid = upload_offsets().get();
    std::vector<cloud_storage::remote_segment_path> remote_paths;
    size_t num_uploads = 0;
    for (const auto& paths : paths_per_pid) {
        remote_paths.insert(remote_paths.end(), paths.begin(), paths.end());
        num_uploads += paths.size();
    }
    // Because we uploaded in group batches of size 1, there should be at least
    // the same number of groups as there are uploads (not exact equality, to
    // account for offsets partitions that were empty).
    BOOST_REQUIRE_GE(num_uploads, 30);
    validate_downloaded_offsets(*this, committed_offsets, remote_paths).get();
}

FIXTURE_TEST(test_local_recovery, offsets_recovery_fixture) {
    make_partitions(1).get();
    constexpr const auto num_groups = 30;
    auto [groups_per_ntp, committed_offsets] = create_groups_and_commit(
      num_groups);
    auto remote_paths = upload_offsets().get();
    BOOST_REQUIRE_EQUAL(16, remote_paths.size());

    // Wipe the cluster and restore the groups from the snapshot.
    info("Clearing cluster...");
    restart(should_wipe::yes);
    make_partitions(1).get();
    BOOST_REQUIRE(kafka::try_create_consumer_group_topic(
                    app.coordinator_ntp_mapper.local(),
                    app.controller->get_topics_frontend().local(),
                    1)
                    .get());

    // Wait for the group_manager to report healthy (but empty).
    absl::flat_hash_map<model::ntp, size_t> empty_group_map;
    for (const auto& ntp : offset_ntps) {
        wait_for_leader(ntp).get();
        empty_group_map[ntp] = 0;
    }
    validate_group_counts_eventually(empty_group_map).get();

    // Now run recovery across the partitions using the snapshots.
    offsets_recoverer recoverer(
      config::node().node_id().value(),
      app.cloud_storage_api,
      app.shadow_index_cache,
      app.offsets_lookup,
      app.controller->get_partition_leaders(),
      app._connection_cache,
      app._group_manager);
    auto cleanup = ss::defer([&] { recoverer.stop().get(); });
    for (int i = 0; i < remote_paths.size(); i++) {
        offsets_recovery_request req;
        req.offsets_ntp = {
          model::kafka_consumer_offsets_nt.ns,
          model::kafka_consumer_offsets_nt.tp,
          model::partition_id(i),
        };
        req.bucket = bucket;
        BOOST_REQUIRE_EQUAL(remote_paths[i].size(), 1);
        req.offsets_snapshot_paths.emplace_back(remote_paths[i][0]());
        auto small_batch_size = 1;
        auto reply = recoverer.recover(std::move(req), small_batch_size).get();
        BOOST_REQUIRE_EQUAL(reply.ec, cluster::errc::success);
    }
    validate_group_counts_eventually(groups_per_ntp).get();
    size_t num_groups_total = 0;
    for (const auto& [_, num_groups] : groups_per_ntp) {
        num_groups_total += num_groups;
    }
    BOOST_REQUIRE_EQUAL(num_groups_total, num_groups);
}

FIXTURE_TEST(
  test_offsets_recoverer_skips_existing_groups, offsets_recovery_fixture) {
    make_partitions(1).get();
    constexpr const auto num_groups = 30;
    auto [groups_per_ntp, committed_offsets] = create_groups_and_commit(
      num_groups);
    auto remote_paths = upload_offsets().get();
    BOOST_REQUIRE_EQUAL(16, remote_paths.size());

    // Wipe the cluster and restore the groups from the snapshot.
    restart(should_wipe::yes);
    make_partitions(1).get();
    BOOST_REQUIRE(kafka::try_create_consumer_group_topic(
                    app.coordinator_ntp_mapper.local(),
                    app.controller->get_topics_frontend().local(),
                    1)
                    .get());

    // Wait for the group_manager to report healthy (but empty).
    absl::flat_hash_map<model::ntp, size_t> empty_group_map;
    for (const auto& ntp : offset_ntps) {
        wait_for_leader(ntp).get();
        empty_group_map[ntp] = 0;
    }
    validate_group_counts_eventually(empty_group_map).get();

    // Create the same set of groups without committing any offsets.
    auto client = make_connected_client();
    auto stop_client = ss::defer([&client]() { client.stop().get(); });
    auto groups = group_ids("test_group", num_groups);
    auto members = create_groups(client, groups).get();
    validate_group_counts_eventually(groups_per_ntp).get();

    // Now run recovery across the partitions using the snapshots.
    offsets_recoverer recoverer(
      config::node().node_id().value(),
      app.cloud_storage_api,
      app.shadow_index_cache,
      app.offsets_lookup,
      app.controller->get_partition_leaders(),
      app._connection_cache,
      app._group_manager);
    auto cleanup = ss::defer([&] { recoverer.stop().get(); });
    for (int i = 0; i < remote_paths.size(); i++) {
        offsets_recovery_request req;
        req.offsets_ntp = {
          model::kafka_consumer_offsets_nt.ns,
          model::kafka_consumer_offsets_nt.tp,
          model::partition_id(i),
        };
        req.bucket = bucket;
        BOOST_REQUIRE_EQUAL(remote_paths[i].size(), 1);
        req.offsets_snapshot_paths.emplace_back(remote_paths[i][0]());
        auto reply = recoverer.recover(std::move(req)).get();
        BOOST_REQUIRE_EQUAL(reply.ec, cluster::errc::success);
    }

    // Because the groups already existed at recovery time, the offsets
    // shouldn't be restored.
    for (const auto& ntp : offset_ntps) {
        auto snapshot_res
          = app._group_manager.local().snapshot_groups(ntp).get();
        BOOST_REQUIRE(snapshot_res.has_value());
        BOOST_REQUIRE_EQUAL(snapshot_res.value().size(), 1);
        for (const auto& group : snapshot_res.value()[0].groups) {
            BOOST_REQUIRE(group.offsets.empty());
        }
    }
}

FIXTURE_TEST(test_upload_dispatch_to_leaders, offsets_recovery_fixture) {
    make_partitions(1).get();

    // Start a second fixture. Note it's unimportant for the partitions to be
    // hosted by particular nodes; they just need to exist so we can commit
    // offsets referencing them.
    auto other_fx = start_second_fixture();
    RPTEST_REQUIRE_EVENTUALLY(3s, [this] {
        return app.controller->get_members_table().local().node_ids().size()
               == 2;
    });

    auto client = make_connected_client();
    auto stop_client = ss::defer([&client]() { client.stop().get(); });
    constexpr int num_groups = 30;
    auto groups = group_ids("test_group", num_groups);
    auto members = create_groups(client, groups, other_fx.get()).get();
    BOOST_REQUIRE_EQUAL(groups.size(), members.size());

    auto committed_offsets = commit_random_offsets(client, members).get();

    // Dispatch upload requests from both fixtures. THe resulting uploaded
    // snapshots should be identical to what was committed.
    int meta_id = 0;
    for (auto* fx :
         std::vector<redpanda_thread_fixture*>{this, other_fx.get()}) {
        std::vector<cloud_storage::remote_segment_path> remote_paths;
        for (const auto& ntp : offset_ntps) {
            offsets_upload_request upload_req;
            upload_req.offsets_ntp = ntp;
            upload_req.meta_id = cluster::cloud_metadata::cluster_metadata_id{
              meta_id++};
            auto upload_resp = fx->app.offsets_upload_router.local()
                                 .process_or_dispatch(upload_req, ntp, 5s)
                                 .get();
            BOOST_REQUIRE_EQUAL(cluster::errc::success, upload_resp.ec);
            BOOST_REQUIRE(!upload_resp.uploaded_paths.empty());
            for (const auto& p : upload_resp.uploaded_paths) {
                remote_paths.emplace_back(p);
            }
        }
        validate_downloaded_offsets(*fx, committed_offsets, remote_paths).get();
    }
}

FIXTURE_TEST(test_controller_upload_offsets, offsets_recovery_fixture) {
    make_partitions(1).get();
    scoped_config test_local_cfg;
    test_local_cfg.get("cloud_storage_cluster_metadata_upload_interval_ms")
      .set_value(5000ms);

    const auto [_, committed_offsets] = create_groups_and_commit(30);

    // Set up some metadata: a controller snapshot and some offsets.
    auto& controller_stm = app.controller->get_controller_stm().local();
    RPTEST_REQUIRE_EVENTUALLY(
      5s, [&] { return controller_stm.maybe_write_snapshot(); });

    // Now begin uploading metadata.
    auto& uploader = app.controller->metadata_uploader().value().get();
    cluster::consensus_ptr raft0
      = app.partition_manager.local().get(model::controller_ntp)->raft();
    RPTEST_REQUIRE_EVENTUALLY(5s, [&] { return raft0->is_leader(); });

    auto upload_in_term = uploader.upload_until_term_change();
    cluster_metadata_manifest manifest;
    RPTEST_REQUIRE_EVENTUALLY(30s, [&] {
        auto manifest_opt = uploader.manifest();
        if (!manifest_opt.has_value()) {
            return false;
        }
        const auto& m = manifest_opt.value().get();
        if (m.offsets_snapshots_by_partition.empty()) {
            return false;
        }
        if (m.metadata_id() < 3) {
            return false;
        }
        manifest = m;
        return true;
    });
    BOOST_REQUIRE_EQUAL(
      manifest.offsets_snapshots_by_partition.size(), offset_ntps.size());
    for (const auto& snap : manifest.offsets_snapshots_by_partition) {
        BOOST_REQUIRE_EQUAL(1, snap.size());
    }
    raft0->step_down("test").get();
    upload_in_term.get();
    std::vector<cloud_storage::remote_segment_path> paths_in_manifest;
    paths_in_manifest.reserve(manifest.offsets_snapshots_by_partition.size());
    for (const auto& remote_paths : manifest.offsets_snapshots_by_partition) {
        paths_in_manifest.emplace_back(
          cloud_storage::remote_segment_path{remote_paths[0]});
    }
    // We should still be able to access the remote files in the manifest.
    validate_downloaded_offsets(*this, committed_offsets, paths_in_manifest)
      .get();

    auto reqs = get_requests();
    int num_deleted_offsets_snapshots = 0;
    for (const auto& req : reqs) {
        if (req.method != "DELETE") {
            continue;
        }
        // None of the paths in the manifest should be deleted.
        for (const auto& p : paths_in_manifest) {
            BOOST_REQUIRE_NE(p(), req.url);
        }
        // On the other hand, the uploader should have been deleting stale
        // offsets snapshots.
        if (req.url.contains("/offsets/")) {
            num_deleted_offsets_snapshots++;
        }
    }
    BOOST_REQUIRE_GT(num_deleted_offsets_snapshots, 0);
}

FIXTURE_TEST(test_recover_offsets, offsets_recovery_fixture) {
    // Test that from an offset snapshot, we're able to recover.
    make_partitions(1).get();

    // Simulate a large enough number of groups to upload multiple snapshots
    // per partition.
    scoped_config cfg;
    cfg.get("cloud_storage_cluster_metadata_num_consumer_groups_per_upload")
      .set_value(size_t(1));
    auto [num_groups_per_ntp, committed_offsets] = create_groups_and_commit(30);

    auto paths_per_pid = upload_offsets().get();
    BOOST_REQUIRE_EQUAL(paths_per_pid.size(), 16);
    std::vector<cloud_storage::remote_segment_path> remote_paths;
    size_t num_uploads = 0;
    for (const auto& paths : paths_per_pid) {
        remote_paths.insert(remote_paths.end(), paths.begin(), paths.end());
        num_uploads += paths.size();
    }
    BOOST_REQUIRE_GE(num_uploads, 30);

    // Restart empty.
    restart(should_wipe::yes);
    RPTEST_REQUIRE_EVENTUALLY(5s, [this] {
        return app.storage.local().get_cluster_uuid().has_value();
    });

    // Cheat a little here: instead of restoring partitions, we'll just produce
    // to new partitions to fake their offsets.
    auto new_cluster_uuid = app.storage.local().get_cluster_uuid().value();
    BOOST_REQUIRE_NE(new_cluster_uuid(), cluster_uuid());
    make_partitions(1).get();
    tests::kafka_produce_transport produce(make_kafka_client().get());
    produce.start().get();
    int64_t num_records = 5;
    produce
      .produce_to_partition(
        topic_name,
        model::partition_id(0),
        tests::kv_t::sequence(0, num_records))
      .get();

    // Begin recovery.
    offsets_recovery_manager recovery_mgr(
      app.offsets_recovery_router,
      app.coordinator_ntp_mapper,
      app.controller->get_members_table(),
      app.controller->get_api(),
      app.controller->get_topics_frontend());
    retry_chain_node retry_node(never_abort, 30s, 10ms);
    auto err = recovery_mgr
                 .recover(retry_node, bucket, std::move(paths_per_pid))
                 .get();
    BOOST_REQUIRE_EQUAL(err, error_outcome::success);

    RPTEST_REQUIRE_EVENTUALLY(10s, [&] {
        // Wait for all partitions to finish loading.
        const auto [err, l] = app._group_manager.local().list_groups();
        return err == kafka::error_code::none;
    });
    validate_group_counts_exactly(num_groups_per_ntp).get();
    absl::flat_hash_map<kafka::group_id, int> restored_offsets_per_group;
    for (const auto& ntp : offset_ntps) {
        auto snap = app._group_manager.local().snapshot_groups(ntp).get();
        BOOST_REQUIRE(snap.has_value());

        // All groups should have committed one offset.
        for (const auto& g : snap.value()[0].groups) {
            BOOST_REQUIRE_EQUAL(1, g.offsets.size());
            BOOST_REQUIRE_EQUAL(1, g.offsets[0].partitions.size());

            // Trim the expected offset.
            int64_t expected_offset
              = committed_offsets[kafka::group_id{g.group_id}];
            expected_offset = std::min(expected_offset, num_records);
            BOOST_REQUIRE_EQUAL(
              g.offsets[0].partitions[0].offset(), expected_offset);
        }
    }

    restart(should_wipe::no);
    RPTEST_REQUIRE_EVENTUALLY(10s, [&] {
        const auto [err, l] = app._group_manager.local().list_groups();
        return err == kafka::error_code::none;
    });
    validate_group_counts_exactly(num_groups_per_ntp).get();
}

FIXTURE_TEST(test_cluster_recovery_with_offsets, offsets_recovery_fixture) {
    make_partitions(1).get();
    auto client = make_connected_client();
    auto stop_client = ss::defer([&client]() { client.stop().get(); });

    // Create a bunch of groups.
    constexpr int num_groups = 30;
    auto groups = group_ids("test_group", num_groups);
    auto members = create_groups(client, groups).get();
    auto committed_offsets = commit_random_offsets(client, members).get();
    auto num_groups_per_ntp = snap_num_group_per_offsets_ntp();
    client.stop().get();
    stop_client.cancel();

    // Upload a controller snapshot from which we will restore.
    auto& controller_stm = app.controller->get_controller_stm().local();
    RPTEST_REQUIRE_EVENTUALLY(
      5s, [&controller_stm] { return controller_stm.maybe_write_snapshot(); });
    auto raft0
      = app.partition_manager.local().get(model::controller_ntp)->raft();
    auto& uploader = app.controller->metadata_uploader().value().get();
    retry_chain_node retry_node(never_abort, 30s, 1s);
    cluster_metadata_manifest manifest;
    manifest.cluster_uuid = cluster_uuid;
    uploader.upload_next_metadata(raft0->confirmed_term(), manifest, retry_node)
      .get();
    BOOST_REQUIRE_EQUAL(manifest.metadata_id, cluster_metadata_id(0));
    BOOST_REQUIRE(!manifest.controller_snapshot_path.empty());
    BOOST_REQUIRE_EQUAL(16, manifest.offsets_snapshots_by_partition.size());

    // Clear the cluster and restore.
    raft0 = nullptr;
    restart(should_wipe::yes);
    RPTEST_REQUIRE_EVENTUALLY(5s, [this] {
        return app.storage.local().get_cluster_uuid().has_value();
    });

    auto recover_err = app.controller->get_cluster_recovery_manager()
                         .local()
                         .initialize_recovery(bucket)
                         .get();
    BOOST_REQUIRE(recover_err.has_value());
    BOOST_REQUIRE_EQUAL(recover_err.value(), cluster::errc::success);
    RPTEST_REQUIRE_EVENTUALLY(30s, [this] {
        auto latest_recovery = app.controller->get_cluster_recovery_table()
                                 .local()
                                 .current_recovery();
        return latest_recovery.has_value()
               && latest_recovery.value().get().stage
                    == cluster::recovery_stage::complete;
    });
    auto controller_prt = app.partition_manager.local().get(
      model::controller_ntp);
    raft0 = controller_prt->raft();
    validate_group_counts_exactly(num_groups_per_ntp).get();

    auto stages = read_recovery_stages(*controller_prt).get();
    auto expected_stages = std::vector<cluster::recovery_stage>{
      cluster::recovery_stage::initialized,
      cluster::recovery_stage::recovered_cluster_config,
      cluster::recovery_stage::recovered_remote_topic_data,
      cluster::recovery_stage::recovered_controller_snapshot,
      cluster::recovery_stage::recovered_offsets_topic,
      cluster::recovery_stage::recovered_tx_coordinator,
      cluster::recovery_stage::complete};
    BOOST_REQUIRE_EQUAL(stages, expected_stages);
}

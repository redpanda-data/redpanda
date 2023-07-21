/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "archival/ntp_archiver_service.h"
#include "cloud_storage/remote.h"
#include "cloud_storage/spillover_manifest.h"
#include "cloud_storage/tests/produce_utils.h"
#include "cloud_storage/tests/s3_imposter.h"
#include "cluster/types.h"
#include "config/configuration.h"
#include "kafka/server/tests/produce_consume_utils.h"
#include "model/fundamental.h"
#include "redpanda/tests/fixture.h"
#include "storage/disk_log_impl.h"

#include <seastar/core/io_priority_class.hh>

#include <boost/algorithm/string/predicate.hpp>

#include <iterator>

using tests::kafka_consume_transport;
using tests::kafka_produce_transport;
using tests::kv_t;

static ss::logger e2e_test_log("e2e_test");

class e2e_fixture
  : public s3_imposter_fixture
  , public redpanda_thread_fixture
  , public enable_cloud_storage_fixture {
public:
    e2e_fixture()
      : redpanda_thread_fixture(
        redpanda_thread_fixture::init_cloud_storage_tag{},
        httpd_port_number()) {
        // No expectations: tests will PUT and GET organically.
        set_expectations_and_listen({});
        wait_for_controller_leadership().get();
    }
};

FIXTURE_TEST(test_produce_consume_from_cloud, e2e_fixture) {
    config::shard_local_cfg()
      .cloud_storage_disable_upload_loop_for_tests.set_value(true);
    const model::topic topic_name("tapioca");
    model::ntp ntp(model::kafka_namespace, topic_name, 0);
    cluster::topic_properties props;
    props.shadow_indexing = model::shadow_indexing_mode::full;
    props.retention_local_target_bytes = tristate<size_t>(1);
    add_topic({model::kafka_namespace, topic_name}, 1, props).get();
    wait_for_leader(ntp).get();

    // Do some sanity checks that our partition looks the way we expect (has a
    // log, archiver, etc).
    auto partition = app.partition_manager.local().get(ntp);
    auto* log = dynamic_cast<storage::disk_log_impl*>(
      partition->log().get_impl());
    auto& archiver = partition->archiver().value().get();
    BOOST_REQUIRE(archiver.sync_for_tests().get());

    tests::remote_segment_generator gen(make_kafka_client().get(), *partition);
    BOOST_REQUIRE_EQUAL(3, gen.records_per_batch(3).produce().get());
    BOOST_REQUIRE_EQUAL(2, log->segments().size());
    BOOST_REQUIRE_EQUAL(1, archiver.manifest().size());

    // Compact the local log to GC to the collectible offset.
    ss::abort_source as;
    storage::housekeeping_config housekeeping_conf(
      model::timestamp::min(),
      1,
      log->stm_manager()->max_collectible_offset(),
      ss::default_priority_class(),
      as);
    partition->log().housekeeping(housekeeping_conf).get();
    // NOTE: the storage layer only initially requests eviction; it relies on
    // Raft to write a snapshot and subsequently truncate.
    tests::cooperative_spin_wait_with_timeout(3s, [log] {
        return log->segments().size() == 1;
    }).get();

    // Attempt to consume from the beginning of the log. Since our local log
    // has been truncated, this exercises reading from cloud storage.
    kafka_consume_transport consumer(make_kafka_client().get());
    consumer.start().get();
    auto consumed_records = consumer
                              .consume_from_partition(
                                topic_name,
                                model::partition_id(0),
                                model::offset(0))
                              .get();
    auto records = kv_t::sequence(0, 3);
    BOOST_CHECK_EQUAL(records.size(), consumed_records.size());
    for (int i = 0; i < records.size(); ++i) {
        BOOST_CHECK_EQUAL(records[i].key, consumed_records[i].key);
        BOOST_CHECK_EQUAL(records[i].val, consumed_records[i].val);
    }
}

FIXTURE_TEST(test_produce_consume_from_cloud_with_spillover, e2e_fixture) {
#ifndef _NDEBUG
    config::shard_local_cfg()
      .cloud_storage_disable_upload_loop_for_tests.set_value(true);
    config::shard_local_cfg().cloud_storage_spillover_manifest_size.set_value(
      std::make_optional((size_t)0x1000));

    config::shard_local_cfg().cloud_storage_enable_segment_merging.set_value(
      false);

    config::shard_local_cfg().enable_metrics_reporter.set_value(false);
    config::shard_local_cfg().retention_local_strict.set_value(true);

    const model::topic topic_name("tapioca");
    model::ntp ntp(model::kafka_namespace, topic_name, 0);
    cluster::topic_properties props;
    BOOST_REQUIRE(props.is_compacted() == false);
    props.shadow_indexing = model::shadow_indexing_mode::full;
    props.retention_local_target_bytes = tristate<size_t>(1);
    add_topic({model::kafka_namespace, topic_name}, 1, props).get();
    wait_for_leader(ntp).get();

    // Do some sanity checks that our partition looks the way we expect (has a
    // log, archiver, etc).
    auto partition = app.partition_manager.local().get(ntp);
    auto* log = dynamic_cast<storage::disk_log_impl*>(
      partition->log().get_impl());
    auto archiver_ref = partition->archiver();
    BOOST_REQUIRE(archiver_ref.has_value());
    auto& archiver = archiver_ref.value().get();

    kafka_produce_transport producer(make_kafka_client().get());
    producer.start().get();

    // Produce to partition until the manifest is large enough to trigger
    // spillover
    size_t total_records = 0;
    while (partition->archival_meta_stm()->manifest().segments_metadata_bytes()
           < 12000) {
        vlog(
          e2e_test_log.info,
          "manifest size: {}, producing to partition",
          partition->archival_meta_stm()->manifest().segments_metadata_bytes());
        std::vector<kv_t> records;
        for (size_t i = 0; i < 4; i++) {
            records.emplace_back(
              ssx::sformat("key{}", total_records + i),
              ssx::sformat("val{}", total_records + i));
        }
        producer
          .produce_to_partition(topic_name, model::partition_id(0), records)
          .get();
        total_records += records.size();
        log->flush().get();
        log->force_roll(ss::default_priority_class()).get();

        BOOST_REQUIRE(archiver.sync_for_tests().get());
        archiver.upload_next_candidates().get();
    }
    BOOST_REQUIRE_EQUAL(
      cloud_storage::upload_result::success,
      archiver.upload_manifest("test").get());
    archiver.flush_manifest_clean_offset().get();

    // Create a new segment so we have data to upload.
    vlog(e2e_test_log.info, "Test log has {} segments", log->segments().size());
    vlog(
      e2e_test_log.info,
      "Test manifest size is {} bytes",
      partition->archival_meta_stm()->manifest().segments_metadata_bytes());

    // Wait for storage GC to remove local segments
    tests::cooperative_spin_wait_with_timeout(30s, [log] {
        return log->segments().size() == 1;
    }).get();

    // This should upload several spillover manifests and apply changes to the
    // archival metadata STM.
    BOOST_REQUIRE(archiver.sync_for_tests().get());
    archiver.apply_spillover().get();

    const auto& local_manifest = partition->archival_meta_stm()->manifest();
    auto so = local_manifest.get_start_offset();
    auto ko = local_manifest.get_start_kafka_offset();
    auto archive_so = local_manifest.get_archive_start_offset();
    auto archive_ko = local_manifest.get_archive_start_kafka_offset();
    auto archive_clean = local_manifest.get_archive_clean_offset();

    vlog(
      e2e_test_log.info,
      "new start offset: {}, new start kafka offset: {}, archive start offset: "
      "{}, archive start kafka offset: {}, "
      "archive clean offset: {}",
      so,
      ko,
      archive_so,
      archive_ko,
      archive_clean);

    // Validate uploaded spillover manifest
    vlog(e2e_test_log.info, "Reconciling storage bucket");
    std::map<model::offset, cloud_storage::partition_manifest>
      spillover_manifests;
    for (const auto& [key, req] : get_targets()) {
        if (boost::algorithm::contains(key, "manifest") == false) {
            // Skip segments
            continue;
        }
        if (boost::algorithm::ends_with(key, ".bin")) {
            // Skip regular manifest
            continue;
        }
        if (boost::algorithm::ends_with(key, "topic_manifest.json")) {
            // Skip topic manifests manifest
            continue;
        }
        BOOST_REQUIRE_EQUAL(req.method, "PUT");
        cloud_storage::partition_manifest spm(
          partition->get_ntp_config().ntp(),
          partition->get_ntp_config().get_initial_revision());
        iobuf sbuf;
        sbuf.append(req.content.data(), req.content_length);
        vlog(
          e2e_test_log.debug,
          "Loading manifest {}, {}",
          req.url,
          sbuf.hexdump(100));
        auto sstr = make_iobuf_input_stream(std::move(sbuf));
        spm.update(std::move(sstr)).get();
        auto spm_so = spm.get_start_offset().value_or(model::offset{});
        vlog(
          e2e_test_log.info,
          "Loaded {}, size bytes: {}, num elements: {}",
          key,
          spm.segments_metadata_bytes(),
          spm.size());
        spillover_manifests.insert(std::make_pair(spm_so, std::move(spm)));
    }

    BOOST_REQUIRE(spillover_manifests.size() != 0);
    const auto& last = spillover_manifests.rbegin()->second;
    const auto& first = spillover_manifests.begin()->second;

    BOOST_REQUIRE(model::next_offset(last.get_last_offset()) == so);
    BOOST_REQUIRE(first.get_start_offset().has_value());
    BOOST_REQUIRE(first.get_start_offset().value() == archive_so);
    BOOST_REQUIRE(first.get_start_kafka_offset().has_value());
    BOOST_REQUIRE(first.get_start_kafka_offset().value() == archive_ko);

    model::offset expected_so = archive_so;
    for (const auto& [key, m] : spillover_manifests) {
        std::ignore = key;
        BOOST_REQUIRE(m.get_start_offset().value() == expected_so);
        expected_so = model::next_offset(m.get_last_offset());
    }

    // Consume from start offset of the partition (data available in the STM).
    vlog(e2e_test_log.info, "Consuming from the partition");
    kafka_consume_transport consumer(make_kafka_client().get());
    consumer.start().get();
    std::vector<kv_t> consumed_records;
    auto next_offset = archive_ko;
    while (consumed_records.size() < total_records) {
        auto tmp = consumer
                     .consume_from_partition(
                       topic_name,
                       model::partition_id(0),
                       kafka::offset_cast(next_offset))
                     .get();
        vlog(e2e_test_log.debug, "{} records consumed", tmp.size());
        std::copy(tmp.begin(), tmp.end(), std::back_inserter(consumed_records));
        next_offset += model::offset((int64_t)tmp.size());
    }

    BOOST_REQUIRE_EQUAL(total_records, consumed_records.size());
    int i = 0;
    for (const auto& rec : consumed_records) {
        auto expected_key = ssx::sformat("key{}", i);
        auto expected_val = ssx::sformat("val{}", i);
        BOOST_REQUIRE_EQUAL(rec.key, expected_key);
        BOOST_REQUIRE_EQUAL(rec.val, expected_val);
        i++;
    }

    // Truncate and consume again
    const int64_t new_so = 100;
    const auto timeout = 10s;
    auto deadline = ss::lowres_clock::now() + timeout;
    ss::abort_source as;
    vlog(e2e_test_log.debug, "Truncating log up to kafka offset {}", new_so);
    auto truncation_result = partition->archival_meta_stm()
                               ->truncate(kafka::offset(new_so), deadline, as)
                               .get();
    if (!truncation_result) {
        vlog(
          e2e_test_log.error,
          "Failed to replicate truncation command, {}",
          truncation_result.message());
    }

    consumed_records.clear();
    auto last_offset = next_offset - model::offset(1);
    next_offset = kafka::offset(new_so);
    while (next_offset < last_offset) {
        auto tmp = consumer
                     .consume_from_partition(
                       topic_name,
                       model::partition_id(0),
                       kafka::offset_cast(next_offset))
                     .get();
        std::copy(tmp.begin(), tmp.end(), std::back_inserter(consumed_records));
        next_offset += kafka::offset((int64_t)tmp.size());
        vlog(
          e2e_test_log.debug,
          "{} records consumed, next offset: {}, target: {}",
          tmp.size(),
          next_offset,
          last_offset);
    }

    BOOST_REQUIRE_EQUAL(total_records - new_so, consumed_records.size());
    i = new_so;
    for (const auto& rec : consumed_records) {
        auto expected_key = ssx::sformat("key{}", i);
        auto expected_val = ssx::sformat("val{}", i);
        BOOST_REQUIRE_EQUAL(rec.key, expected_key);
        BOOST_REQUIRE_EQUAL(rec.val, expected_val);
        i++;
    }
#endif
}

class cloud_storage_manual_e2e_test
  : public s3_imposter_fixture
  , public redpanda_thread_fixture
  , public enable_cloud_storage_fixture {
public:
    static constexpr auto segs_per_spill = 10;
    cloud_storage_manual_e2e_test()
      : redpanda_thread_fixture(
        redpanda_thread_fixture::init_cloud_storage_tag{},
        httpd_port_number()) {
        // No expectations: tests will PUT and GET organically.
        set_expectations_and_listen({});
        wait_for_controller_leadership().get();

        // Apply local retention frequently.
        config::shard_local_cfg().log_compaction_interval_ms.set_value(
          std::chrono::duration_cast<std::chrono::milliseconds>(1s));
        // We'll control uploads ourselves.
        config::shard_local_cfg()
          .cloud_storage_enable_segment_merging.set_value(false);
        config::shard_local_cfg()
          .cloud_storage_disable_upload_loop_for_tests.set_value(true);
        // Disable metrics to speed things up.
        config::shard_local_cfg().enable_metrics_reporter.set_value(false);
        // Encourage spilling over.
        config::shard_local_cfg()
          .cloud_storage_spillover_manifest_max_segments.set_value(
            std::make_optional<size_t>(segs_per_spill));
        config::shard_local_cfg()
          .cloud_storage_spillover_manifest_size.set_value(
            std::optional<size_t>{});

        topic_name = model::topic("tapioca");
        ntp = model::ntp(model::kafka_namespace, topic_name, 0);

        // Create a tiered storage topic with very little local retention.
        cluster::topic_properties props;
        props.shadow_indexing = model::shadow_indexing_mode::full;
        props.retention_local_target_bytes = tristate<size_t>(1);
        props.cleanup_policy_bitflags
          = model::cleanup_policy_bitflags::deletion;
        add_topic({model::kafka_namespace, topic_name}, 1, props).get();
        wait_for_leader(ntp).get();
        partition = app.partition_manager.local().get(ntp).get();
        log = dynamic_cast<storage::disk_log_impl*>(
          partition->log().get_impl());
        archiver = &partition->archiver()->get();
    }

    model::topic topic_name;
    model::ntp ntp;
    cluster::partition* partition;
    storage::disk_log_impl* log;
    archival::ntp_archiver* archiver;
};

namespace {

ss::future<bool> check_consume_from_beginning(
  kafka::client::transport client,
  const model::topic& topic_name,
  ss::gate& gate) {
    kafka_consume_transport consumer(std::move(client));
    co_await consumer.start();
    int iters = 0;
    while (iters == 0 || !gate.is_closed()) {
        auto holder = gate.hold();
        auto kvs = co_await consumer.consume_from_partition(
          topic_name, model::partition_id(0), model::offset(0));
        if (kvs.empty()) {
            vlog(e2e_test_log.error, "no fetch results");
            co_return false;
        }
        if (kvs[0].key != "key0") {
            vlog(e2e_test_log.error, "{} != key0", kvs[0].key);
            co_return false;
        }
        if (kvs[0].val != "val0") {
            vlog(e2e_test_log.error, "{} != val0", kvs[0].val);
            co_return false;
        }
        iters++;
    }
    co_return true;
}

} // namespace

FIXTURE_TEST(test_consume_during_spillover, cloud_storage_manual_e2e_test) {
    config::shard_local_cfg().fetch_max_bytes.set_value(size_t{10});
    const auto records_per_seg = 5;
    const auto num_segs = 40;
    tests::remote_segment_generator gen(make_kafka_client().get(), *partition);
    auto total_records = gen.num_segments(num_segs)
                           .batches_per_segment(records_per_seg)
                           .produce()
                           .get();
    BOOST_REQUIRE_GE(total_records, 200);

    ss::gate g;

    std::vector<kafka::client::transport> clients;
    std::vector<ss::future<bool>> checks;
    clients.reserve(10);
    checks.reserve(10);
    for (int i = 0; i < 10; i++) {
        clients.emplace_back(make_kafka_client().get());
    }
    for (auto& client : clients) {
        checks.push_back(
          check_consume_from_beginning(std::move(client), topic_name, g));
    }
    auto cleanup = ss::defer([&] {
        if (!g.is_closed()) {
            g.close().get();
        }
        for (auto& check : checks) {
            check.get();
        }
    });

    auto start_before_spill = archiver->manifest().get_start_offset();
    BOOST_REQUIRE(archiver->sync_for_tests().get());
    archiver->apply_spillover().get();
    BOOST_REQUIRE_NE(
      start_before_spill, archiver->manifest().get_start_offset());

    g.close().get();
    for (auto& check : checks) {
        BOOST_CHECK(check.get());
    }
    cleanup.cancel();
}

class cloud_storage_manual_multinode_test
  : public s3_imposter_fixture
  , public redpanda_thread_fixture
  , public enable_cloud_storage_fixture {
public:
    cloud_storage_manual_multinode_test()
      : redpanda_thread_fixture(
        redpanda_thread_fixture::init_cloud_storage_tag{},
        httpd_port_number()) {
        // No expectations: tests will PUT and GET organically.
        set_expectations_and_listen({});

        config::shard_local_cfg()
          .cloud_storage_enable_segment_merging.set_value(false);
        config::shard_local_cfg()
          .cloud_storage_disable_upload_loop_for_tests.set_value(true);

        wait_for_controller_leadership().get();
    }

    std::unique_ptr<redpanda_thread_fixture> start_second_fixture() {
        return std::make_unique<redpanda_thread_fixture>(
          model::node_id(2),
          9092 + 10,
          33145 + 10,
          8082 + 10,
          8081 + 10,
          std::vector<config::seed_server>{
            {.addr = net::unresolved_address("127.0.0.1", 33145)}},
          ssx::sformat("test.second_dir{}", time(0)),
          app.sched_groups,
          true,
          get_s3_config(httpd_port_number()),
          get_archival_config(),
          get_cloud_config(httpd_port_number()));
    }
};

FIXTURE_TEST(
  test_realigned_segment_reupload, cloud_storage_manual_multinode_test) {
    const model::topic topic_name("tapioca");
    model::ntp ntp(model::kafka_namespace, topic_name, 0);

    auto fx2 = start_second_fixture();
    tests::cooperative_spin_wait_with_timeout(3s, [this] {
        return app.controller->get_members_table().local().node_ids().size()
               == 2;
    }).get();

    cluster::topic_properties props;
    props.shadow_indexing = model::shadow_indexing_mode::full;
    props.cleanup_policy_bitflags = model::cleanup_policy_bitflags::compaction
                                    | model::cleanup_policy_bitflags::deletion;
    add_topic({model::kafka_namespace, topic_name}, 1, props, 2).get();

    redpanda_thread_fixture* fx_l = nullptr;
    redpanda_thread_fixture* fx_f = nullptr;
    const auto wait_for_leader = [&] {
        tests::cooperative_spin_wait_with_timeout(10s, [&] {
            cluster::partition* prt_a
              = app.partition_manager.local().get(ntp).get();
            cluster::partition* prt_b
              = fx2->app.partition_manager.local().get(ntp).get();
            if (!prt_a || !prt_b) {
                return false;
            }
            if (prt_a->is_leader()) {
                fx_l = this;
                fx_f = fx2.get();
                return true;
            }
            if (prt_b->is_leader()) {
                fx_l = fx2.get();
                fx_f = this;
                return true;
            }
            return false;
        }).get();
    };

    wait_for_leader();
    auto* prt_l = fx_l->app.partition_manager.local().get(ntp).get();
    auto* log_l = dynamic_cast<storage::disk_log_impl*>(
      prt_l->log().get_impl());
    auto* archiver_l = &prt_l->archiver()->get();

    auto* prt_f = fx_f->app.partition_manager.local().get(ntp).get();
    auto* log_f = dynamic_cast<storage::disk_log_impl*>(
      prt_f->log().get_impl());
    auto* archiver_f = &prt_f->archiver()->get();

    // Write data that would be compacted.
    kafka_produce_transport producer(fx_l->make_kafka_client().get());
    producer.start().get();
    int key = -1;
    for (int i = 0; i < 30; i++) {
        if (i % 5 == 0) {
            key++;
        }
        if (i == 5 || i == 25) {
            // Generate segments with keys like:
            // [00000][111112222233333...][nnnnn]
            // NOTE: we intentionally want to be misalined between nodes.
            log_f->flush().get();
            log_f->force_roll(ss::default_priority_class()).get();
        }
        if (i == 10) {
            // [0000011111][2222233333.....nnnnn]
            log_l->flush().get();
            log_l->force_roll(ss::default_priority_class()).get();
        }
        producer
          .produce_to_partition(
            topic_name, model::partition_id(0), tests::kv_t::sequence(key, 1))
          .get();
    }

    // Force roll the leader. We should have the first ten keys, the rest of
    // the keys, and a new active segment.
    log_l->flush().get();
    log_l->force_roll(ss::default_priority_class()).get();
    tests::cooperative_spin_wait_with_timeout(3s, [log_l] {
        return log_l->segment_count() == 3;
    }).get();

    // Upload and ensure the ones with data land in the manifest.
    BOOST_REQUIRE(archiver_l->sync_for_tests().get());
    archiver_l->upload_next_candidates().get();
    BOOST_REQUIRE_EQUAL(
      cloud_storage::upload_result::success,
      archiver_l->upload_manifest("test").get());
    archiver_l->flush_manifest_clean_offset().get();

    BOOST_REQUIRE_EQUAL(2, archiver_l->manifest().size());
    tests::cooperative_spin_wait_with_timeout(3s, [archiver_f] {
        return archiver_f->manifest().size() == 2;
    }).get();

    // Compact on the leader so the first segment gets reduced and reuploaded.
    // Since the other node has a different segment layout, this will help us
    // exercise the adjustmens done during segment reupload.
    ss::abort_source as;
    auto* second_seg = std::next(log_l->segments().begin())->get();
    storage::housekeeping_config compact_one_cfg(
      model::timestamp::min(), // no time-based retention
      std::nullopt,            // no size-based retention
      // Don't compact the second segment yet.
      second_seg->offsets().base_offset,
      ss::default_priority_class(),
      as);
    log_l->housekeeping(compact_one_cfg).get();
    BOOST_REQUIRE_EQUAL(3, log_l->segment_count());
    BOOST_REQUIRE(log_l->segments().front()->finished_self_compaction());
    BOOST_REQUIRE(!second_seg->finished_self_compaction());

    // Upload and expect the compacted segment gets uploaded.
    BOOST_REQUIRE(archiver_l->sync_for_tests().get());
    auto res = archiver_l->upload_next_candidates().get();
    BOOST_REQUIRE_EQUAL(1, res.compacted_upload_result.num_succeeded);
    BOOST_REQUIRE_EQUAL(
      cloud_storage::upload_result::success,
      archiver_l->upload_manifest("test").get());
    archiver_l->flush_manifest_clean_offset().get();

    // Sanity check that the manifest says we compacted the segment.
    BOOST_REQUIRE_EQUAL(2, archiver_l->manifest().size());
    BOOST_REQUIRE(archiver_l->manifest()
                    .segment_containing(model::offset(0))
                    ->is_compacted);
    BOOST_REQUIRE(!archiver_l->manifest()
                     .segment_containing(second_seg->offsets().base_offset)
                     ->is_compacted);

    // Sanity check that the other node's log hasn't rolled at all.
    BOOST_REQUIRE_EQUAL(3, log_f->segment_count());
    BOOST_REQUIRE(!log_f->segments().front()->finished_self_compaction());

    // Switch leaders so we can operate on the other node.
    cluster::transfer_leadership_request transfer_req{
      .group = prt_l->raft()->group(),
      .target = prt_f->raft()->self().id(),
    };
    auto transfer_resp = prt_l->raft()->transfer_leadership(transfer_req).get();
    BOOST_REQUIRE(transfer_resp.success);
    tests::cooperative_spin_wait_with_timeout(3s, [&prt_f] {
        return prt_f->raft()->is_leader();
    }).get();
    // One new segment for the new term.
    BOOST_REQUIRE_EQUAL(4, log_f->segment_count());

    // Remove the first segment so there is misalignment between local and
    // manifest segment offsets.
    size_t size_after_first_seg = 0;
    auto second_seg_it = std::next(log_f->segments().begin());
    for (auto it = second_seg_it; it != log_f->segments().end(); it++) {
        size_after_first_seg += it->get()->file_size();
    }
    storage::gc_config remove_first_cfg(
      model::timestamp::min(), // no time-based retention
      size_after_first_seg);   // remove just the first segment
    log_f->gc(remove_first_cfg).get();
    tests::cooperative_spin_wait_with_timeout(3s, [log_f] {
        return log_f->segment_count() == 3;
    }).get();
    BOOST_REQUIRE(!log_f->segments().front()->finished_self_compaction());

    // Compact the next segments.
    storage::housekeeping_config compact_all_cfg(
      model::timestamp::min(), // no time-based retention
      std::nullopt,            // no size-based retention
      model::offset::max(),    // Everything is viable for compaction.
      ss::default_priority_class(),
      as);
    log_f->housekeeping(compact_all_cfg).get();
    log_f->housekeeping(compact_all_cfg).get();
    BOOST_REQUIRE_EQUAL(3, log_f->segment_count());
    BOOST_REQUIRE(log_f->segments().front()->finished_self_compaction());
    BOOST_REQUIRE(
      std::next(log_f->segments().begin())->get()->finished_self_compaction());

    // Do a segment reupload.
    BOOST_REQUIRE(archiver_f->sync_for_tests().get());
    res = archiver_f->upload_next_candidates().get();
    BOOST_REQUIRE_EQUAL(1, res.compacted_upload_result.num_succeeded);
    BOOST_REQUIRE_EQUAL(
      cloud_storage::upload_result::success,
      archiver_f->upload_manifest("test").get());
    archiver_f->flush_manifest_clean_offset().get();

    // Now remove local segments so we're forced to look in cloud.
    storage::gc_config remove_all_cfg(
      model::timestamp::max(), // remove everything
      1);                      // remove everything
    log_f->gc(remove_all_cfg).get();
    tests::cooperative_spin_wait_with_timeout(3s, [log_f] {
        return log_f->segment_count() <= 2;
    }).get();

    // Any stuck_reader_exceptions?
    // (no).
    kafka_consume_transport consumer(fx_f->make_kafka_client().get());
    consumer.start().get();
    for (int i = 0; i < 30; i++) {
        auto r = consumer
                   .consume_from_partition(
                     topic_name, model::partition_id(0), model::offset(i))
                   .get();
        BOOST_REQUIRE(!r.empty());
    }
}

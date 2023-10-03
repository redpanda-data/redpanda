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
#include "cloud_storage/tests/manual_fixture.h"
#include "cloud_storage/tests/produce_utils.h"
#include "cloud_storage/tests/s3_imposter.h"
#include "cluster/health_monitor_frontend.h"
#include "config/configuration.h"
#include "kafka/server/tests/produce_consume_utils.h"
#include "model/fundamental.h"
#include "redpanda/tests/fixture.h"

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
    auto log = partition->log();
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
    partition->log()->housekeeping(housekeeping_conf).get();
    // NOTE: the storage layer only initially requests eviction; it relies on
    // Raft to write a snapshot and subsequently truncate.
    tests::cooperative_spin_wait_with_timeout(10s, [log] {
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
    auto log = partition->log();
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
        log = partition->log();
        archiver = &partition->archiver()->get();
    }

    model::topic topic_name;
    model::ntp ntp;
    cluster::partition* partition;
    ss::shared_ptr<storage::log> log;
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

FIXTURE_TEST(
  reclaimable_reported_in_health_report,
  cloud_storage_manual_multinode_test_base) {
    config::shard_local_cfg().retention_local_trim_interval.set_value(
      std::chrono::milliseconds(2000));

    // start a second fixutre and wait for stable setup
    auto fx2 = start_second_fixture();
    tests::cooperative_spin_wait_with_timeout(3s, [this] {
        return app.controller->get_members_table().local().node_ids().size()
               == 2;
    }).get();

    // test topic
    const model::topic topic_name("tapioca");
    model::ntp ntp(model::kafka_namespace, topic_name, 0);
    cluster::topic_properties props;
    props.shadow_indexing = model::shadow_indexing_mode::full;
    props.cleanup_policy_bitflags = model::cleanup_policy_bitflags::deletion;
    props.segment_size = 64_KiB;
    props.retention_local_target_bytes = tristate<size_t>(1);
    add_topic({model::kafka_namespace, topic_name}, 1, props, 2).get();

    // figuring out the leader is useful for constructing the producer. the
    // follower is just the "other" node.
    redpanda_thread_fixture* fx_l = nullptr;
    boost_require_eventually(10s, [&] {
        cluster::partition* prt_a
          = app.partition_manager.local().get(ntp).get();
        cluster::partition* prt_b
          = fx2->app.partition_manager.local().get(ntp).get();
        if (!prt_a || !prt_b) {
            return false;
        }
        if (prt_a->is_leader()) {
            fx_l = this;
            return true;
        }
        if (prt_b->is_leader()) {
            fx_l = fx2.get();
            return true;
        }
        return false;
    });

    auto prt_l = fx_l->app.partition_manager.local().get(ntp);

    kafka_produce_transport producer(fx_l->make_kafka_client().get());
    producer.start().get();

    auto get_reclaimable = [&]() -> std::optional<std::vector<size_t>> {
        auto report = app.controller->get_health_monitor()
                        .local()
                        .get_cluster_health(
                          cluster::cluster_report_filter{},
                          cluster::force_refresh::yes,
                          model::timeout_clock::now() + std::chrono::seconds(2))
                        .get();
        if (report.has_value()) {
            std::vector<size_t> sizes;
            for (auto& node_report : report.value().node_reports) {
                for (auto& topic : node_report.topics) {
                    if (
                      topic.tp_ns
                      != model::topic_namespace_view(
                        model::kafka_namespace, topic_name)) {
                        continue;
                    }
                    for (auto partition : topic.partitions) {
                        sizes.push_back(
                          partition.reclaimable_size_bytes.value_or(0));
                    }
                }
            }
            if (!sizes.empty()) {
                return sizes;
            }
        }
        return std::nullopt;
    };

    for (int j = 0; j < 20; j++) {
        for (int i = 0; i < 200; i++) {
            producer
              .produce_to_partition(
                topic_name,
                model::partition_id(0),
                tests::kv_t::sequence(0, 200))
              .get();
        }

        // drive the uploading
        auto& archiver = prt_l->archiver()->get();
        archiver.sync_for_tests().get();
        archiver.upload_next_candidates().get();

        // not for synchronization... just to give the system time to propogate
        // all the state changes are are happening so that this overall loop
        // doesn't spin to completion too fast.
        ss::sleep(std::chrono::seconds(2)).get();

        auto sizes = get_reclaimable();
        if (sizes.has_value()) {
            BOOST_REQUIRE(!sizes->empty());
            if (std::all_of(sizes->begin(), sizes->end(), [](size_t s) {
                    return s > 0;
                })) {
                return; // test success
            }
        }
    }

    // health report never reported non-zero reclaimable sizes. bummer!
    BOOST_REQUIRE(false);
}

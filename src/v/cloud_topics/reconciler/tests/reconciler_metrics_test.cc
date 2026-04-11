/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_topics/level_one/common/fake_io.h"
#include "cloud_topics/level_one/metastore/simple_metastore.h"
#include "cloud_topics/reconciler/reconciler.h"
#include "cloud_topics/reconciler/reconciliation_source.h"
#include "cloud_topics/reconciler/tests/test_utils.h"
#include "model/fundamental.h"
#include "model/record.h"
#include "model/record_batch_reader.h"
#include "model/tests/random_batch.h"
#include "model/tests/randoms.h"
#include "test_utils/metrics.h"

#include <seastar/core/manual_clock.hh>
#include <seastar/core/scheduling.hh>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <optional>

using namespace cloud_topics;
using cloud_topics::reconciler::test::fake_source;
using cloud_topics::reconciler::test::unreliable_io;
using cloud_topics::reconciler::test::unreliable_metastore;

namespace {

class ReconcilerMetricsTest : public testing::Test {
public:
    ReconcilerMetricsTest() { _reconciler.setup_metrics_for_tests(); }

    ss::shared_ptr<fake_source> add_source(
      std::optional<model::topic> tp = std::nullopt,
      std::optional<model::topic_id> tid = std::nullopt) {
        if (!tid.has_value()) {
            tid = model::create_topic_id();
        }
        auto ntp = model::random_ntp();
        if (tp.has_value()) {
            ntp.tp.topic = tp.value();
        }

        auto tidp = model::topic_id_partition(tid.value(), ntp.tp.partition);
        auto src = ss::make_shared<fake_source>(ntp, tidp);
        _reconciler.attach_source(src);
        return src;
    }

    void reconcile() {
        // Advance the clock to ensure all topics are due for reconciliation.
        ss::manual_clock::advance(std::chrono::hours(1));
        _reconciler.reconcile().get();
    }

    unreliable_io& io() { return _io; }
    unreliable_metastore& metastore() { return _metastore; }
    reconciler::reconciler<ss::manual_clock>& reconciler() {
        return _reconciler;
    }

private:
    unreliable_io _io;
    unreliable_metastore _metastore;
    reconciler::reconciler<ss::manual_clock> _reconciler{
      &_io, &_metastore, nullptr, ss::default_scheduling_group()};
};

using ::testing::Gt;
using ::testing::Optional;

std::optional<uint64_t> get_objects_uploaded() {
    return test_utils::find_metric_value<uint64_t>(
      "cloud_topics_reconciler_objects_uploaded");
}

std::optional<uint64_t> get_bytes_reconciled() {
    return test_utils::find_metric_value<uint64_t>(
      "cloud_topics_reconciler_bytes_reconciled");
}

std::optional<uint64_t> get_batches_reconciled() {
    return test_utils::find_metric_value<uint64_t>(
      "cloud_topics_reconciler_batches_reconciled");
}

std::optional<uint64_t> get_partitions_reconciled() {
    return test_utils::find_metric_value<uint64_t>(
      "cloud_topics_reconciler_partitions_reconciled");
}

std::optional<uint64_t> get_metastore_retries() {
    return test_utils::find_metric_value<uint64_t>(
      "cloud_topics_reconciler_metastore_retries");
}

std::optional<uint64_t> get_offset_corrections() {
    return test_utils::find_metric_value<uint64_t>(
      "cloud_topics_reconciler_offset_corrections");
}

} // namespace

TEST_F(ReconcilerMetricsTest, ThroughputCounters) {
    EXPECT_THAT(get_objects_uploaded(), Optional(0));
    EXPECT_THAT(get_bytes_reconciled(), Optional(0));
    EXPECT_THAT(get_batches_reconciled(), Optional(0));
    EXPECT_THAT(get_partitions_reconciled(), Optional(0));

    const model::topic tp{"tapioca"};
    const model::topic_id tid = model::topic_id::create();

    auto src1 = add_source(tp, tid);
    auto src2 = add_source(tp, tid);

    src1->add_batch({.count = 10});
    src1->add_batch({.count = 10});
    src1->add_batch({.count = 10});
    src2->add_batch({.count = 10});
    src2->add_batch({.count = 10});

    reconcile();

    EXPECT_THAT(get_objects_uploaded(), Optional(1));
    EXPECT_THAT(get_partitions_reconciled(), Optional(2));
    EXPECT_THAT(get_batches_reconciled(), Optional(5));
    EXPECT_THAT(get_bytes_reconciled(), Optional(Gt(0)));

    auto bytes_after_first = *get_bytes_reconciled();

    reconcile();

    EXPECT_THAT(get_objects_uploaded(), Optional(1));
    EXPECT_THAT(get_partitions_reconciled(), Optional(2));
    EXPECT_THAT(get_batches_reconciled(), Optional(5));
    EXPECT_THAT(get_bytes_reconciled(), Optional(bytes_after_first));

    src1->add_batch({.count = 15});

    reconcile();

    EXPECT_THAT(get_objects_uploaded(), Optional(2));
    EXPECT_THAT(get_partitions_reconciled(), Optional(3));
    EXPECT_THAT(get_batches_reconciled(), Optional(6));
    EXPECT_THAT(get_bytes_reconciled(), Optional(Gt(bytes_after_first)));
}

TEST_F(ReconcilerMetricsTest, FailedObjectsCounter) {
    EXPECT_THAT(get_objects_uploaded(), Optional(0));

    auto src = add_source();
    src->add_batch({.count = 10});

    io().fail_complete(true);

    reconcile();

    // Object was not uploaded because multipart complete failed.
    EXPECT_THAT(get_objects_uploaded(), Optional(0));

    io().fail_complete(false);

    reconcile();

    EXPECT_THAT(get_objects_uploaded(), Optional(1));
}

TEST_F(ReconcilerMetricsTest, HistogramMetrics) {
    auto src = add_source();
    src->add_batch({.count = 10});

    reconcile();

    const auto& probe = reconciler().get_probe_for_tests();

    auto metastore_add_objects_duration
      = probe.get_metastore_add_objects_duration_for_tests();
    EXPECT_GT(metastore_add_objects_duration.sample_count, 0);
}

// This test depends on the simple metastore packing all sources of the same
// topic into one object.
TEST_F(ReconcilerMetricsTest, ObjectMetrics) {
    const model::topic tp{"tapioca"};
    const model::topic_id tid = model::topic_id::create();

    auto src1 = add_source(tp, tid);
    auto src2 = add_source(tp, tid);

    src1->add_batch({.count = 10});
    src2->add_batch({.count = 5});

    reconcile();

    const auto& probe = reconciler().get_probe_for_tests();

    auto object_size = probe.get_object_size_bytes_for_tests();
    EXPECT_EQ(object_size.sample_count, 1);

    src1->add_batch({.count = 15});

    reconcile();

    object_size = probe.get_object_size_bytes_for_tests();
    EXPECT_EQ(object_size.sample_count, 2);
}

TEST_F(ReconcilerMetricsTest, MetastoreRetries) {
    EXPECT_THAT(get_metastore_retries(), Optional(0));
    EXPECT_THAT(get_offset_corrections(), Optional(0));

    auto src = add_source();
    src->add_batch({.count = 10});

    metastore().fail_add_objects_transiently(2);

    reconcile();

    EXPECT_THAT(get_metastore_retries(), Optional(2));
    EXPECT_THAT(get_objects_uploaded(), Optional(1));

    src->add_batch({.count = 5});
    reconcile();

    EXPECT_THAT(get_metastore_retries(), Optional(2));
    EXPECT_THAT(get_objects_uploaded(), Optional(2));
}

// For more explanation of offset correction, see the LROUpdateFailure
// reconciler unit test.
TEST_F(ReconcilerMetricsTest, OffsetCorrection) {
    auto src = add_source();

    src->add_batch({.count = 10});
    src->fail_set_lro(true);

    reconcile();

    EXPECT_EQ(src->last_reconciled_offset(), kafka::offset{});
    EXPECT_THAT(get_offset_corrections(), Optional(0));

    src->add_batch({.count = 10});
    src->fail_set_lro(false);

    reconcile();

    EXPECT_EQ(src->last_reconciled_offset(), kafka::offset{9});
    EXPECT_THAT(get_offset_corrections(), Optional(1));

    reconcile();

    EXPECT_EQ(src->last_reconciled_offset(), kafka::offset{19});
    EXPECT_THAT(get_offset_corrections(), Optional(1));
}

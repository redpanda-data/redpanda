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
#include "gmock/gmock.h"
#include "model/fundamental.h"
#include "model/record.h"
#include "model/record_batch_reader.h"
#include "model/tests/random_batch.h"
#include "model/tests/randoms.h"
#include "test_utils/scoped_config.h"

#include <seastar/core/manual_clock.hh>
#include <seastar/core/scheduling.hh>
#include <seastar/util/defer.hh>

#include <gtest/gtest.h>

#include <expected>
#include <memory>
#include <utility>

using namespace cloud_topics;
using cloud_topics::reconciler::test::fake_source;
using cloud_topics::reconciler::test::unreliable_io;
using cloud_topics::reconciler::test::unreliable_metastore;

namespace {

class ReconcilerTest : public testing::Test {
public:
    ReconcilerTest()
      : _reconciler(
          &_io, &_metastore, nullptr, ss::default_scheduling_group()) {}

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

    // Call reconcile without advancing the clock.
    void reconcile_without_advancing_clock() { _reconciler.reconcile().get(); }

    std::optional<kafka::offset>
    metastore_next_offset(ss::shared_ptr<fake_source> src) {
        auto offsets = _metastore.get_offsets(src->topic_id_partition()).get();
        if (!offsets.has_value()) {
            return std::nullopt;
        }
        return offsets.value().next_offset;
    }

    unreliable_metastore& metastore() { return _metastore; }
    unreliable_io& io() { return _io; }

    // Advance the manual clock to make topics due for reconciliation.
    void advance_clock() {
        // Advance past the max reconciliation interval to ensure topics are
        // due.
        ss::manual_clock::advance(std::chrono::hours(1));
    }

protected:
    unreliable_io _io;
    unreliable_metastore _metastore;
    reconciler::reconciler<ss::manual_clock> _reconciler;
};

using ::testing::Optional;

} // namespace

TEST_F(ReconcilerTest, EmptySource) {
    auto src = add_source();
    reconcile();
    EXPECT_EQ(src->last_reconciled_offset(), kafka::offset{});
    EXPECT_EQ(metastore_next_offset(src), std::nullopt);
}

TEST_F(ReconcilerTest, EmptyRoundDoesNotUploadObject) {
    auto src = add_source();
    src->add_batch({.count = 10});
    reconcile();

    // First round should upload one object.
    auto objects_after_first = io().list_objects();
    EXPECT_EQ(objects_after_first.size(), 1);

    // Second round with no new data should not upload anything.
    reconcile();
    auto objects_after_second = io().list_objects();
    EXPECT_EQ(objects_after_second.size(), objects_after_first.size());
}

TEST_F(ReconcilerTest, SingleSource) {
    auto src = add_source();
    src->add_batch({.count = 10});
    src->add_batch({.count = 10});
    src->add_batch({.count = 10});
    reconcile();
    EXPECT_EQ(src->last_reconciled_offset(), kafka::offset{29});
    EXPECT_THAT(metastore_next_offset(src), Optional(kafka::offset{30}));
    src->add_batch({.count = 10});
    reconcile();
    EXPECT_EQ(src->last_reconciled_offset(), kafka::offset{39});
    EXPECT_THAT(metastore_next_offset(src), Optional(kafka::offset{40}));
}

TEST_F(ReconcilerTest, SingleSourceWithControlBatches) {
    auto src = add_source();
    src->add_batch({.count = 1, .is_control = true});
    src->add_batch({.count = 8});
    src->add_batch({.count = 1, .is_control = true});
    src->add_batch({.count = 10});
    src->add_batch({.count = 10});
    reconcile();
    EXPECT_EQ(src->last_reconciled_offset(), kafka::offset{29});
    EXPECT_THAT(metastore_next_offset(src), Optional(kafka::offset{30}));
}

TEST_F(ReconcilerTest, MultipleSources) {
    const model::topic tp{"tapioca"};
    const model::topic_id tid = model::topic_id::create();

    auto src1 = add_source(tp, tid);
    auto src2 = add_source(tp, tid);
    auto src3 = add_source(tp, tid);

    src1->add_batch({.count = 10});
    src1->add_batch({.count = 5});

    src2->add_batch({.count = 20});
    src2->add_batch({.count = 15});
    src2->add_batch({.count = 10});

    src3->add_batch({.count = 8});

    reconcile();

    EXPECT_EQ(src1->last_reconciled_offset(), kafka::offset{14});
    EXPECT_EQ(src2->last_reconciled_offset(), kafka::offset{44});
    EXPECT_EQ(src3->last_reconciled_offset(), kafka::offset{7});

    EXPECT_THAT(metastore_next_offset(src1), Optional(kafka::offset{15}));
    EXPECT_THAT(metastore_next_offset(src2), Optional(kafka::offset{45}));
    EXPECT_THAT(metastore_next_offset(src3), Optional(kafka::offset{8}));

    // Add more data.
    src1->add_batch({.count = 10});

    src2->add_batch({.count = 10});

    src3->add_batch({.count = 10});

    reconcile();

    EXPECT_EQ(src1->last_reconciled_offset(), kafka::offset{24});
    EXPECT_EQ(src2->last_reconciled_offset(), kafka::offset{54});
    EXPECT_EQ(src3->last_reconciled_offset(), kafka::offset{17});

    EXPECT_THAT(metastore_next_offset(src1), Optional(kafka::offset{25}));
    EXPECT_THAT(metastore_next_offset(src2), Optional(kafka::offset{55}));
    EXPECT_THAT(metastore_next_offset(src3), Optional(kafka::offset{18}));

    // Add data to only one of the sources.
    src2->add_batch({.count = 10});

    reconcile();

    EXPECT_EQ(src1->last_reconciled_offset(), kafka::offset{24});
    EXPECT_EQ(src2->last_reconciled_offset(), kafka::offset{64});
    EXPECT_EQ(src3->last_reconciled_offset(), kafka::offset{17});

    EXPECT_THAT(metastore_next_offset(src1), Optional(kafka::offset{25}));
    EXPECT_THAT(metastore_next_offset(src2), Optional(kafka::offset{65}));
    EXPECT_THAT(metastore_next_offset(src3), Optional(kafka::offset{18}));
}

TEST_F(ReconcilerTest, ObjectSizeLimit) {
    // Use a small max object size for fast tests.
    scoped_config cfg;
    cfg.get("cloud_topics_reconciliation_max_object_size")
      .set_value(size_t{1_MiB});

    auto src = add_source();

    // Total size = 20 * 1 * 64KiB = 1.25MiB, which exceeds the 1MiB limit.
    // Disable compression to ensure predictable sizes.
    constexpr auto batch_count = 20;
    constexpr auto record_count = 1;
    constexpr auto record_size = 64_KiB;
    for (size_t i = 0; i < batch_count; ++i) {
        src->add_batch(
          {.allow_compression = false,
           .count = record_count,
           .record_sizes = std::vector<size_t>(record_count, record_size)});
    }

    reconcile();

    // Check that some, but not all, data was reconciled.
    constexpr auto last_offset = batch_count * record_count - 1;
    auto lro = src->last_reconciled_offset();
    EXPECT_GT(lro, kafka::offset{0});
    EXPECT_LT(lro, kafka::offset{last_offset});

    // Reconciling again should process the rest of the data.
    reconcile();
    EXPECT_EQ(src->last_reconciled_offset(), kafka::offset{last_offset});
    EXPECT_THAT(
      metastore_next_offset(src), Optional(kafka::offset{last_offset + 1}));
}

TEST_F(ReconcilerTest, ObjectSizeLimitMultipleSources) {
    // Use a small max object size for fast tests.
    scoped_config cfg;
    cfg.get("cloud_topics_reconciliation_max_object_size")
      .set_value(size_t{1_MiB});

    const model::topic tp{"tapioca"};
    const model::topic_id tid = model::topic_id::create();

    auto src1 = add_source(tp, tid);
    auto src2 = add_source(tp, tid);

    // 960KiB in source 1 (15 * 64KiB).
    // Disable compression to ensure predictable sizes.
    constexpr auto batch_count_1 = 15;
    constexpr auto record_count = 1;
    constexpr auto record_size = 64_KiB;
    for (size_t i = 0; i < batch_count_1; ++i) {
        src1->add_batch(
          {.allow_compression = false,
           .count = record_count,
           .record_sizes = std::vector<size_t>(record_count, record_size)});
    }

    // 640KiB in source 2 (10 * 64KiB), total 1.6MiB > 1MiB limit.
    constexpr auto batch_count_2 = 10;
    for (size_t i = 0; i < batch_count_2; ++i) {
        src2->add_batch(
          {.allow_compression = false,
           .count = record_count,
           .record_sizes = std::vector<size_t>(record_count, record_size)});
    }

    // First reconciliation round should process some data but not all.
    reconcile();

    constexpr auto last_offset_1 = batch_count_1 * record_count - 1;
    constexpr auto last_offset_2 = batch_count_2 * record_count - 1;

    auto lro1_round1 = src1->last_reconciled_offset();
    auto lro2_round1 = src2->last_reconciled_offset();

    // At least one source should not be fully processed.
    EXPECT_TRUE(
      lro1_round1 < kafka::offset{last_offset_1}
      || lro2_round1 < kafka::offset{last_offset_2});

    // Second reconciliation round should process all remaining data.
    reconcile();
    EXPECT_EQ(src1->last_reconciled_offset(), kafka::offset{last_offset_1});
    EXPECT_EQ(src2->last_reconciled_offset(), kafka::offset{last_offset_2});
    EXPECT_THAT(
      metastore_next_offset(src1), Optional(kafka::offset{last_offset_1 + 1}));
    EXPECT_THAT(
      metastore_next_offset(src2), Optional(kafka::offset{last_offset_2 + 1}));
}

// Test that when two sources each exceed max_object_size, only one
// is processed per reconciliation round. This verifies that the size_budget
// calculation doesn't underflow (which would allow both to be processed).
TEST_F(ReconcilerTest, ObjectSizeLimitOneSourcePerRound) {
    // Use a small max object size for fast tests.
    scoped_config cfg;
    cfg.get("cloud_topics_reconciliation_max_object_size")
      .set_value(size_t{1_MiB});

    const model::topic tp{"tapioca"};
    const model::topic_id tid = model::topic_id::create();

    auto src1 = add_source(tp, tid);
    auto src2 = add_source(tp, tid);

    // Each source: 25 batches of 64KiB = 1.6MiB > 1MiB limit.
    constexpr auto batch_count = 25;
    constexpr auto record_count = 1;
    constexpr auto record_size = 64_KiB;
    for (size_t i = 0; i < batch_count; ++i) {
        src1->add_batch(
          {.allow_compression = false,
           .count = record_count,
           .record_sizes = std::vector<size_t>(record_count, record_size)});
        src2->add_batch(
          {.allow_compression = false,
           .count = record_count,
           .record_sizes = std::vector<size_t>(record_count, record_size)});
    }

    reconcile();

    auto lro1 = src1->last_reconciled_offset();
    auto lro2 = src2->last_reconciled_offset();

    // Exactly one source should make progress, the other should not advance.
    // The size budget prevents processing multiple sources in one round.
    bool src1_started = lro1 >= kafka::offset{0};
    bool src2_started = lro2 >= kafka::offset{0};
    EXPECT_TRUE(src1_started != src2_started)
      << "Expected exactly one source to be processed per round. "
      << "src1 LRO: " << lro1 << ", src2 LRO: " << lro2;
}

TEST_F(ReconcilerTest, SourceReadFailure) {
    auto src = add_source();
    src->add_batch({.count = 10});
    src->fail_make_reader(true);

    reconcile();

    // The failure of one source should not stop others from being reconciled.
    EXPECT_EQ(src->last_reconciled_offset(), kafka::offset{});
    EXPECT_EQ(metastore_next_offset(src), std::nullopt);

    src->fail_make_reader(false);

    reconcile();

    // Reconciliation should resume on a source after a failure.
    EXPECT_EQ(src->last_reconciled_offset(), kafka::offset{9});
    EXPECT_THAT(metastore_next_offset(src), Optional(kafka::offset{10}));
}

TEST_F(ReconcilerTest, MetastoreAddObjectsFailure) {
    auto src = add_source();
    src->add_batch({.count = 10});
    metastore().fail_add_objects(true);

    reconcile();

    // Verify LRO doesn't advance when metastore fails, for any source.
    EXPECT_EQ(src->last_reconciled_offset(), kafka::offset{});
    EXPECT_EQ(metastore_next_offset(src), std::nullopt);

    metastore().fail_add_objects(false);

    reconcile();

    // Reconciliation should resume after a metastore failure.
    EXPECT_EQ(src->last_reconciled_offset(), kafka::offset{9});
    EXPECT_THAT(metastore_next_offset(src), Optional(kafka::offset{10}));
}

TEST_F(ReconcilerTest, MetastoreAddObjectsTransientFailure) {
    auto src1 = add_source();
    auto src2 = add_source();

    src1->add_batch({.count = 10});
    src2->add_batch({.count = 10});

    metastore().fail_add_objects_transiently(2);

    reconcile();

    // A transient failure should be retried until success or timeout, and
    // retries will be too fast to time out in this test.
    EXPECT_EQ(src1->last_reconciled_offset(), kafka::offset{9});
    EXPECT_THAT(metastore_next_offset(src1), Optional(kafka::offset{10}));
    EXPECT_EQ(src2->last_reconciled_offset(), kafka::offset{9});
    EXPECT_THAT(metastore_next_offset(src2), Optional(kafka::offset{10}));
}

TEST_F(ReconcilerTest, IOMultipartInitFailure) {
    auto src = add_source();
    src->add_batch({.count = 10});

    io().fail_create_multipart(true);

    reconcile();

    EXPECT_EQ(src->last_reconciled_offset(), kafka::offset{});
    EXPECT_EQ(metastore_next_offset(src), std::nullopt);

    io().fail_create_multipart(false);

    reconcile();

    // Reconciliation should resume after an IO failure.
    EXPECT_EQ(src->last_reconciled_offset(), kafka::offset{9});
    EXPECT_THAT(metastore_next_offset(src), Optional(kafka::offset{10}));
}

TEST_F(ReconcilerTest, IOMultipartCompleteFailure) {
    auto src = add_source();
    src->add_batch({.count = 10});

    io().fail_complete(true);

    reconcile();

    EXPECT_EQ(src->last_reconciled_offset(), kafka::offset{});
    EXPECT_EQ(metastore_next_offset(src), std::nullopt);

    io().fail_complete(false);

    reconcile();

    // Reconciliation should resume after a multipart complete failure.
    EXPECT_EQ(src->last_reconciled_offset(), kafka::offset{9});
    EXPECT_THAT(metastore_next_offset(src), Optional(kafka::offset{10}));
}

TEST_F(ReconcilerTest, IOMultipartPartUploadFailure) {
    auto src = add_source();

    // Need >16MiB (default part size) to trigger an intermediate part upload.
    // 260 uncompressed batches of 64KiB records ≈ 16.6MiB.
    constexpr auto batch_count = 260;
    constexpr auto record_count = 1;
    constexpr auto record_size = 64_KiB;
    for (size_t i = 0; i < batch_count; ++i) {
        src->add_batch(
          {.allow_compression = false,
           .count = record_count,
           .record_sizes = std::vector<size_t>(record_count, record_size)});
    }

    io().fail_upload_part(true);

    reconcile();

    EXPECT_EQ(src->last_reconciled_offset(), kafka::offset{});
    EXPECT_EQ(metastore_next_offset(src), std::nullopt);

    io().fail_upload_part(false);

    reconcile();

    // After recovery, data should be reconciled.
    EXPECT_GT(src->last_reconciled_offset(), kafka::offset{0});
}

TEST_F(ReconcilerTest, IOMultipartAbortFailure) {
    auto src = add_source();
    src->add_batch({.count = 10});

    // When complete fails, the multipart_upload::complete() method attempts
    // to abort via abort_on_error(). If abort also fails, it should be
    // handled gracefully (the multipart_upload layer catches abort errors).
    io().fail_complete(true);
    io().fail_abort(true);

    reconcile();

    EXPECT_EQ(src->last_reconciled_offset(), kafka::offset{});
    EXPECT_EQ(metastore_next_offset(src), std::nullopt);

    io().fail_complete(false);
    io().fail_abort(false);

    reconcile();

    // Reconciliation should resume after the combined failure.
    EXPECT_EQ(src->last_reconciled_offset(), kafka::offset{9});
    EXPECT_THAT(metastore_next_offset(src), Optional(kafka::offset{10}));
}

TEST_F(ReconcilerTest, LROUpdateFailure) {
    // Note that this test tests the behavior when the LRO and the metastore's
    // next_offset go out of sync, so it's also a test for recovery in cases
    // like a reconciliation race between an old and new leader.
    auto src1 = add_source();
    auto src2 = add_source();

    src1->add_batch({.count = 10});
    src2->add_batch({.count = 10});

    src1->fail_set_lro(true);

    reconcile();

    // LRO failure doesn't stop metastore update, and it doesn't
    // interfere with reconciliation of other sources.
    EXPECT_EQ(src1->last_reconciled_offset(), kafka::offset{});
    EXPECT_THAT(metastore_next_offset(src1), Optional(kafka::offset{10}));
    EXPECT_EQ(src2->last_reconciled_offset(), kafka::offset{9});
    EXPECT_THAT(metastore_next_offset(src2), Optional(kafka::offset{10}));

    src1->fail_set_lro(false);

    src1->add_batch({.count = 10});
    src2->add_batch({.count = 10});

    reconcile();

    // The reconciler will adjust the LRO based on the metastore's next offset
    // in the next round, but it won't make any more progress.
    EXPECT_EQ(src1->last_reconciled_offset(), kafka::offset{9});
    EXPECT_THAT(metastore_next_offset(src1), Optional(kafka::offset{10}));
    EXPECT_EQ(src2->last_reconciled_offset(), kafka::offset{19});
    EXPECT_THAT(metastore_next_offset(src2), Optional(kafka::offset{20}));

    reconcile();

    // In the round after that, reconciliation makes progress.
    EXPECT_EQ(src1->last_reconciled_offset(), kafka::offset{19});
    EXPECT_THAT(metastore_next_offset(src1), Optional(kafka::offset{20}));
    EXPECT_EQ(src2->last_reconciled_offset(), kafka::offset{19});
    EXPECT_THAT(metastore_next_offset(src2), Optional(kafka::offset{20}));
}

TEST_F(ReconcilerTest, MultipleSourcesWithFailures) {
    auto src1 = add_source();
    auto src2 = add_source();
    auto src3 = add_source();

    src1->add_batch({.count = 10});
    src2->add_batch({.count = 20});
    src3->add_batch({.count = 30});

    src2->fail_make_reader(true);

    reconcile();

    // When one source in an object fails, then the entire object doesn't fail.
    // NB: This depends on the simple_metastore grouping all sources into the
    //     same object.
    EXPECT_EQ(src1->last_reconciled_offset(), kafka::offset{9});
    EXPECT_EQ(src2->last_reconciled_offset(), kafka::offset{});
    EXPECT_EQ(src3->last_reconciled_offset(), kafka::offset{29});

    EXPECT_EQ(metastore_next_offset(src1), kafka::offset{10});
    EXPECT_EQ(metastore_next_offset(src2), std::nullopt);
    EXPECT_EQ(metastore_next_offset(src3), kafka::offset{30});
}

TEST_F(ReconcilerTest, TermTracking) {
    auto src = add_source();

    src->add_batch({.count = 10}, model::term_id{1});
    src->add_batch({.count = 10}, model::term_id{1});
    src->add_batch({.count = 10}, model::term_id{2});
    src->add_batch({.count = 10}, model::term_id{3});

    reconcile();

    EXPECT_EQ(src->last_reconciled_offset(), kafka::offset{39});
    EXPECT_THAT(metastore_next_offset(src), Optional(kafka::offset{40}));

    auto term_at_5 = metastore()
                       .get_term_for_offset(
                         src->topic_id_partition(), kafka::offset{5})
                       .get();
    auto term_at_25 = metastore()
                        .get_term_for_offset(
                          src->topic_id_partition(), kafka::offset{25})
                        .get();
    auto term_at_35 = metastore()
                        .get_term_for_offset(
                          src->topic_id_partition(), kafka::offset{35})
                        .get();

    EXPECT_TRUE(term_at_5.has_value());
    EXPECT_EQ(term_at_5.value(), model::term_id{1});
    EXPECT_TRUE(term_at_25.has_value());
    EXPECT_EQ(term_at_25.value(), model::term_id{2});
    EXPECT_TRUE(term_at_35.has_value());
    EXPECT_EQ(term_at_35.value(), model::term_id{3});
}

TEST_F(ReconcilerTest, OffsetAndTimestampTracking) {
    auto src = add_source();

    model::timestamp base_ts{1000000};
    src->add_batch({.count = 10, .timestamp = base_ts});
    src->add_batch(
      {.count = 10, .timestamp = model::timestamp{base_ts() + 1000}});

    reconcile();

    EXPECT_EQ(src->last_reconciled_offset(), kafka::offset{19});

    src->add_batch(
      {.count = 10, .timestamp = model::timestamp{base_ts() + 2000}});

    reconcile();

    EXPECT_EQ(src->last_reconciled_offset(), kafka::offset{29});

    // Offset queries.
    auto obj_at_offset_0 = metastore()
                             .get_first_ge(
                               src->topic_id_partition(), kafka::offset{0})
                             .get();
    EXPECT_TRUE(obj_at_offset_0.has_value());

    auto obj_at_offset_25 = metastore()
                              .get_first_ge(
                                src->topic_id_partition(), kafka::offset{25})
                              .get();
    EXPECT_TRUE(obj_at_offset_25.has_value());
    EXPECT_NE(obj_at_offset_0.value().oid, obj_at_offset_25.value().oid);

    auto obj_beyond_offset = metastore()
                               .get_first_ge(
                                 src->topic_id_partition(), kafka::offset{1000})
                               .get();
    EXPECT_FALSE(obj_beyond_offset.has_value());
    EXPECT_EQ(obj_beyond_offset.error(), l1::metastore::errc::out_of_range);

    // Timestamp queries.
    auto obj_at_ts = metastore()
                       .get_first_ge(
                         src->topic_id_partition(), kafka::offset{}, base_ts)
                       .get();
    EXPECT_TRUE(obj_at_ts.has_value());

    auto obj_at_later_ts = metastore()
                             .get_first_ge(
                               src->topic_id_partition(),
                               kafka::offset{},
                               model::timestamp{base_ts() + 1500})
                             .get();
    EXPECT_TRUE(obj_at_later_ts.has_value());
    EXPECT_NE(obj_at_ts.value().oid, obj_at_later_ts.value().oid);

    auto obj_beyond_ts = metastore()
                           .get_first_ge(
                             src->topic_id_partition(),
                             kafka::offset{},
                             model::timestamp{base_ts() + 10000})
                           .get();
    EXPECT_FALSE(obj_beyond_ts.has_value());
    EXPECT_EQ(obj_beyond_ts.error(), l1::metastore::errc::out_of_range);
}

TEST_F(ReconcilerTest, TopicNotDueIsNotReconciled) {
    auto src = add_source();
    src->add_batch({.count = 10});

    // First reconcile - topic is due because it's new (last_reconciled =
    // time_point::min()).
    reconcile();
    EXPECT_EQ(src->last_reconciled_offset(), kafka::offset{9});

    // Add more data.
    src->add_batch({.count = 10});

    // Reconcile without advancing clock - topic is not due yet.
    reconcile_without_advancing_clock();

    // LRO should not have advanced because the topic wasn't due.
    EXPECT_EQ(src->last_reconciled_offset(), kafka::offset{9});

    // Now advance clock and reconcile again.
    reconcile();

    // Now the data should be reconciled.
    EXPECT_EQ(src->last_reconciled_offset(), kafka::offset{19});
}

TEST_F(ReconcilerTest, OnlyDueTopicIsReconciled) {
    // Create topic1 and reconcile it.
    const model::topic tp1{"topic1"};
    const model::topic_id tid1 = model::topic_id::create();
    auto src1 = add_source(tp1, tid1);
    src1->add_batch({.count = 10});

    reconcile();
    EXPECT_EQ(src1->last_reconciled_offset(), kafka::offset{9});

    // Add more data to topic1.
    src1->add_batch({.count = 10});

    // Now create topic2 - it will be immediately due since it's new.
    const model::topic tp2{"topic2"};
    const model::topic_id tid2 = model::topic_id::create();
    auto src2 = add_source(tp2, tid2);
    src2->add_batch({.count = 20});

    // Reconcile without advancing clock.
    // Topic2 should be reconciled (new topic, immediately due).
    // Topic1 should NOT be reconciled (not due yet).
    reconcile_without_advancing_clock();

    // Topic1's LRO should not have advanced.
    EXPECT_EQ(src1->last_reconciled_offset(), kafka::offset{9});
    // Topic2 should be fully reconciled.
    EXPECT_EQ(src2->last_reconciled_offset(), kafka::offset{19});

    // Now advance clock and reconcile - topic1 should now be reconciled.
    reconcile();
    EXPECT_EQ(src1->last_reconciled_offset(), kafka::offset{19});
}

// Regression test: detaching a source during reconciliation (e.g. due to a
// leadership change) must not leave an orphaned topic scheduler. Before the
// fix, get_or_create_topic_scheduler in the post-reconciliation path would
// recreate the scheduler with partition_count=0 after detach had removed it,
// causing compute_next_wait to see it as perpetually due and busy-loop.
TEST_F(ReconcilerTest, DetachDuringReconcileDoesNotOrphanScheduler) {
    // Two topics: src1 will be detached mid-reconciliation, src2 stays.
    auto src1 = add_source();
    src1->add_batch({.count = 10});
    auto src2 = add_source();
    src2->add_batch({.count = 10});

    // Detach src1 when it's read during reconciliation. This simulates
    // a leadership change firing during an async reconciliation pass.
    auto ntp1 = src1->ntp();
    src1->set_on_make_reader([this, ntp1] { _reconciler.detach(ntp1); });

    reconcile();

    // The scheduler for the detached topic must not survive. With the bug,
    // it would be recreated with partition_count=0 and never cleaned up.
    // A second reconcile() would then hit the vassert in reconcile() because
    // the orphaned scheduler has no matching sources.
    reconcile();
    EXPECT_EQ(_reconciler.topic_scheduler_count_for_tests(), 1);
}

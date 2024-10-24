/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#include "container/fragmented_vector.h"
#include "datalake/coordinator/state.h"
#include "datalake/coordinator/state_update.h"
#include "datalake/coordinator/tests/state_test_utils.h"
#include "datalake/coordinator/translated_offset_range.h"
#include "model/fundamental.h"

#include <gtest/gtest.h>

#include <vector>

using namespace datalake::coordinator;

namespace {
const model::topic topic{"test_topic"};
const model::partition_id pid{0};
const model::topic_partition tp{topic, pid};

// Asserts that the given ranges can't be applied to the given partition state.
void check_add_doesnt_apply(
  topics_state& state,
  const model::topic_partition& tp,
  const std::vector<std::pair<int64_t, int64_t>>& offset_bounds) {
    // We should fail to build the update in the first place.
    auto update = add_files_update::build(
      state, tp, make_pending_files(offset_bounds));
    EXPECT_TRUE(update.has_error());

    // Also explicitly build the bad update and make sure it doesn't apply.
    auto res
      = add_files_update{.tp = tp, .entries = make_pending_files(offset_bounds)}
          .apply(state);
    EXPECT_TRUE(res.has_error());
}

// Asserts that the commit offset can't be applied to the partition state.
void check_commit_doesnt_apply(
  topics_state& state,
  const model::topic_partition& tp,
  int64_t commit_offset) {
    // We should fail to build the update in the first place.
    auto update = mark_files_committed_update::build(
      state, tp, kafka::offset{commit_offset});
    EXPECT_TRUE(update.has_error());

    // Also explicitly build the bad update and make sure it doesn't apply.
    auto res
      = mark_files_committed_update{.tp = tp, .new_committed = kafka::offset{commit_offset}}
          .apply(state);
    EXPECT_TRUE(res.has_error());
}

} // namespace

TEST(StateUpdateTest, TestAddFile) {
    topics_state state;
    auto update = add_files_update::build(
      state, tp, make_pending_files({{0, 100}}));
    // We can always add files to a topic or partition that isn't yet tracked.
    ASSERT_FALSE(update.has_error());
    EXPECT_FALSE(state.partition_state(tp).has_value());

    // Now apply the update and check that we have the expected tracked file.
    auto res = update.value().apply(state);
    ASSERT_FALSE(res.has_error());
    ASSERT_NO_FATAL_FAILURE(
      check_partition(state, tp, std::nullopt, {{0, 100}}));

    // Apply the same update won't work because it doesn't align with the back
    // of the last entry, as well as a few others that don't align.
    ASSERT_NO_FATAL_FAILURE(check_add_doesnt_apply(state, tp, {{0, 100}}));
    ASSERT_NO_FATAL_FAILURE(check_add_doesnt_apply(state, tp, {{0, 101}}));
    ASSERT_NO_FATAL_FAILURE(check_add_doesnt_apply(state, tp, {{100, 100}}));
    ASSERT_NO_FATAL_FAILURE(check_add_doesnt_apply(state, tp, {{102, 102}}));
    ASSERT_NO_FATAL_FAILURE(
      check_partition(state, tp, std::nullopt, {{0, 100}}));

    // Now build one that does align properly.
    update = add_files_update::build(
      state, tp, make_pending_files({{101, 200}}));
    ASSERT_FALSE(update.has_error());
    res = update.value().apply(state);
    ASSERT_FALSE(res.has_error());
    ASSERT_NO_FATAL_FAILURE(
      check_partition(state, tp, std::nullopt, {{0, 100}, {101, 200}}));
}

TEST(StateUpdateTest, TestAddFileWithCommittedOffset) {
    // First, set up an existing committed offset, e.g. if we've committed all
    // our files up to offset 100.
    topics_state state;
    state.topic_to_state[topic].pid_to_pending_files[pid].last_committed
      = kafka::offset{100};

    // Try a few adds that don't apply because they don't align with the
    // committed offset.
    ASSERT_NO_FATAL_FAILURE(check_add_doesnt_apply(state, tp, {{0, 100}}));
    ASSERT_NO_FATAL_FAILURE(check_add_doesnt_apply(state, tp, {{100, 100}}));
    ASSERT_NO_FATAL_FAILURE(check_add_doesnt_apply(state, tp, {{102, 102}}));
    ASSERT_NO_FATAL_FAILURE(check_partition(state, tp, 100, {}));

    // Now successfully add some.
    auto update = add_files_update::build(
      state, tp, make_pending_files({{101, 101}, {102, 200}}));
    ASSERT_FALSE(update.has_error());
    auto res = update.value().apply(state);
    EXPECT_FALSE(res.has_error());
    ASSERT_NO_FATAL_FAILURE(
      check_partition(state, tp, 100, {{101, 101}, {102, 200}}));

    // Try a few more that don't align, this time with a non-empty pending
    // files list.
    ASSERT_NO_FATAL_FAILURE(check_add_doesnt_apply(state, tp, {{100, 100}}));
    ASSERT_NO_FATAL_FAILURE(check_add_doesnt_apply(state, tp, {{101, 101}}));
    ASSERT_NO_FATAL_FAILURE(check_add_doesnt_apply(state, tp, {{200, 200}}));
    ASSERT_NO_FATAL_FAILURE(
      check_partition(state, tp, 100, {{101, 101}, {102, 200}}));

    // Now successfully add some, this time with a non-empty pending files.
    update = add_files_update::build(
      state, tp, make_pending_files({{201, 201}}));
    ASSERT_FALSE(update.has_error());
    res = update.value().apply(state);
    EXPECT_FALSE(res.has_error());
    ASSERT_NO_FATAL_FAILURE(
      check_partition(state, tp, 100, {{101, 101}, {102, 200}, {201, 201}}));
}

TEST(StateUpdateTest, TestMarkCommitted) {
    topics_state state;

    // When there's no pending files, we can't commit anything.
    ASSERT_NO_FATAL_FAILURE(check_commit_doesnt_apply(state, tp, 0));
    ASSERT_NO_FATAL_FAILURE(check_commit_doesnt_apply(state, tp, 100));
    ASSERT_NO_FATAL_FAILURE(check_commit_doesnt_apply(state, tp, 101));

    // Even if we explicitly have a committed offset already, we still have no
    // pending files and therefore can't commit.
    state.topic_to_state[topic].pid_to_pending_files[pid].last_committed
      = kafka::offset{100};
    ASSERT_NO_FATAL_FAILURE(check_commit_doesnt_apply(state, tp, 0));
    ASSERT_NO_FATAL_FAILURE(check_commit_doesnt_apply(state, tp, 100));
    ASSERT_NO_FATAL_FAILURE(check_commit_doesnt_apply(state, tp, 101));
    ASSERT_NO_FATAL_FAILURE(check_partition(state, tp, 100, {}));

    // Now add some files.
    auto res = add_files_update::build(
                 state, tp, make_pending_files({{101, 200}}))
                 .value()
                 .apply(state);
    EXPECT_FALSE(res.has_error());
    ASSERT_NO_FATAL_FAILURE(check_partition(state, tp, 100, {{101, 200}}));

    // The commit should only succeed if it aligns with a file end offset.
    ASSERT_NO_FATAL_FAILURE(check_commit_doesnt_apply(state, tp, 100));
    ASSERT_NO_FATAL_FAILURE(check_commit_doesnt_apply(state, tp, 101));
    ASSERT_NO_FATAL_FAILURE(check_commit_doesnt_apply(state, tp, 199));
    ASSERT_NO_FATAL_FAILURE(check_commit_doesnt_apply(state, tp, 201));
    ASSERT_NO_FATAL_FAILURE(check_partition(state, tp, 100, {{101, 200}}));

    res = mark_files_committed_update::build(state, tp, kafka::offset{200})
            .value()
            .apply(state);
    EXPECT_FALSE(res.has_error());
    ASSERT_NO_FATAL_FAILURE(check_partition(state, tp, 200, {}));

    // Now let's try commit when there are multiple pending files.
    // First, add multiple files.
    res = add_files_update::build(
            state, tp, make_pending_files({{201, 205}, {206, 210}, {211, 220}}))
            .value()
            .apply(state);
    EXPECT_FALSE(res.has_error());
    ASSERT_NO_FATAL_FAILURE(
      check_partition(state, tp, 200, {{201, 205}, {206, 210}, {211, 220}}));

    // Again, commits that aren't aligned with an end offset will fail.
    ASSERT_NO_FATAL_FAILURE(check_commit_doesnt_apply(state, tp, 200));
    ASSERT_NO_FATAL_FAILURE(check_commit_doesnt_apply(state, tp, 201));
    ASSERT_NO_FATAL_FAILURE(check_commit_doesnt_apply(state, tp, 206));
    ASSERT_NO_FATAL_FAILURE(check_commit_doesnt_apply(state, tp, 211));
    ASSERT_NO_FATAL_FAILURE(
      check_partition(state, tp, 200, {{201, 205}, {206, 210}, {211, 220}}));

    // But it should work with one of the inner files.
    res = mark_files_committed_update::build(state, tp, kafka::offset{205})
            .value()
            .apply(state);
    EXPECT_FALSE(res.has_error());
    ASSERT_NO_FATAL_FAILURE(
      check_partition(state, tp, 205, {{206, 210}, {211, 220}}));

    // And it should work with the last file.
    res = mark_files_committed_update::build(state, tp, kafka::offset{220})
            .value()
            .apply(state);
    EXPECT_FALSE(res.has_error());
    ASSERT_NO_FATAL_FAILURE(check_partition(state, tp, 220, {}));
}

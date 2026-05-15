/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#include "cloud_topics/reconciler/tests/test_utils.h"
#include "model/fundamental.h"

#include <seastar/core/lowres_clock.hh>

#include <gtest/gtest.h>

#include <optional>

using cloud_topics::reconciler::test::fake_source;

namespace {

ss::shared_ptr<fake_source> make_test_source() {
    auto ntp = model::ntp(
      model::ns("test"), model::topic("t"), model::partition_id(0));
    auto tidp = model::topic_id_partition(
      model::topic_id::create(), model::partition_id(0));
    return ss::make_shared<fake_source>(std::move(ntp), tidp);
}

} // namespace

TEST(reconciliation_source, local_retention_state_defaults) {
    auto src = make_test_source();
    EXPECT_EQ(src->local_retention_bytes_since_eval(), 0u);
    EXPECT_FALSE(src->local_retention_last_eval_time().has_value());
    EXPECT_FALSE(src->local_retention_last_published().has_value());
}

TEST(reconciliation_source, local_retention_bytes_mutators) {
    auto src = make_test_source();
    src->add_local_retention_bytes(100);
    src->add_local_retention_bytes(50);
    EXPECT_EQ(src->local_retention_bytes_since_eval(), 150u);
    src->reset_local_retention_eval_counter();
    EXPECT_EQ(src->local_retention_bytes_since_eval(), 0u);
}

TEST(reconciliation_source, local_retention_last_eval_time_mutator) {
    auto src = make_test_source();
    auto now = ss::lowres_clock::now();
    src->set_local_retention_last_eval_time(now);
    ASSERT_TRUE(src->local_retention_last_eval_time().has_value());
    EXPECT_EQ(*src->local_retention_last_eval_time(), now);
}

TEST(reconciliation_source, local_retention_last_published_mutator) {
    auto src = make_test_source();
    src->set_local_retention_last_published(kafka::offset{42});
    ASSERT_TRUE(src->local_retention_last_published().has_value());
    EXPECT_TRUE(src->local_retention_last_published()->has_value());
    EXPECT_EQ(**src->local_retention_last_published(), kafka::offset{42});

    src->set_local_retention_last_published(std::nullopt);
    ASSERT_TRUE(src->local_retention_last_published().has_value());
    EXPECT_FALSE(src->local_retention_last_published()->has_value());
}

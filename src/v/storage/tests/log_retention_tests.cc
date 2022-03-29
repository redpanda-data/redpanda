// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "storage/tests/utils/disk_log_builder.h"
// fixture
#include "test_utils/fixture.h"

#include <optional>

struct gc_fixture {
    storage::disk_log_builder builder;
};

FIXTURE_TEST(empty_log_garbage_collect, gc_fixture) {
    builder | storage::start()
      | storage::garbage_collect(
        model::timestamp::now(), std::make_optional<size_t>(1024))
      | storage::stop();
}

FIXTURE_TEST(retention_test_time, gc_fixture) {
    builder | storage::start() | storage::add_segment(0)
      | storage::add_random_batch(0, 100, storage::maybe_compress_batches::yes)
      | storage::add_random_batch(100, 2, storage::maybe_compress_batches::yes)
      | storage::add_segment(102)
      | storage::add_random_batch(102, 2, storage::maybe_compress_batches::yes)
      | storage::add_segment(104) | storage::add_random_batches(104, 3);
    builder.get_log().set_collectible_offset(
      builder.get_log().offsets().dirty_offset);
    BOOST_TEST_MESSAGE(
      "Should not collect segments with timestamp older than 1");
    BOOST_CHECK_EQUAL(builder.get_log().segment_count(), 3);
    builder | storage::garbage_collect(model::timestamp(1), std::nullopt);
    BOOST_CHECK_EQUAL(builder.get_log().segment_count(), 3);

    BOOST_TEST_MESSAGE("Should not collect segments because size is infinity");
    builder
      | storage::garbage_collect(
        model::timestamp(1),
        std::make_optional<size_t>(std::numeric_limits<size_t>::max()));
    BOOST_CHECK_EQUAL(builder.get_log().segment_count(), 3);

    BOOST_TEST_MESSAGE("Should leave one active segment");
    builder | storage::garbage_collect(model::timestamp::now(), std::nullopt)
      | storage::stop();

    BOOST_CHECK_EQUAL(builder.get_log().segment_count(), 1);
}

FIXTURE_TEST(retention_test_size, gc_fixture) {
    builder | storage::start() | storage::add_segment(0)
      | storage::add_random_batch(0, 100, storage::maybe_compress_batches::yes)
      | storage::add_random_batch(100, 2, storage::maybe_compress_batches::yes)
      | storage::add_segment(102)
      | storage::add_random_batch(102, 2, storage::maybe_compress_batches::yes)
      | storage::add_segment(104) | storage::add_random_batches(104, 3);
    builder.get_log().set_collectible_offset(
      builder.get_log().offsets().dirty_offset);
    BOOST_TEST_MESSAGE("Should not collect segments because size equal to "
                       "current partition size");
    builder
      | storage::garbage_collect(
        model::timestamp(1),
        std::make_optional(
          builder.get_disk_log_impl().get_probe().partition_size()));
    BOOST_CHECK_EQUAL(builder.get_log().segment_count(), 3);

    BOOST_TEST_MESSAGE(
      "Should collect all inactive segments, leaving an active one");
    builder
      | storage::garbage_collect(model::timestamp(1), std::optional<size_t>(0))
      | storage::stop();

    BOOST_CHECK_EQUAL(builder.get_log().segment_count(), 1);
}

FIXTURE_TEST(retention_test_after_truncation, gc_fixture) {
    BOOST_TEST_MESSAGE("Should be safe to garbage collect after truncation");
    builder | storage::start() | storage::add_segment(0)
      | storage::add_random_batch(0, 100, storage::maybe_compress_batches::yes)
      | storage::truncate_log(model::offset(0))
      | storage::garbage_collect(model::timestamp::now(), std::nullopt)
      | storage::stop();
    builder.get_log().set_collectible_offset(
      builder.get_log().offsets().dirty_offset);
    BOOST_CHECK_EQUAL(builder.get_log().segment_count(), 0);
    BOOST_CHECK_EQUAL(
      builder.get_disk_log_impl().get_probe().partition_size(), 0);
}

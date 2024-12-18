/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#include "consensus.h"
#include "datalake/translation/types.h"
#include "datalake/translation/utils.h"
#include "fundamental.h"
#include "gtest/gtest.h"
#include "model/fundamental.h"
#include "model/record_batch_types.h"
#include "model/tests/random_batch.h"
#include "storage/disk_log_impl.h"
#include "storage/log.h"
#include "storage/log_manager.h"
#include "storage/ntp_config.h"
#include "storage/tests/utils/disk_log_builder.h"
#include "test_utils/tmp_dir.h"

#include <seastar/util/defer.hh>

// Checks that the highest translated log offset is equivalent to
// an expected offset for a translated kafka offset.
//
// Returns true iff expected_offset == the highest translated log offset.
bool check_translated_log_offset(
  ss::shared_ptr<storage::log> log,
  kafka::offset translated_offset,
  model::offset expected_offset) {
    auto translated_log_offset
      = datalake::translation::get_translated_log_offset(
        log, translated_offset);
    return expected_offset == translated_log_offset;
}

TEST(TranslatedLogOffsetTest, TestTranslatedOffsetsGrowingLog) {
    temporary_dir tmp_dir("translated_offset_test");
    auto data_path = tmp_dir.get_path();

    model::ntp test_ntp{"kafka", "tapioca", 0};
    auto log_cfg = storage::log_config{
      data_path.string(),
      1024,
      ss::default_priority_class(),
      storage::make_sanitized_file_config()};
    auto translator_batch_types = raft::offset_translator_batch_types(test_ntp);
    auto raft_group_id = raft::group_id{0};
    storage::disk_log_builder b{
      std::move(log_cfg), std::move(translator_batch_types), raft_group_id};

    b.start(storage::ntp_config{test_ntp, {data_path}}).get();
    auto defer = ss::defer([&b]() { b.stop().get(); });

    auto log = b.get_log();

    // Must initialize translator state.
    log->start(std::nullopt).get();

    // A good property to check.
    ASSERT_TRUE(
      check_translated_log_offset(log, kafka::offset{}, model::offset{}));

    // Kaf offsets: []
    // Log offsets: []
    ASSERT_TRUE(
      check_translated_log_offset(log, kafka::offset{0}, model::offset{0}));

    auto offset = 0;
    auto make_and_add_record = [&offset, &b](model::record_batch_type bt) {
        auto batch = model::test::make_random_batch(
          model::offset{offset++}, 1, true, bt);
        b.add_batch(std::move(batch)).get();
    };

    make_and_add_record(model::record_batch_type::raft_data);
    // Kaf offsets: [0]
    // Log offsets: [0]
    //               |
    //               |-> TO/TLO
    ASSERT_TRUE(
      check_translated_log_offset(log, kafka::offset{0}, model::offset{0}));

    make_and_add_record(model::record_batch_type::raft_configuration);
    // Kaf offsets: [0]
    // Log offsets: [0]    [C]
    //               |      |
    //               |-> TO |-> TLO
    ASSERT_TRUE(
      check_translated_log_offset(log, kafka::offset{0}, model::offset{1}));

    make_and_add_record(model::record_batch_type::raft_configuration);
    // Kaf offsets: [0]
    // Log offsets: [0] [C] [C]
    //               |       |
    //               |-> TO  |-> TLO
    ASSERT_TRUE(
      check_translated_log_offset(log, kafka::offset{0}, model::offset{2}));

    make_and_add_record(model::record_batch_type::raft_configuration);
    // Kaf offsets: [0]
    // Log offsets: [0] [C] [C] [C]
    //               |           |
    //               |-> TO      |-> TLO
    ASSERT_TRUE(
      check_translated_log_offset(log, kafka::offset{0}, model::offset{3}));

    make_and_add_record(model::record_batch_type::raft_data);
    // Kaf offsets: [0]  .   .   .         [1]
    // Log offsets: [0] [C] [C] [C]        [4]
    //               |           |          |
    //               |-> TO_0    |-> TLO_0  |
    //                                      |-> TO_1/TLO_1
    ASSERT_TRUE(
      check_translated_log_offset(log, kafka::offset{0}, model::offset{3}));
    ASSERT_TRUE(
      check_translated_log_offset(log, kafka::offset{1}, model::offset{4}));

    make_and_add_record(model::record_batch_type::raft_data);
    // Kaf offsets: [0]  .   .   .         [1] [2]
    // Log offsets: [0] [C] [C] [C]        [4] [5]
    //               |           |          |   |
    //               |-> TO_0    |-> TLO_0  |   |
    //                                      |-----> TO_1/TLO_1
    //                                          |-> TO_2/TLO_5
    ASSERT_TRUE(
      check_translated_log_offset(log, kafka::offset{0}, model::offset{3}));
    ASSERT_TRUE(
      check_translated_log_offset(log, kafka::offset{1}, model::offset{4}));
    ASSERT_TRUE(
      check_translated_log_offset(log, kafka::offset{2}, model::offset{5}));

    make_and_add_record(model::record_batch_type::raft_configuration);
    // Kaf offsets: [0]  .   .   .         [1] [2]
    // Log offsets: [0] [C] [C] [C]        [4] [5]             [C]
    //               |           |          |   |               |
    //               |-> TO_0    |-> TLO_0  |   |               |
    //                                      |-----> TO_1/TLO_1  |
    //                                          |-> TO_2        |-> TLO_2
    ASSERT_TRUE(
      check_translated_log_offset(log, kafka::offset{0}, model::offset{3}));
    ASSERT_TRUE(
      check_translated_log_offset(log, kafka::offset{1}, model::offset{4}));
    ASSERT_TRUE(
      check_translated_log_offset(log, kafka::offset{2}, model::offset{6}));
}

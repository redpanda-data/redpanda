/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "kafka/client/direct_consumer/api_types.h"
#include "kafka/client/direct_consumer/direct_consumer.h"
#include "kafka/client/direct_consumer/fetcher.h"
#include "kafka/protocol/errors.h"
#include "kafka/protocol/types.h"
#include "model/record.h"
#include "model/tests/random_batch.h"
#include "test_utils/test.h"

#include <gtest/gtest.h>

#include <iterator>

using namespace kafka;
using namespace kafka::client;

namespace {

const aborted_transaction default_aborted_transaction = aborted_transaction{
  .producer_id = kafka::producer_id{101}, .first_offset = int64_t{8}};

const model::partition_id default_partition_id{1};

ss::future<std::pair<chunked_vector<model::record_batch>, size_t>>
get_random_batches() {
    auto random_batches = co_await model::test::make_random_batches(
      model::offset(0));

    chunked_vector<model::record_batch> to_return{};
    std::ranges::move(std::move(random_batches), std::back_inserter(to_return));

    auto number_of_batches = to_return.size();
    co_return std::pair{std::move(to_return), number_of_batches};
}

partition_data get_default_partition_data() {
    // numbers are mostly arbitrary, assigned to avoid duplicate values
    return partition_data{
      .partition_index = model::partition_id{default_partition_id},
      .error_code = kafka::error_code::none,
      .high_watermark = model::offset{13},
      .last_stable_offset = model::offset{7},
      .log_start_offset = model::offset{5},
      .aborted_transactions
      = std::optional<chunked_vector<aborted_transaction>>{chunked_vector<
        aborted_transaction>({default_aborted_transaction})},
      .preferred_read_replica = model::node_id{27},
      .records = std::optional<kafka::batch_reader>{std::nullopt},
      .diverging_epoch
      = diverging_epoch_end_offset{.epoch = kafka::leader_epoch{52}, .end_offset = model::offset{6}},
      .current_leader
      = leader_id_and_epoch{.leader_id = model::node_id{19}, .leader_epoch = kafka::leader_epoch{67}},
      .snapshot_id
      = snapshot_id{.end_offset = model::offset(11), .epoch = kafka::leader_epoch{63}},
      .unknown_tags = tagged_fields{},
      .has_to_be_included = bool{false}};
}

void assert_defaults(const fetched_partition_data& data) {
    auto default_partition_data = get_default_partition_data();

    ASSERT_EQ(data.partition_id, default_partition_data.partition_index);
    ASSERT_EQ(
      data.leader_epoch, default_partition_data.current_leader.leader_epoch);
    ASSERT_EQ(
      data.start_offset,
      model::offset_cast(default_partition_data.log_start_offset));
    ASSERT_EQ(
      data.high_watermark,
      model::offset_cast(default_partition_data.high_watermark));
    ASSERT_EQ(
      data.last_stable_offset,
      model::offset_cast(default_partition_data.last_stable_offset));

    ASSERT_TRUE(data.aborted_transactions.has_value());
    ASSERT_EQ(
      default_partition_data.aborted_transactions->size(),
      data.aborted_transactions->size());
    ASSERT_EQ(data.aborted_transactions->size(), 1);
    ASSERT_EQ(
      default_partition_data.aborted_transactions->at(0).producer_id,
      data.aborted_transactions->at(0).producer_id);
    ASSERT_EQ(
      default_partition_data.aborted_transactions->at(0).first_offset,
      data.aborted_transactions->at(0).first_offset);
}
} // namespace

class fetcher_accessor {
public:
    static fetcher::epoch_set default_epoch_set;

    static void do_process_partition_smoke() {
        auto partition_data = get_default_partition_data();
        auto [batches, size] = get_random_batches().get();

        auto actions = fetcher::do_process_partition_response(
          std::move(partition_data),
          std::move(batches),
          size,
          default_epoch_set);

        ASSERT_FALSE(actions.should_reset_offsets);
        ASSERT_FALSE(actions.should_update_metadata);
        ASSERT_EQ(actions.error, kafka::error_code::none);
        ASSERT_TRUE(actions.is_dirty);
        ASSERT_TRUE(actions.maybe_fetched_partition_data.has_value());

        auto& fetched_partition_data
          = actions.maybe_fetched_partition_data.value();

        assert_defaults(fetched_partition_data);
        ASSERT_EQ(
          fetched_partition_data.subscription_epoch,
          default_epoch_set.subscription_epoch);
        ASSERT_EQ(fetched_partition_data.size_bytes, size);
        ASSERT_EQ(fetched_partition_data.data.size(), size);
    }

    static void do_process_partition_retriable() {
        auto partition_data = get_default_partition_data();
        partition_data.error_code = kafka::error_code::not_leader_for_partition;
        auto [batches, size] = get_random_batches().get();

        auto actions = fetcher::do_process_partition_response(
          std::move(partition_data),
          std::move(batches),
          size,
          default_epoch_set);

        ASSERT_FALSE(actions.should_reset_offsets);
        ASSERT_TRUE(actions.should_update_metadata);
        ASSERT_EQ(actions.error, kafka::error_code::not_leader_for_partition);
        ASSERT_TRUE(actions.is_dirty);
        ASSERT_FALSE(actions.maybe_fetched_partition_data.has_value());
    }

    static void do_process_partition_out_of_bounds() {
        auto partition_data = get_default_partition_data();
        partition_data.error_code = kafka::error_code::offset_out_of_range;
        auto [batches, size] = get_random_batches().get();

        auto actions = fetcher::do_process_partition_response(
          std::move(partition_data),
          std::move(batches),
          size,
          default_epoch_set);

        ASSERT_TRUE(actions.should_reset_offsets);
        ASSERT_FALSE(actions.should_update_metadata);
        ASSERT_EQ(actions.error, kafka::error_code::offset_out_of_range);
        ASSERT_FALSE(actions.is_dirty);
        ASSERT_FALSE(actions.maybe_fetched_partition_data.has_value());
    }

    static void do_process_partition_unknown_error() {
        auto partition_data = get_default_partition_data();
        partition_data.error_code = kafka::error_code::unknown_server_error;
        auto [batches, size] = get_random_batches().get();

        auto actions = fetcher::do_process_partition_response(
          std::move(partition_data),
          std::move(batches),
          size,
          default_epoch_set);

        ASSERT_FALSE(actions.should_reset_offsets);
        ASSERT_FALSE(actions.should_update_metadata);
        ASSERT_EQ(actions.error, kafka::error_code::unknown_server_error);
        ASSERT_TRUE(actions.is_dirty);
        ASSERT_TRUE(actions.maybe_fetched_partition_data.has_value());

        auto& fetched_partition_data
          = actions.maybe_fetched_partition_data.value();

        ASSERT_EQ(fetched_partition_data.size_bytes, 0);
        ASSERT_EQ(fetched_partition_data.data.size(), 0);
        ASSERT_EQ(
          fetched_partition_data.error,
          kafka::error_code::unknown_server_error);
        ASSERT_EQ(
          fetched_partition_data.subscription_epoch,
          default_epoch_set.subscription_epoch);
        ::fetched_partition_data expected{};
        expected.partition_id = default_partition_id;

        // check that the remainder of the fields are initialized as per
        // defaults
        ASSERT_EQ(fetched_partition_data.partition_id, expected.partition_id);
        ASSERT_EQ(fetched_partition_data.leader_epoch, expected.leader_epoch);
        ASSERT_EQ(fetched_partition_data.start_offset, expected.start_offset);
        ASSERT_EQ(
          fetched_partition_data.high_watermark, expected.high_watermark);
        ASSERT_EQ(
          fetched_partition_data.last_stable_offset,
          expected.last_stable_offset);
    }

    static void do_process_partition_empty() {
        auto partition_data = get_default_partition_data();

        auto actions = fetcher::do_process_partition_response(
          std::move(partition_data), {}, 0, default_epoch_set);

        ASSERT_FALSE(actions.should_reset_offsets);
        ASSERT_FALSE(actions.should_update_metadata);
        ASSERT_EQ(actions.error, kafka::error_code::none);
        ASSERT_FALSE(actions.is_dirty);
        ASSERT_TRUE(actions.maybe_fetched_partition_data.has_value());

        auto& fetched_partition_data
          = actions.maybe_fetched_partition_data.value();

        assert_defaults(fetched_partition_data);
        ASSERT_EQ(
          fetched_partition_data.subscription_epoch,
          default_epoch_set.subscription_epoch);
        ASSERT_EQ(fetched_partition_data.size_bytes, 0);
        ASSERT_EQ(fetched_partition_data.data.size(), 0);
    }
};

fetcher::epoch_set fetcher_accessor::default_epoch_set = {
  fetcher::fetcher_epoch{111}, subscription_epoch{777}};

TEST(FetcherSuite, DoProcessPartitionSmoke) {
    fetcher_accessor::do_process_partition_smoke();
}
TEST(FetcherSuite, DoProcessPartitionEmpty) {
    fetcher_accessor::do_process_partition_empty();
}
TEST(FetcherSuite, DoProcessPartitionOutOfBounds) {
    fetcher_accessor::do_process_partition_out_of_bounds();
}
TEST(FetcherSuite, DoProcessPartitionRetriable) {
    fetcher_accessor::do_process_partition_retriable();
}
TEST(FetcherSuite, DoProcessPartitionUnknownError) {
    fetcher_accessor::do_process_partition_unknown_error();
}

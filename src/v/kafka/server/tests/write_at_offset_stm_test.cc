// Copyright 2025 Redpanda Data, Inc.
//
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "kafka/server/write_at_offset_stm.h"
#include "raft/tests/stm_test_fixture.h"
#include "test_utils/test.h"

#include <gmock/gmock.h>

namespace {
chunked_vector<model::record_batch>
copy_batches(const chunked_vector<model::record_batch>& batches) {
    chunked_vector<model::record_batch> copy;
    for (auto& batch : batches) {
        copy.push_back(batch.copy());
    }
    return copy;
};

} // namespace

static ss::logger t_log{"test_log"};
using namespace testing;
namespace kafka {
class write_at_offset_stm_accessor {
public:
    static kafka::offset
    get_expected_last_offset(const kafka::write_at_offset_stm& stm) {
        return stm.expected_last_offset();
    }
};
} // namespace kafka
struct WriteAtOffsetStmFixture
  : public raft::stm_raft_fixture<kafka::write_at_offset_stm> {
    std::tuple<ss::shared_ptr<kafka::write_at_offset_stm>> create_stms(
      state_machine_manager_builder& builder, raft_node_instance& node) {
        return builder.create_stm<kafka::write_at_offset_stm>(
          node.raft().get(),
          t_log,
          node.get_kvstore(),
          model::offset_translator_batch_types());
    }

    chunked_vector<kafka::offset>
    start_offsets(const chunked_vector<model::record_batch>& batches) {
        chunked_vector<kafka::offset> offsets;
        for (const auto& batch : batches) {
            offsets.push_back(kafka::offset{batch.base_offset()()});
        }
        return offsets;
    }

    chunked_vector<chunked_vector<model::record_batch>> generate_data(
      kafka::offset starting_at, size_t batch_count, size_t records_per_batch) {
        chunked_vector<chunked_vector<model::record_batch>> batches;

        // randomly split batches into multiple batch groups
        for (auto _ : boost::irange(batch_count)) {
            if (batches.empty() || tests::random_bool()) {
                batches.emplace_back();
            }
            storage::record_batch_builder builder(
              model::record_batch_type::raft_data, model::offset(starting_at));

            for (size_t j = 0; j < records_per_batch; ++j) {
                builder.add_raw_kv(
                  serde::to_iobuf(tests::random_bytes(64)),
                  serde::to_iobuf(starting_at()));
                starting_at++;
            }
            batches.back().push_back(std::move(builder).build());
        }
        return batches;
    }

    std::
      tuple<chunked_vector<chunked_vector<model::record_batch>>, kafka::offset>
      generate_data_with_deltas(
        kafka::offset starting_at,
        size_t batch_count,
        size_t records_per_batch) {
        chunked_vector<chunked_vector<model::record_batch>> batches;

        // randomly split batches into multiple batch groups
        for (auto i : boost::irange(batch_count)) {
            if (batches.empty() || tests::random_bool()) {
                batches.emplace_back();
            }
            auto type
              = i % 2 == 0
                  ? model::record_batch_type::raft_data
                  : model::record_batch_type::partition_properties_update;
            storage::record_batch_builder builder(
              type, kafka::offset_cast(starting_at));

            for (size_t j = 0; j < records_per_batch; ++j) {
                builder.add_raw_kv(
                  serde::to_iobuf(tests::random_bytes(64)),
                  serde::to_iobuf(starting_at()));
                if (type == model::record_batch_type::raft_data) {
                    starting_at++;
                }
            }
            batches.back().push_back(std::move(builder).build());
        }
        return std::make_tuple(std::move(batches), kafka::offset(starting_at));
    }
    struct offset_validating_consumer {
        ss::future<ss::stop_iteration> operator()(model::record_batch& batch) {
            if (batch.header().type != model::record_batch_type::raft_data) {
                co_return ss::stop_iteration::no;
            }
            auto record_iterator = model::record_batch_iterator::create(batch);
            while (record_iterator.has_next()) {
                auto record = record_iterator.next();
                auto expected_offset = serde::from_iobuf<kafka::offset>(
                  record.release_value());
                auto offset = batch.base_offset()
                              + model::offset(record.offset_delta());
                if (offset() != expected_offset) {
                    valid = false;
                    t_log.error(
                      "Expected offset {} does not match {} record offset: {}",
                      expected_offset,
                      batch.header(),
                      offset);
                    co_return ss::stop_iteration::yes;
                }
            }
            co_return ss::stop_iteration::no;
        }

        bool end_of_stream() { return valid; }

        bool valid = true;
    };

    ss::future<bool> validate_node_offsets(raft_node_instance& node) {
        storage::local_log_reader_config r_cfg(
          node.raft()->start_offset(), model::offset::max());
        r_cfg.translate_offsets = model::translate_offsets::yes;
        auto rdr = co_await node.raft()->make_reader(std::move(r_cfg));

        co_return co_await rdr.for_each_ref(
          offset_validating_consumer{}, model::no_timeout);
    }

    ss::future<bool> logs_content_is_valid() {
        for (auto& [id, node] : nodes()) {
            auto result = co_await validate_node_offsets(*node);
            if (!result) {
                t_log.error("Node {} has invalid offsets", id);
                co_return false;
            }
        }
        co_return true;
    }

    size_t
    total_record_count(const chunked_vector<model::record_batch>& batches) {
        size_t count = 0;
        for (const auto& batch : batches) {
            count += batch.record_count();
        }
        return count;
    }

    chunked_vector<chunked_vector<model::record_batch>>
    make_gaps(chunked_vector<chunked_vector<model::record_batch>> data) {
        chunked_vector<chunked_vector<model::record_batch>> filtered_data;

        /**
         * Drop some batches randomly. Sometimes the entire vector of batches
         * are dropped and sometimes individual batches from within the vector
         * are dropped.
         */
        for (auto& batches : data) {
            if (random_generators::get_int(0, 100) < 50) {
                chunked_vector<model::record_batch> inner_filtered;
                for (auto& batch : batches) {
                    if (random_generators::get_int(0, 100) < 70) {
                        inner_filtered.push_back(std::move(batch));
                    }
                }
                if (inner_filtered.empty()) {
                    inner_filtered.push_back(std::move(batches[0]));
                }
                filtered_data.push_back(std::move(inner_filtered));
            }
        }
        if (filtered_data.empty()) {
            filtered_data.push_back(std::move(data[0]));
        }
        return filtered_data;
    }

    std::optional<ss::shared_ptr<kafka::write_at_offset_stm>> get_leader_stm() {
        auto leader_id = get_leader();
        if (!leader_id) {
            return std::nullopt;
        }
        return node(*leader_id)
          .raft()
          ->stm_manager()
          ->get<kafka::write_at_offset_stm>();
    }
};

TEST_F(WriteAtOffsetStmFixture, test_write_at_offset_happy_path) {
    enable_offset_translation();
    initialize_state_machines(3).get();
    wait_for_leader(10s).get();
    auto stm = get_leader_stm();
    kafka::offset gen_offset{0};

    for (int i = 0; i < 10; ++i) {
        auto num_batches = random_generators::get_int(1, 10);
        auto records_per_batch = random_generators::get_int(1, 10);
        auto data = generate_data(gen_offset, num_batches, records_per_batch);
        gen_offset = model::offset_cast(
          model::next_offset(data.back().back().last_offset()));
        for (auto& batches : data) {
            auto expected_offsets = start_offsets(batches);
            auto stages = stm->get()->replicate(
              std::move(batches),
              std::move(expected_offsets),
              std::nullopt,
              10s);
            stages.request_enqueued.get();
            auto result = stages.replicate_finished.get();
            ASSERT_FALSE(result.has_error()) << result.error();
        }
    }

    ASSERT_TRUE(logs_content_is_valid().get());
}

TEST_F(WriteAtOffsetStmFixture, test_write_at_offset_gaps) {
    enable_offset_translation();
    initialize_state_machines(3).get();
    wait_for_leader(10s).get();
    auto stm = get_leader_stm();

    auto data = make_gaps(
      generate_data(kafka::offset{0}, 200, random_generators::get_int(1, 10)));

    kafka::offset expected_prev_offset{};
    for (auto& batches : data) {
        auto o = model::offset_cast(batches.back().last_offset());
        auto expected_offsets = start_offsets(batches);
        auto stages = (*stm)->replicate(
          std::move(batches),
          std::move(expected_offsets),
          expected_prev_offset,
          10s);
        expected_prev_offset = o;
        stages.request_enqueued.get();
        auto result = stages.replicate_finished.get();
        ASSERT_FALSE(result.has_error());
    }

    ASSERT_TRUE(logs_content_is_valid().get());
}

TEST_F(WriteAtOffsetStmFixture, test_start_offset_advancement) {
    enable_offset_translation();
    initialize_state_machines(3).get();
    wait_for_leader(10s).get();
    auto stm = get_leader_stm();
    // generate a batch that stars at non zero offset
    auto data = generate_data(
      kafka::offset{1000123}, 10, random_generators::get_int(1, 10));

    kafka::offset expected_prev_offset{};
    for (auto& batches : data) {
        auto o = model::offset_cast(batches.back().last_offset());
        auto expected_offsets = start_offsets(batches);
        auto stages = (*stm)->replicate(
          std::move(batches),
          std::move(expected_offsets),
          expected_prev_offset,
          10s);
        expected_prev_offset = o;
        stages.request_enqueued.get();
        auto result = stages.replicate_finished.get();
        ASSERT_FALSE(result.has_error());
    }

    ASSERT_TRUE(logs_content_is_valid().get());
}

TEST_F(WriteAtOffsetStmFixture, test_concurrent_writes) {
    enable_offset_translation();
    initialize_state_machines(3).get();
    wait_for_leader(10s).get();
    auto stm = get_leader_stm();

    auto data = make_gaps(
      generate_data(kafka::offset{0}, 200, random_generators::get_int(1, 10)));
    chunked_vector<raft::replicate_stages> all_stages;
    kafka::offset expected_prev_offset{};
    for (auto& batches : data) {
        auto o = model::offset_cast(batches.back().last_offset());
        auto expected_offsets = start_offsets(batches);
        auto stages = (*stm)->replicate(
          std::move(batches),
          std::move(expected_offsets),
          expected_prev_offset,
          10s);
        expected_prev_offset = o;
        all_stages.push_back(std::move(stages));
    }
    auto enq = std::views::transform(
      all_stages, &raft::replicate_stages::request_enqueued);
    ss::when_all(enq.begin(), enq.end()).get();
    auto done = std::views::transform(
      all_stages, &raft::replicate_stages::replicate_finished);
    ss::when_all(done.begin(), done.end()).get();
    ASSERT_TRUE(logs_content_is_valid().get());
}

TEST_F(WriteAtOffsetStmFixture, test_writes_out_of_order) {
    enable_offset_translation();
    initialize_state_machines(3).get();
    auto leader_id = wait_for_leader(10s).get();
    auto stm = node(leader_id)
                 .raft()
                 ->stm_manager()
                 ->get<kafka::write_at_offset_stm>();

    auto num_records_per_batch = random_generators::get_int(1, 10);
    constexpr auto num_batches = 30;
    auto total_records = num_batches * num_records_per_batch;

    auto data = make_gaps(
      generate_data(kafka::offset{0}, num_batches, num_records_per_batch));

    std::vector<size_t> indicies;
    indicies.reserve(total_records);
    for (size_t i = 0; i < data.size(); ++i) {
        indicies.push_back(i);
    }
    // randomize the order of batches, finally we should replicate all of them,
    // this simulate gaps & duplicates
    while (!indicies.empty()) {
        auto idx = random_generators::random_choice(indicies);
        auto b_cp = copy_batches(data[idx]);
        auto o = model::offset_cast(b_cp.back().last_offset());
        auto expected_prev_offset = idx == 0
                                      ? kafka::offset{}
                                      : model::offset_cast(
                                          data[idx - 1].back().last_offset());
        auto expected_offsets = start_offsets(b_cp);
        auto stages = stm->replicate(
          std::move(b_cp),
          std::move(expected_offsets),
          expected_prev_offset,
          10s);
        expected_prev_offset = o;
        stages.request_enqueued.get();
        auto result = stages.replicate_finished.get();
        if (!result.has_error()) {
            std::erase_if(indicies, [idx](size_t i) { return i == idx; });
        }
    }

    ASSERT_TRUE(logs_content_is_valid().get());
}

class WriteAtOffsetConcurrentWritesTest
  : public WriteAtOffsetStmFixture
  , public ::testing::WithParamInterface<int> {
public:
    WriteAtOffsetConcurrentWritesTest()
      : replication_factor(GetParam()) {}

    const int replication_factor;
};

/**
 * This test validates the state machine behaviour when there are
 * concurrent writes to the same partition. The test will create
 * multiple batches and replicate them concurrently. The test
 * will validate that all batches are replicated correctly and
 * that the offsets are correct. In the test the leader is stepping down to
 * enforce leadership changes
 */
TEST_P(WriteAtOffsetConcurrentWritesTest, TestConcurrentWrites) {
    enable_offset_translation();
    initialize_state_machines(replication_factor).get();
    wait_for_leader(10s).get();

    auto data = make_gaps(
      generate_data(kafka::offset{0}, 1000, random_generators::get_int(1, 10)));

    std::vector<size_t> indicies;
    indicies.reserve(data.size());
    for (size_t i = 0; i < data.size(); ++i) {
        indicies.push_back(i);
    }

    auto produce_fiber = [&] {
        // randomize the order of batches, finally we should replicate all of
        // them
        return ss::do_until(
          [&] { return indicies.empty(); },
          [&] {
              auto idx = random_generators::random_choice(indicies);
              auto b_cp = copy_batches(data[idx]);
              auto o = model::offset_cast(b_cp.back().last_offset());
              auto expected_offsets = start_offsets(b_cp);
              auto expected_prev_offset
                = idx == 0
                    ? kafka::offset{}
                    : model::offset_cast(data[idx - 1].back().last_offset());
              auto maybe_stm = get_leader_stm();
              if (!maybe_stm) {
                  return ss::sleep(50ms);
              }
              auto stages = (*maybe_stm)
                              ->replicate(
                                std::move(b_cp),
                                std::move(expected_offsets),
                                expected_prev_offset,
                                10s);
              expected_prev_offset = o;

              return stages.request_enqueued
                .then([f = std::move(stages.replicate_finished)]() mutable {
                    return std::move(f);
                })
                .then([&, idx](result<raft::replicate_result> result) {
                    if (!result.has_error()) {
                        std::erase_if(
                          indicies, [idx](size_t i) { return i == idx; });
                    }
                });
          }

        );
    };
    auto stop = false;
    size_t max_transfers = 50;
    auto t_fiber = ss::do_until(
      [&] { return stop || max_transfers <= 0; },
      [&] {
          return wait_for_leader(10s).then([&](model::node_id l_id) {
              return node(l_id).raft()->step_down("test").then([&] {
                  max_transfers--;
                  return ss::sleep(200ms);
              });
          });
      });

    std::vector<ss::future<>> futures;
    futures.reserve(10);
    for (auto i = 0; i < 10; ++i) {
        futures.push_back(produce_fiber());
    }
    ss::when_all(futures.begin(), futures.end()).get();
    stop = true;
    t_fiber.get();
    auto leader_id = wait_for_leader(10s).get();
    auto dirty_offset = node(leader_id).raft()->log()->offsets().dirty_offset;

    ASSERT_EQ(
      node(leader_id).raft()->log()->from_log_offset(dirty_offset),
      data.back().back().last_offset());
    ASSERT_TRUE(logs_content_is_valid().get());
}

INSTANTIATE_TEST_SUITE_P(
  TestConcurrentWrites,
  WriteAtOffsetConcurrentWritesTest,
  testing::Values(1, 3, 5));

TEST_F(WriteAtOffsetStmFixture, test_recovery_from_snapshot) {
    enable_offset_translation();
    initialize_state_machines(3).get();
    auto leader_id = wait_for_leader(10s).get();
    auto stm = get_leader_stm();
    auto leader_raft = node(leader_id).raft();
    kafka::offset gen_offset{0};
    // store the truncation point, it is the base offset of second replicated
    // batch.
    model::offset snapshot_offset{};
    for (int i = 0; i < 200; ++i) {
        auto res = generate_data_with_deltas(
          gen_offset,
          random_generators::get_int(1, 10),
          random_generators::get_int(1, 10));
        auto [data, last_offset] = std::move(res);
        gen_offset = last_offset;
        for (auto& batches : data) {
            auto result = leader_raft
                            ->replicate(
                              std::move(batches),
                              raft::replicate_options(
                                raft::consistency_level::quorum_ack))
                            .get();

            ASSERT_FALSE(result.has_error());
        }
        if (snapshot_offset == model::offset{}) {
            snapshot_offset = node(leader_id).raft()->dirty_offset();
        }
    }

    ASSERT_TRUE(logs_content_is_valid().get());
    leader_id = wait_for_leader(10s).get();
    auto& leader = node(leader_id);
    auto dirty_offset = leader.raft()->dirty_offset();

    auto to_restart = random_follower_id();
    for (auto& [id, node] : nodes()) {
        // do not explicitly write snapshot as node selected for recovery
        if (id == *to_restart) {
            continue;
        }
        node->raft()
          ->write_snapshot(raft::write_snapshot_cfg(snapshot_offset, iobuf{}))
          .get();
    }

    restart_node_and_delete_data(*to_restart).get();
    auto& follower = node(*to_restart);
    // wait for recovery
    wait_for_committed_offset(dirty_offset, 10s).get();
    ASSERT_THAT(
      follower.raft()->log()->offsets().start_offset,
      Eq(model::next_offset(snapshot_offset)));

    auto leader_stm = get_leader_stm();
    ASSERT_TRUE(leader_stm.has_value());
    auto leader_last_offset
      = leader_stm.value()->get_expected_last_offset(10s).get();
    ASSERT_FALSE(leader_last_offset.has_error());
    for (auto& [id, node] : nodes()) {
        auto stm
          = node->raft()->stm_manager()->get<kafka::write_at_offset_stm>();
        stm->wait(node->raft()->committed_offset(), model::no_timeout).get();
        auto last_offset
          = kafka::write_at_offset_stm_accessor::get_expected_last_offset(*stm);
        ASSERT_EQ(last_offset, leader_last_offset.value()) << fmt::format(
          "Node {} has last offset {} but leader has {}",
          id,
          last_offset,
          leader_last_offset.value());
    }
    ASSERT_TRUE(logs_content_is_valid().get());
}

TEST_F(WriteAtOffsetStmFixture, test_failed_replication) {
    enable_offset_translation();
    set_enable_longest_log_detection(false);
    initialize_state_machines(3).get();
    wait_for_leader(10s).get();
    auto stm = get_leader_stm();
    kafka::offset gen_offset{0};
    // replicate some batches
    for (int i = 0; i < 10; ++i) {
        auto data = generate_data(
          gen_offset,
          random_generators::get_int(1, 10),
          random_generators::get_int(1, 10));
        gen_offset = model::offset_cast(
          model::next_offset(data.back().back().last_offset()));
        for (auto& batches : data) {
            auto expected_offsets = start_offsets(batches);
            auto stages = stm->get()->replicate(
              std::move(batches),
              std::move(expected_offsets),
              std::nullopt,
              10s);
            stages.request_enqueued.get();
            auto result = stages.replicate_finished.get();
            ASSERT_FALSE(result.has_error());
        }
    }
    auto leader_id = wait_for_leader(10s).get();
    auto& leader = node(leader_id);
    auto dirty_offset = leader.raft()->dirty_offset();
    wait_for_committed_offset(dirty_offset, 10s).get();
    // fail all the append entries requests
    leader.on_dispatch([](model::node_id, raft::msg_type t) {
        if (t == raft::msg_type::append_entries) {
            throw std::runtime_error("error");
        }
        return ss::now();
    });
    stm = get_leader_stm();
    auto err_expected_offset = gen_offset;
    auto data = generate_data(
      err_expected_offset, 10, random_generators::get_int(1, 10));

    std::vector<ss::future<result<raft::replicate_result>>> results;
    for (auto& batches : data) {
        auto expected_offsets = start_offsets(batches);
        auto stages = stm->get()->replicate(
          std::move(batches), std::move(expected_offsets), std::nullopt, 10s);
        stages.request_enqueued.get();
        results.push_back(std::move(stages.replicate_finished));
    }

    leader.raft()->block_new_leadership();
    leader.raft()->step_down("test").get();
    auto new_leader_id = wait_for_leader(10s).get();
    ASSERT_NE(leader_id, new_leader_id);
    auto ready_results
      = ss::when_all_succeed(results.begin(), results.end()).get();
    leader.raft()->unblock_new_leadership();
    leader.reset_dispatch_handlers();
    // transfer leadership back to the original leader to check the state
    // machine state after leadership change
    node(new_leader_id)
      .raft()
      ->transfer_leadership(
        raft::transfer_leadership_request{
          .group = leader.raft()->group(), .target = leader_id, .timeout = 10s})
      .get();
    // Go back to batches with previous expected offsets
    auto new_data = generate_data(
      gen_offset, 10, random_generators::get_int(1, 10));
    leader_id = wait_for_leader(10s).get();
    stm = get_leader_stm();

    for (auto& batches : new_data) {
        auto expected_offsets = start_offsets(batches);
        auto stages = stm->get()->replicate(
          std::move(batches), std::move(expected_offsets), std::nullopt, 10s);
        stages.request_enqueued.get();
        auto res = stages.replicate_finished.get();
        ASSERT_FALSE(res.has_error());
    }
    ASSERT_TRUE(logs_content_is_valid().get());
}

TEST_F(WriteAtOffsetStmFixture, test_ensure_truncatable) {
    enable_offset_translation();
    initialize_state_machines(3).get();
    wait_for_leader(10s).get();
    auto stm = get_leader_stm();

    // negative offset should return invalid_truncation_offset error
    auto result = (*stm)->ensure_truncatable(kafka::offset{-1}, 10s).get();
    ASSERT_EQ(
      result, kafka::write_at_offset_stm::errc::invalid_truncation_offset);

    // on empty log, ensure_truncatable should fill the gap from 0 to
    // new_start_offset - 1
    auto new_start_offset = kafka::offset{100};
    result = (*stm)->ensure_truncatable(new_start_offset, 10s).get();
    ASSERT_EQ(result, kafka::write_at_offset_stm::errc::success);

    // verify the stm's expected_last_offset is at least new_start_offset - 1
    auto expected_last = (*stm)->get_expected_last_offset(10s).get();
    ASSERT_FALSE(expected_last.has_error());
    ASSERT_GE(expected_last.value(), kafka::prev_offset(new_start_offset));

    // replicate some batches
    auto data = generate_data(new_start_offset, 10, 5);
    kafka::offset last_offset{};
    for (auto& batches : data) {
        last_offset = model::offset_cast(batches.back().last_offset());
        auto expected_offsets = start_offsets(batches);
        auto stages = (*stm)->replicate(
          std::move(batches), std::move(expected_offsets), std::nullopt, 10s);
        stages.request_enqueued.get();
        auto replicate_result = stages.replicate_finished.get();
        ASSERT_FALSE(replicate_result.has_error());
    }

    // ensure_truncatable with offset <= last_offset should succeed immediately
    result = (*stm)->ensure_truncatable(last_offset, 10s).get();
    ASSERT_EQ(result, kafka::write_at_offset_stm::errc::success);

    // ensure_truncatable with a higher offset should fill the gap
    new_start_offset = kafka::offset{last_offset() + 100};
    result = (*stm)->ensure_truncatable(new_start_offset, 10s).get();
    ASSERT_EQ(result, kafka::write_at_offset_stm::errc::success);

    // verify the stm's expected_last_offset is at least new_start_offset - 1
    expected_last = (*stm)->get_expected_last_offset(10s).get();
    ASSERT_FALSE(expected_last.has_error());
    ASSERT_GE(expected_last.value(), kafka::prev_offset(new_start_offset));
}

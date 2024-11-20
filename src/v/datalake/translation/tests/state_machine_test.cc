/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#include "datalake/translation/state_machine.h"
#include "raft/tests/stm_test_fixture.h"
#include "storage/disk_log_impl.h"
#include "test_utils/scoped_config.h"

using stm = datalake::translation::translation_stm;
using stm_ptr = ss::shared_ptr<datalake::translation::translation_stm>;

struct translator_stm_fixture : stm_raft_fixture<stm> {
    stm_shptrs_t create_stms(
      state_machine_manager_builder& builder, raft_node_instance& node) {
        return builder.create_stm<stm>(logger(), node.raft().get());
    }

    ss::future<> update_iceberg_config(model::iceberg_mode mode) {
        co_await parallel_for_each_node([mode](raft_node_instance& node) {
            auto log = node.raft()->log();
            log->set_overrides(
              storage::ntp_config::default_overrides{.iceberg_mode = mode});
            return ss::make_ready_future<>();
        });
    }

    ss::future<> enable_iceberg() {
        return update_iceberg_config(
          model::iceberg_mode::value_schema_id_prefix);
    }

    ss::future<> disable_iceberg() {
        return update_iceberg_config(model::iceberg_mode::disabled);
    }

    template<typename Func>
    ss::future<> for_each_stm(Func&& f) {
        for (auto& [_, stms] : node_stms) {
            co_await ss::futurize_invoke(f, std::get<0>(stms));
        }
    }

    ss::future<std::vector<model::offset>> max_collectible_offsets() {
        std::vector<model::offset> result;
        result.reserve(node_stms.size());
        co_await for_each_stm([&result](stm_ptr stm) {
            result.push_back(stm->max_collectible_offset());
        });
        co_return result;
    }

    ss::future<result<std::optional<kafka::offset>, raft::errc>>
    get_highest_translated_offset() {
        auto leader_id = get_leader();
        if (!leader_id) {
            co_return raft::errc::not_leader;
        }
        auto& stms = node_stms[node(leader_id.value()).get_vnode()];
        co_return co_await std::get<0>(stms)->highest_translated_offset(5s);
    }

    ss::future<std::error_code>
    set_highest_translated_offset(kafka::offset update) {
        auto leader_id = get_leader();
        if (!leader_id) {
            co_return raft::errc::not_leader;
        }
        auto& stms = node_stms[node(leader_id.value()).get_vnode()];
        auto stm = std::get<0>(stms);
        ss::abort_source as;
        co_return co_await stm->reset_highest_translated_offset(
          update, stm->raft()->term(), 5s, as);
    }

    ss::future<> check_max_collectible_offset(model::offset expected) {
        RPTEST_REQUIRE_EVENTUALLY_CORO(10s, [this, expected]() {
            return max_collectible_offsets().then(
              [this, expected](std::vector<model::offset> result) {
                  vlog(
                    logger().info,
                    "max collectible offsets: {}, expected: {}",
                    result,
                    expected);
                  auto equal = std::all_of(
                    result.begin(),
                    result.end(),
                    [expected](model::offset current) {
                        return current == expected;
                    });
                  return ss::make_ready_future<bool>(equal);
              });
        });
    }

    ss::future<>
    check_highest_translated_offset(std::optional<kafka::offset> expected) {
        RPTEST_REQUIRE_EVENTUALLY_CORO(10s, [this, expected] {
            return get_highest_translated_offset().then(
              [expected](
                result<std::optional<kafka::offset>, raft::errc> result) {
                  return ss::make_ready_future<bool>(
                    result.has_value() && result.value() == expected);
              });
        });
    }
};

TEST_F_CORO(translator_stm_fixture, state_machine_ops) {
    co_await initialize_state_machines();
    co_await wait_for_leader(5s);
    scoped_config config;
    config.get("iceberg_enabled").set_value(true);
    // since iceberg is not enabled, ensure max_collectible offset
    // is max()
    co_await check_max_collectible_offset(model::offset::max());
    co_await check_highest_translated_offset(std::nullopt);

    auto new_translated_offset = kafka::offset{10};
    // update highest_translated offset;
    RPTEST_REQUIRE_EVENTUALLY_CORO(10s, [this, new_translated_offset] {
        return set_highest_translated_offset(new_translated_offset)
          .then([](std::error_code ec) { return !bool(ec); });
    });

    // iceberg is still disabled, max_collectible offset shouldn't change.
    co_await check_max_collectible_offset(model::offset::max());
    co_await check_highest_translated_offset(std::nullopt);

    // enable iceberg.
    co_await enable_iceberg();

    model::offset max_collectible_offset{};
    {
        auto log = std::get<0>(node_stms.begin()->second)->raft()->log();
        max_collectible_offset = log->to_log_offset(
          kafka::offset_cast(new_translated_offset));
    }
    co_await check_max_collectible_offset(max_collectible_offset);
    co_await check_highest_translated_offset(new_translated_offset);

    co_await disable_iceberg();

    co_await check_max_collectible_offset(model::offset::max());
    co_await check_highest_translated_offset(std::nullopt);

    // test snapshots
    // write a snapshot.
    co_await for_each_stm(
      [](stm_ptr stm) { return stm->write_local_snapshot(); });
    // restart nodes so the snapshot is applied on startup
    co_await restart_nodes();

    co_await enable_iceberg();
    co_await check_max_collectible_offset(max_collectible_offset);
    co_await check_highest_translated_offset(new_translated_offset);
}

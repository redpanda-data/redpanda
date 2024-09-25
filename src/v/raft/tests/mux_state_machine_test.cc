// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "base/vassert.h"
#include "group_configuration.h"
#include "model/fundamental.h"
#include "model/record.h"
#include "model/record_batch_types.h"
#include "model/timeout_clock.h"
#include "raft/mux_state_machine.h"
#include "raft/tests/raft_fixture_retry_policy.h"
#include "raft/tests/simple_raft_fixture.h"
#include "reflection/adl.h"
#include "state_machine_base.h"
#include "storage/record_batch_builder.h"
#include "storage/tests/utils/disk_log_builder.h"
#include "test_utils/fixture.h"
#include "test_utils/test.h"
#include "tests/raft_fixture.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/future.hh>
#include <seastar/core/loop.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/sleep.hh>
#include <seastar/util/log.hh>

#include <boost/range/irange.hpp>
#include <boost/test/tools/old/interface.hpp>
#include <fmt/format.h>
#include <gtest/gtest.h>

#include <algorithm>
#include <chrono>
#include <exception>
#include <memory>
#include <numeric>
#include <system_error>

using namespace std::chrono_literals;
using namespace raft;

namespace mux_state_machine_test {

ss::logger kvlog{"kv-test"};

struct set_cmd {
    static constexpr uint8_t record_key = 0;
    ss::sstring key;
    int value;
};

struct delete_cmd {
    static constexpr uint8_t record_key = 1;
    ss::sstring key;
};

struct cas_cmd {
    static constexpr uint8_t record_key = 2;
    ss::sstring key;
    int predicate;
    int val;
};

struct timeout_cmd {
    static constexpr uint8_t record_key = 3;
    ss::sstring key;
};

enum class errc {
    success = 0,
    key_already_exists = 1,
    key_not_exist = 2,
    cas_error = 3,
    timeout = 4,
};

struct errc_category final : public std::error_category {
    const char* name() const noexcept final { return "simple_kv::errc"; }

    std::string message(int c) const final {
        switch (static_cast<errc>(c)) {
        case errc::success:
            return "Success";
        case errc::key_already_exists:
            return "Key is already there";
        case errc::key_not_exist:
            return "Key not exists";
        case errc::cas_error:
            return "CAS operation error";
        default:
            return "simple_kv::errc::unknown";
        }
    }
};
inline const std::error_category& error_category() noexcept {
    static errc_category e;
    return e;
}
inline std::error_code make_error_code(errc e) noexcept {
    return std::error_code(static_cast<int>(e), error_category());
}
} // namespace mux_state_machine_test
namespace std {
template<>
struct is_error_code_enum<mux_state_machine_test::errc> : true_type {};
} // namespace std

namespace mux_state_machine_test {
static constexpr int8_t batch_type_1{10};
static constexpr int8_t batch_type_2{11};

template<int8_t bt>
struct simple_kv {
    using map_t = absl::flat_hash_map<ss::sstring, int>;
    map_t kv_map;
    ss::abort_source as;

    ss::future<std::error_code> apply_update(model::record_batch&& b) {
        return do_apply_update(std::move(b));
    }

    ss::future<std::error_code> do_apply_update(model::record_batch&& b) {
        auto r = b.copy_records();
        auto rk = reflection::adl<uint8_t>{}.from(r.begin()->release_key());
        switch (rk) {
        case set_cmd::record_key:
            return apply(
              reflection::adl<set_cmd>{}.from(r.begin()->release_value()));
        case delete_cmd::record_key:
            return apply(
              reflection::adl<delete_cmd>{}.from(r.begin()->release_value()));
        case cas_cmd::record_key:
            return apply(
              reflection::adl<cas_cmd>{}.from(r.begin()->release_value()));
        case timeout_cmd::record_key:
            return apply(
              reflection::adl<timeout_cmd>{}.from(r.begin()->release_value()));
        default:
            throw std::logic_error("Unknown command type");
        }
    }

    ss::future<std::error_code> apply(set_cmd c) {
        if (kv_map.contains(c.key)) {
            return ss::make_ready_future<std::error_code>(
              errc::key_already_exists);
        }
        kv_map.emplace(c.key, c.value);
        return ss::make_ready_future<std::error_code>(errc::success);
    }

    ss::future<std::error_code> apply(delete_cmd c) {
        if (!kv_map.contains(c.key)) {
            return ss::make_ready_future<std::error_code>(errc::key_not_exist);
        }
        kv_map.erase(c.key);
        return ss::make_ready_future<std::error_code>(errc::success);
    }

    ss::future<std::error_code> apply(cas_cmd c) {
        if (auto it = kv_map.find(c.key); it->second == c.predicate) {
            it->second = c.val;
            return ss::make_ready_future<std::error_code>(errc::success);
        }

        return ss::make_ready_future<std::error_code>(errc::cas_error);
    }

    ss::future<std::error_code> apply(timeout_cmd c) {
        return ss::sleep_abortable(5s, as).then(
          [] { return std::error_code(errc::success); });
    }

    bool is_batch_applicable(const model::record_batch& batch) const {
        return batch.header().type == model::record_batch_type(bt);
    }
};

template<int8_t... bt>
struct simple_kv_stm final : public raft::mux_state_machine<simple_kv<bt>...> {
    using base_t = raft::mux_state_machine<simple_kv<bt>...>;

    template<typename... Args>
    simple_kv_stm(Args&&... stm_args)
      : base_t(std::forward<Args>(stm_args)...) {}
    ss::future<std::optional<iobuf>>
    maybe_make_snapshot(ssx::semaphore_units) final {
        iobuf buf;
        std::apply(
          [&](auto&&... states) { (serde::write(buf, states.kv_map), ...); },
          base_t::_state);
        co_return buf;
    }
    ss::future<>
    apply_snapshot(model::offset, storage::snapshot_reader& reader) final {
        const size_t size = co_await reader.get_snapshot_size();
        auto snap_buf_parser = iobuf_parser{
          co_await read_iobuf_exactly(reader.input(), size)};
        auto read_single = [&](auto& state) {
            state.kv_map = serde::read<simple_kv<0>::map_t>(snap_buf_parser);
        };
        std::apply(
          [&](auto&&... states) { (read_single(states), ...); },
          base_t::_state);
    }
};

static absl::flat_hash_set<model::record_batch_type> not_handled_batch_types{
  model::record_batch_type::raft_configuration};

template<typename T>
model::record_batch
serialize_cmd(T t, int8_t type, model::offset o = model::offset(0)) {
    storage::record_batch_builder b(model::record_batch_type(type), o);
    iobuf key_buf;
    reflection::adl<uint8_t>{}.to(key_buf, T::record_key);
    iobuf v_buf;
    reflection::adl<T>{}.to(v_buf, std::forward<T>(t));
    b.add_raw_kv(std::move(key_buf), std::move(v_buf));
    return std::move(b).build();
}

template<class STM>
struct kv_stm_fixture : raft::stm_raft_fixture<STM> {
    static constexpr auto TIMEOUT = 5s;

    using base = raft::stm_raft_fixture<STM>;
    using stm_shptr_t = ss::shared_ptr<STM>;

    ss::future<> start_stms() {
        return base::parallel_for_each_node([this](raft_node_instance& node) {
            return std::get<0>(this->node_stms[node.get_vnode()])->start();
        });
    };

    ss::future<> TearDownAsync() override {
        co_await base::parallel_for_each_node([this](raft_node_instance& node) {
            return std::get<0>(this->node_stms[node.get_vnode()])->stop();
        });
        co_await base::TearDownAsync();
    };

    ss::future<std::error_code> replicate_and_wait(
      model::record_batch b, std::chrono::milliseconds timeout = 1s) {
        return base::template stm_retry_with_leader<0>(
          TIMEOUT, [b = std::move(b), timeout, this](const stm_shptr_t& stm) {
              return stm->replicate_and_wait(
                b.copy(), model::timeout_clock::now() + timeout, as);
          });
    }
    ss::abort_source as;
};

using kv1_stm_t = simple_kv_stm<batch_type_1>;
struct kv1_stm_fixture : kv_stm_fixture<kv1_stm_t> {
    using kv_t = simple_kv<batch_type_1>;
    using map_t = kv_t::map_t;
    std::tuple<ss::shared_ptr<kv1_stm_t>> create_stms(
      raft::state_machine_manager_builder& builder,
      raft_node_instance& node) override {
        auto [it, inserted] = kvs.emplace(
          std::piecewise_construct,
          std::tuple{node.get_vnode()},
          std::tuple{new kv_t()});
        vassert(inserted, "attempted to initialize multiple stms per node");
        return ss::make_shared<kv1_stm_t>(
          kvlog,
          node.raft().get(),
          raft::persistent_last_applied::yes,
          not_handled_batch_types,
          *it->second);
    }

    template<class Func>
    ss::future<bool> with_leaders_kv(Func&& f) {
        return with_leader(
          30s,
          [this, f = std::forward<Func>(f)](raft_node_instance& node) mutable {
              return f(kvs[node.get_vnode()]->kv_map);
          });
    }

    absl::flat_hash_map<raft::vnode, std::unique_ptr<kv_t>> kvs;
};

TEST_F_CORO(kv1_stm_fixture, test_mux_state_machine_simple_scenarios) {
    co_await initialize_state_machines(1);
    co_await start_stms();
    // success set
    vlog(kvlog.info, "Test case: success set");
    auto res = co_await replicate_and_wait(
      serialize_cmd(set_cmd{"test", 10}, batch_type_1));
    vlog(kvlog.info, "long afterwards");
    ASSERT_EQ_CORO(res, errc::success);
    ASSERT_TRUE_CORO(co_await with_leaders_kv(
      [](const map_t& map) { return map.find("test")->second == 10; }));

    // error
    vlog(kvlog.info, "Test case: error set");
    res = co_await replicate_and_wait(
      serialize_cmd(set_cmd{"test", 11}, batch_type_1));

    ASSERT_EQ_CORO(res, errc::key_already_exists);
    ASSERT_TRUE_CORO(co_await with_leaders_kv(
      [](const map_t& map) { return map.find("test")->second == 10; }));

    // success cas
    vlog(kvlog.info, "Test case: success cas");
    res = co_await replicate_and_wait(
      serialize_cmd(cas_cmd{"test", 10, 20}, batch_type_1));
    ASSERT_EQ_CORO(res, errc::success);
    ASSERT_TRUE_CORO(co_await with_leaders_kv(
      [](const map_t& map) { return map.find("test")->second == 20; }));

    // error cas
    vlog(kvlog.info, "Test case: error cas");
    res = co_await replicate_and_wait(
      serialize_cmd(cas_cmd{"test", 11, 20}, batch_type_1));
    ASSERT_EQ_CORO(res, errc::cas_error);
    ASSERT_TRUE_CORO(co_await with_leaders_kv(
      [](const map_t& map) { return map.find("test")->second == 20; }));

    // success delete
    vlog(kvlog.info, "Test case: success delete");
    res = co_await replicate_and_wait(
      serialize_cmd(delete_cmd{"test"}, batch_type_1));

    ASSERT_EQ_CORO(res, errc::success);
    ASSERT_TRUE_CORO(
      co_await with_leaders_kv([](const map_t& map) { return map.empty(); }));
}

TEST_F_CORO(kv1_stm_fixture, test_concurrent_sets) {
    co_await initialize_state_machines(1);
    co_await start_stms();
    auto range = boost::irange(0, 50);

    std::vector<ss::future<std::error_code>> futures;
    futures.reserve(range.size());
    std::transform(
      range.begin(), range.end(), std::back_inserter(futures), [this](int i) {
          return ss::sleep(
                   std::chrono::milliseconds(random_generators::get_int(10)))
            .then([this, i] {
                return replicate_and_wait(
                  serialize_cmd(set_cmd{"test", i}, batch_type_1));
            });
      });

    auto results = co_await ss::when_all_succeed(
      futures.begin(), futures.end());

    auto success_count = 0;
    for (int i = 0; i < results.size(); ++i) {
        if (results[i] == errc::success) {
            vlog(kvlog.info, "Applied value: {}", i);
            ++success_count;
            ASSERT_TRUE_CORO(co_await with_leaders_kv(
              [i](const map_t& map) { return map.find("test")->second == i; }));
        }
    }
    ASSERT_EQ_CORO(success_count, 1);
}

TEST_F_CORO(raft_fixture, test_stm_recovery) {
    add_node(model::node_id(1), model::revision_id(0));
    raft_node_instance& node = *(nodes().begin()->second);
    {
        auto ntp = node.ntp();
        auto cfg = storage::log_builder_config();
        cfg.base_dir = node.base_directory();
        storage::disk_log_builder builder(cfg);
        model::offset offset(0);
        std::vector<model::record_batch> batches;

        co_await builder.start(ntp);
        co_await builder.add_segment(model::offset(0));
        co_await builder.add_batch(serialize_cmd(
          set_cmd{"test", 1}, batch_type_1, offset++)); // -> test = 1
        co_await builder.add_batch(serialize_cmd(
          set_cmd{"test", 2}, batch_type_1, offset++)); // -> failed
        co_await builder.add_batch(serialize_cmd(
          set_cmd{"test", 3}, batch_type_1, offset++)); // -> failed
        co_await builder.add_batch(serialize_cmd(
          cas_cmd{"test", 1, 10}, batch_type_1, offset++)); // -> test =10
        co_await builder.add_batch(serialize_cmd(
          set_cmd{"test-1", 2}, batch_type_1, offset++)); // -> failed
        co_await builder.add_batch(serialize_cmd(
          set_cmd{"test-2", 15}, batch_type_1, offset++)); // -> test-2 = 15
        co_await builder.add_batch(serialize_cmd(
          cas_cmd{"test-2", 15, 1}, batch_type_1, offset++)); // -> test-2 = 1
        co_await builder.add_batch(serialize_cmd(
          cas_cmd{"test-2", 15, 2}, batch_type_1, offset++)); // -> failed
        co_await builder.add_batch(serialize_cmd(
          delete_cmd{"test-1"}, batch_type_1, offset++)); // -> test-1 deleted
        co_await builder.stop();
    }
    co_await node.initialise(all_vnodes());
    co_await node.start(std::nullopt);
    co_await wait_for_leader(30s);
    auto last_offset = node.raft()->dirty_offset();

    // Correct state:
    // test = 10
    // test-2 = 1
    {
        simple_kv<batch_type_1> state;
        simple_kv_stm<batch_type_1> stm(
          kvlog,
          node.raft().get(),
          raft::persistent_last_applied::yes,
          not_handled_batch_types,
          state);
        std::exception_ptr ex = nullptr;
        try {
            co_await stm.start();

            co_await stm.wait(last_offset, model::timeout_clock::now() + 1s);
            ASSERT_EQ_CORO(stm.get_last_applied_offset(), last_offset);
            ASSERT_EQ_CORO(state.kv_map.size(), 2);
            ASSERT_EQ_CORO(state.kv_map.find("test")->second, 10);
            ASSERT_EQ_CORO(state.kv_map.contains("test-1"), false);
            ASSERT_EQ_CORO(state.kv_map.contains("test-2"), 1);

            // create a snapshot
            ASSERT_TRUE_CORO(co_await stm.maybe_write_snapshot());
            ASSERT_EQ_CORO(
              node.raft()->start_offset(),
              model::next_offset(stm.get_last_applied_offset()));
        } catch (...) {
            ex = std::current_exception();
        }
        co_await stm.stop();
        if (ex) {
            std::rethrow_exception(ex);
        }
    }

    {
        // test recovering from the snapshot
        simple_kv<batch_type_1> state;
        simple_kv_stm<batch_type_1> stm(
          kvlog,
          node.raft().get(),
          raft::persistent_last_applied::yes,
          not_handled_batch_types,
          state);
        std::exception_ptr ex = nullptr;
        try {
            co_await stm.start();

            co_await stm.wait(last_offset, model::timeout_clock::now() + 1s);
            ASSERT_EQ_CORO(state.kv_map.size(), 2);
            ASSERT_EQ_CORO(state.kv_map.find("test")->second, 10);
            ASSERT_EQ_CORO(state.kv_map.contains("test-1"), false);
            ASSERT_EQ_CORO(state.kv_map.contains("test-2"), 1);
        } catch (...) {
            ex = std::current_exception();
        }
        co_await stm.stop();
        if (ex) {
            std::rethrow_exception(ex);
        }
    }
}

using kv2_stm_t = simple_kv_stm<batch_type_1, batch_type_2>;
struct kv2_stm_fixture : kv_stm_fixture<kv2_stm_t> {
    using kv1_t = simple_kv<batch_type_1>;
    using kv2_t = simple_kv<batch_type_2>;
    using kv_t = std::tuple<kv1_t, kv2_t>;
    using map1_t = kv1_t::map_t;
    using map2_t = kv2_t::map_t;
    std::tuple<ss::shared_ptr<kv2_stm_t>> create_stms(
      raft::state_machine_manager_builder& builder,
      raft_node_instance& node) override {
        auto [it, inserted] = kvs.emplace(
          std::piecewise_construct,
          std::tuple{node.get_vnode()},
          std::tuple{new kv_t()});
        vassert(inserted, "attempted to initialize multiple stms per node");
        return ss::make_shared<kv2_stm_t>(
          kvlog,
          node.raft().get(),
          raft::persistent_last_applied::yes,
          not_handled_batch_types,
          std::get<0>(*it->second),
          std::get<1>(*it->second));
    }

    template<class Func>
    ss::future<bool> with_leaders_kv(Func&& f) {
        return with_leader(
          30s,
          [this, f = std::forward<Func>(f)](raft_node_instance& node) mutable {
              kv_t& pair = *kvs[node.get_vnode()];
              return f(std::get<0>(pair).kv_map, std::get<1>(pair).kv_map);
          });
    }
    absl::flat_hash_map<raft::vnode, std::unique_ptr<kv_t>> kvs;
};

TEST_F_CORO(kv2_stm_fixture, test_mulitple_states) {
    co_await initialize_state_machines(3);
    co_await start_stms();

    // set in state 1
    auto res = co_await replicate_and_wait(
      serialize_cmd(set_cmd{"test", 10}, batch_type_1));
    ASSERT_EQ_CORO(res, errc::success);
    ASSERT_TRUE_CORO(
      co_await with_leaders_kv([](const map1_t& map1, const map2_t& map2) {
          return map1.find("test")->second == 10 && map2.empty();
      }));

    // set in state 2
    res = co_await replicate_and_wait(
      serialize_cmd(set_cmd{"test", 11}, batch_type_2));

    ASSERT_EQ_CORO(res, errc::success);
    ASSERT_TRUE_CORO(
      co_await with_leaders_kv([](const map1_t& map1, const map2_t& map2) {
          return map1.find("test")->second == 10
                 && map2.find("test")->second == 11;
      }));

    // cas in state 2
    res = co_await replicate_and_wait(
      serialize_cmd(cas_cmd{"test", 11, 20}, batch_type_2));
    ASSERT_EQ_CORO(res, errc::success);
    ASSERT_TRUE_CORO(
      co_await with_leaders_kv([](const map1_t& map1, const map2_t& map2) {
          return map1.find("test")->second == 10
                 && map2.find("test")->second == 20;
      }));

    // failed delete in state 1
    res = co_await replicate_and_wait(
      serialize_cmd(delete_cmd{"other"}, batch_type_1));
    ASSERT_EQ_CORO(res, errc::key_not_exist);
    ASSERT_TRUE_CORO(
      co_await with_leaders_kv([](const map1_t& map1, const map2_t& map2) {
          return map1.find("test")->second == 10
                 && map2.find("test")->second == 20;
      }));

    // success delete in state 1
    vlog(kvlog.info, "Test case: success delete");
    res = co_await replicate_and_wait(
      serialize_cmd(delete_cmd{"test"}, batch_type_1));
    ASSERT_EQ_CORO(res, errc::success);
    ASSERT_TRUE_CORO(
      co_await with_leaders_kv([](const map1_t& map1, const map2_t& map2) {
          return map1.empty() && map2.find("test")->second == 20;
      }));
}

TEST_F_CORO(kv1_stm_fixture, timeout_test) {
    co_await initialize_state_machines(1);
    co_await start_stms();
    auto res = co_await replicate_and_wait(
      serialize_cmd(timeout_cmd{"test"}, batch_type_1), 40ms);
    ASSERT_EQ_CORO(res, raft::errc::timeout);
    vlog(kvlog.info, "1234");
    co_await with_leader(10s, [this](raft_node_instance& node) {
        vlog(kvlog.info, "leader={}", node.get_vnode());
        vlog(kvlog.info, "calling as {}", (void*)(&kvs[node.get_vnode()]));
        kvs[node.get_vnode()]->as.request_abort();
    });
}
} // namespace mux_state_machine_test

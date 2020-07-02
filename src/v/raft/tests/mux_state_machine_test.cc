#include "model/fundamental.h"
#include "model/record.h"
#include "model/timeout_clock.h"
#include "outcome.h"
#include "raft/tests/mux_state_machine_fixture.h"
#include "raft/types.h"
#include "random/generators.h"
#include "reflection/adl.h"
#include "storage/record_batch_builder.h"
#include "storage/tests/utils/disk_log_builder.h"
#include "storage/tests/utils/random_batch.h"
#include "test_utils/async.h"
#include "test_utils/fixture.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/future.hh>
#include <seastar/core/sleep.hh>
#include <seastar/util/defer.hh>
#include <seastar/util/log.hh>

#include <boost/range/irange.hpp>
#include <boost/test/tools/old/interface.hpp>

#include <thread>

using namespace std::chrono_literals;

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

namespace std {
template<>
struct is_error_code_enum<::errc> : true_type {};
} // namespace std

static constexpr int8_t batch_type_1{10};
static constexpr int8_t batch_type_2{11};

template<int8_t bt>
struct simple_kv {
    absl::flat_hash_map<ss::sstring, int> kv_map;
    ss::abort_source as;

    ss::future<std::error_code> apply_update(model::record_batch&& b) {
        return do_apply_update(std::move(b));
    }

    ss::future<std::error_code> do_apply_update(model::record_batch&& b) {
        auto rk = reflection::adl<uint8_t>{}.from(b.begin()->release_key());
        switch (rk) {
        case set_cmd::record_key:
            return apply(
              reflection::adl<set_cmd>{}.from(b.begin()->release_value()));
        case delete_cmd::record_key:
            return apply(
              reflection::adl<delete_cmd>{}.from(b.begin()->release_value()));
        case cas_cmd::record_key:
            return apply(
              reflection::adl<cas_cmd>{}.from(b.begin()->release_value()));
        case timeout_cmd::record_key:
            return apply(
              reflection::adl<timeout_cmd>{}.from(b.begin()->release_value()));
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

ss::logger kvlog{"kv-test"};

template<typename T>
model::record_batch serialize_cmd(T t, int8_t type) {
    storage::record_batch_builder b(
      model::record_batch_type(type), model::offset(0));
    iobuf key_buf;
    reflection::adl<uint8_t>{}.to(key_buf, T::record_key);
    iobuf v_buf;
    reflection::adl<T>{}.to(v_buf, std::forward<T>(t));
    b.add_raw_kv(std::move(key_buf), std::move(v_buf));
    return std::move(b).build();
}

FIXTURE_TEST(
  test_mux_state_machine_simple_scenarios, mux_state_machine_fixture) {
    start_raft();
    simple_kv<batch_type_1> state;
    raft::mux_state_machine stm(kvlog, _raft.get(), state);
    stm.start().get0();
    auto stop = ss::defer([&stm] { stm.stop().get0(); });
    wait_for_leader();
    ss::abort_source as;

    // success set
    info("Test case: success set");
    auto res = stm
                 .replicate_and_wait(
                   serialize_cmd(set_cmd{"test", 10}, batch_type_1),
                   model::timeout_clock::now() + 2s,
                   as)
                 .get0();

    BOOST_REQUIRE_EQUAL(res, errc::success);
    BOOST_CHECK(state.kv_map.find("test")->second == 10);

    // error
    info("Test case: error set");
    res = stm
            .replicate_and_wait(
              serialize_cmd(set_cmd{"test", 11}, batch_type_1),
              model::timeout_clock::now() + 2s,
              as)
            .get0();

    BOOST_REQUIRE_EQUAL(res, errc::key_already_exists);
    BOOST_CHECK(state.kv_map.find("test")->second == 10);

    // success cas
    info("Test case: success cas");
    res = stm
            .replicate_and_wait(
              serialize_cmd(cas_cmd{"test", 10, 20}, batch_type_1),
              model::timeout_clock::now() + 2s,
              as)
            .get0();

    BOOST_REQUIRE_EQUAL(res, errc::success);
    BOOST_CHECK(state.kv_map.find("test")->second == 20);

    // error cas
    info("Test case: error cas");
    res = stm
            .replicate_and_wait(
              serialize_cmd(cas_cmd{"test", 11, 20}, batch_type_1),
              model::timeout_clock::now() + 2s,
              as)
            .get0();

    BOOST_REQUIRE_EQUAL(res, errc::cas_error);
    BOOST_CHECK(state.kv_map.find("test")->second == 20);

    // success delete
    info("Test case: success delete");
    res = stm
            .replicate_and_wait(
              serialize_cmd(delete_cmd{"test"}, batch_type_1),
              model::timeout_clock::now() + 2s,
              as)
            .get0();

    BOOST_REQUIRE_EQUAL(res, errc::success);
    BOOST_CHECK(state.kv_map.empty());
}

FIXTURE_TEST(test_concurrent_sets, mux_state_machine_fixture) {
    start_raft();
    simple_kv<batch_type_1> state;
    raft::mux_state_machine stm(kvlog, _raft.get(), state);
    stm.start().get0();
    wait_for_leader();
    ss::abort_source as;
    auto stop = ss::defer([&stm] { stm.stop().get0(); });
    auto range = boost::irange(0, 50);
    std::vector<ss::future<std::error_code>> futures;
    futures.reserve(range.size());
    std::transform(
      range.begin(),
      range.end(),
      std::back_inserter(futures),
      [&stm, &as](int i) {
          return ss::sleep(
                   std::chrono::milliseconds(random_generators::get_int(10)))
            .then([&stm, &as, i] {
                return stm.replicate_and_wait(
                  serialize_cmd(set_cmd{"test", i}, batch_type_1),
                  model::timeout_clock::now() + 2s,
                  as);
            });
      });

    auto results = ss::when_all_succeed(futures.begin(), futures.end()).get0();

    auto success_count = 0;
    for (int i = 0; i < results.size(); ++i) {
        if (results[i] == errc::success) {
            info("Applied value: {}", i);
            ++success_count;
            BOOST_REQUIRE_EQUAL(state.kv_map.find("test")->second, i);
        }
    }

    BOOST_REQUIRE_EQUAL(success_count, 1);
}

FIXTURE_TEST(test_stm_recovery, mux_state_machine_fixture) {
    {
        auto cfg = storage::log_builder_config();
        cfg.base_dir = _data_dir;
        storage::disk_log_builder builder(cfg);

        builder | storage::start(_ntp) | storage::add_segment(0)
          | storage::add_batch(
            serialize_cmd(set_cmd{"test", 1}, batch_type_1)) // -> test = 1
          | storage::add_batch(
            serialize_cmd(set_cmd{"test", 2}, batch_type_1)) // -> failed
          | storage::add_batch(
            serialize_cmd(set_cmd{"test", 3}, batch_type_1)) // -> failed
          | storage::add_batch(
            serialize_cmd(cas_cmd{"test", 1, 10}, batch_type_1)) // -> test =10
          | storage::add_batch(
            serialize_cmd(set_cmd{"test-1", 2}, batch_type_1)) // -> failed
          | storage::add_batch(serialize_cmd(
            set_cmd{"test-2", 15}, batch_type_1)) // -> test-2 = 15
          | storage::add_batch(serialize_cmd(
            cas_cmd{"test-2", 15, 1}, batch_type_1)) // -> test-2 = 1
          | storage::add_batch(
            serialize_cmd(cas_cmd{"test-2", 15, 2}, batch_type_1)) // -> failed
          | storage::add_batch(serialize_cmd(
            delete_cmd{"test-1"}, batch_type_1)) // -> test-1 deleted
          | storage::stop();
    }
    // Correct state:
    // test = 10
    // test-2 = 1
    start_raft();
    simple_kv<batch_type_1> state;
    raft::mux_state_machine stm(kvlog, _raft.get(), state);
    stm.start().get0();
    auto stop = ss::defer([&stm] { stm.stop().get0(); });
    wait_for_leader();
    auto offset = _storage.local().log_mgr().get(_ntp)->offsets().dirty_offset;
    stm.wait(offset, model::timeout_clock::now() + 1s).get0();
    BOOST_REQUIRE_EQUAL(state.kv_map.size(), 2);
    BOOST_REQUIRE_EQUAL(state.kv_map.find("test")->second, 10);
    BOOST_REQUIRE_EQUAL(state.kv_map.contains("test-1"), false);
    BOOST_REQUIRE_EQUAL(state.kv_map.contains("test-2"), 1);
}

FIXTURE_TEST(test_mulitple_states, mux_state_machine_fixture) {
    start_raft();
    simple_kv<batch_type_1> state_1;
    simple_kv<batch_type_2> state_2;
    raft::mux_state_machine stm(kvlog, _raft.get(), state_1, state_2);
    stm.start().get0();
    auto stop = ss::defer([&stm] { stm.stop().get0(); });
    wait_for_leader();
    ss::abort_source as;

    // set in state 1
    auto res = stm
                 .replicate_and_wait(
                   serialize_cmd(set_cmd{"test", 10}, batch_type_1),
                   model::timeout_clock::now() + 2s,
                   as)
                 .get0();

    BOOST_REQUIRE_EQUAL(res, errc::success);
    BOOST_REQUIRE_EQUAL(state_1.kv_map.find("test")->second, 10);
    BOOST_CHECK(state_2.kv_map.empty());

    // set in state 2
    res = stm
            .replicate_and_wait(
              serialize_cmd(set_cmd{"test", 11}, batch_type_2),
              model::timeout_clock::now() + 2s,
              as)
            .get0();

    BOOST_REQUIRE_EQUAL(res, errc::success);
    BOOST_REQUIRE_EQUAL(state_1.kv_map.find("test")->second, 10);
    BOOST_REQUIRE_EQUAL(state_2.kv_map.find("test")->second, 11);

    // cas in state 2
    res = stm
            .replicate_and_wait(
              serialize_cmd(cas_cmd{"test", 11, 20}, batch_type_2),
              model::timeout_clock::now() + 2s,
              as)
            .get0();

    BOOST_REQUIRE_EQUAL(res, errc::success);
    BOOST_REQUIRE_EQUAL(state_1.kv_map.find("test")->second, 10);
    BOOST_REQUIRE_EQUAL(state_2.kv_map.find("test")->second, 20);

    // failed delete in state 1
    res = stm
            .replicate_and_wait(
              serialize_cmd(delete_cmd{"other"}, batch_type_1),
              model::timeout_clock::now() + 2s,
              as)
            .get0();

    BOOST_REQUIRE_EQUAL(res, errc::key_not_exist);
    BOOST_REQUIRE_EQUAL(state_1.kv_map.find("test")->second, 10);
    BOOST_REQUIRE_EQUAL(state_2.kv_map.find("test")->second, 20);

    // success delete in state 1
    info("Test case: success delete");
    res = stm
            .replicate_and_wait(
              serialize_cmd(delete_cmd{"test"}, batch_type_1),
              model::timeout_clock::now() + 2s,
              as)
            .get0();

    BOOST_REQUIRE_EQUAL(res, errc::success);
    BOOST_REQUIRE_EQUAL(state_1.kv_map.empty(), true);
    BOOST_REQUIRE_EQUAL(state_2.kv_map.find("test")->second, 20);
}

FIXTURE_TEST(timeout_test, mux_state_machine_fixture) {
    start_raft();
    simple_kv<batch_type_1> state_1;
    raft::mux_state_machine stm(kvlog, _raft.get(), state_1);
    stm.start().get0();
    auto stop = ss::defer([&stm] { stm.stop().get0(); });
    wait_for_leader();
    ss::abort_source as;

    // timeout
    auto res = stm
                 .replicate_and_wait(
                   serialize_cmd(timeout_cmd{"test"}, batch_type_1),
                   model::timeout_clock::now() + 40ms,
                   as)
                 .get0();
    state_1.as.request_abort();
    BOOST_REQUIRE_EQUAL(res, raft::errc::timeout);
}
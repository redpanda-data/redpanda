/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "gmock/gmock.h"
#include "model/fundamental.h"
#include "model/tests/random_batch.h"
#include "model/tests/randoms.h"
#include "model/transform.h"
#include "random/generators.h"
#include "wasm/cache.h"
#include "wasm/engine.h"
#include "wasm/wasi_logger.h"

#include <seastar/core/lowres_clock.hh>
#include <seastar/core/sharded.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/thread.hh>
#include <seastar/core/when_all.hh>

#include <gtest/gtest.h>

#include <cstdlib>
#include <memory>
#include <stdexcept>
#include <type_traits>
#include <utility>
#include <vector>

namespace wasm {

namespace {
// NOLINTNEXTLINE(*-avoid-non-const-global-variables,cert-err58-cpp)
static ss::logger test_logger("wasm_cache_test_logger");

struct state {
    std::atomic_int factories = 0;
    std::atomic_int engines = 0;
    std::atomic_int running_engines = 0;
    std::atomic_int engine_restarts = 0;

    std::atomic_bool engine_transform_should_throw = false;
};

class fake_logger : public logger {
public:
    fake_logger() = default;
    void log(ss::log_level, std::string_view) noexcept override {}
};

class fake_engine : public engine {
public:
    explicit fake_engine(state* state)
      : _state(state) {
        ++_state->engines;
    }
    fake_engine(const fake_engine&) = delete;
    fake_engine(fake_engine&&) = delete;
    fake_engine& operator=(const fake_engine&) = delete;
    fake_engine& operator=(fake_engine&&) = delete;
    ~fake_engine() override { --_state->engines; }

    ss::future<> start() override {
        ++_state->running_engines;
        if (_has_been_stopped) {
            ++_state->engine_restarts;
        }
        co_return;
    }
    ss::future<> stop() override {
        --_state->running_engines;
        _has_been_stopped = true;
        co_return;
    }

    ss::future<> transform(
      model::record_batch, transform_probe*, transform_callback) override {
        if (_state->engine_transform_should_throw) {
            throw std::runtime_error("test error");
        }
        co_return;
    }

private:
    bool _has_been_stopped = false;
    state* _state;
};

class fake_factory : public factory {
public:
    explicit fake_factory(state* state)
      : _state(state) {
        ++_state->factories;
    }
    fake_factory(const fake_factory&) = delete;
    fake_factory(fake_factory&&) = delete;
    fake_factory& operator=(const fake_factory&) = delete;
    fake_factory& operator=(fake_factory&&) = delete;
    ~fake_factory() noexcept override { --_state->factories; }

    ss::future<ss::shared_ptr<engine>>
    make_engine(std::unique_ptr<wasm::logger>) override {
        co_return ss::make_shared<fake_engine>(_state);
    }

private:
    state* _state;
};

class fake_runtime : public runtime {
public:
    ss::future<> start(runtime::config) override { co_return; }
    ss::future<> stop() override { co_return; }

    ss::future<ss::shared_ptr<factory>>
    make_factory(model::transform_metadata, model::wasm_binary_iobuf) override {
        co_return ss::make_shared<fake_factory>(&_state);
    }

    state* get_state() { return &_state; }

    ss::future<> validate(model::wasm_binary_iobuf) override { co_return; }

private:
    state _state;
};

} // namespace

class WasmCacheTest : public ::testing::Test {
public:
    static void SetUpTestSuite() {
        vassert(ss::smp::count > 1, "This test expects multiple shards");
    }

    void SetUp() override {
        auto fr = std::make_unique<fake_runtime>();
        _fake_runtime = fr.get();
        // Effectively disable the gc interval
        _caching_runtime = std::make_unique<caching_runtime>(
          std::move(fr), /*gc_interval=*/std::chrono::hours(1));
        _caching_runtime->start({}).get();
    }

    void TearDown() override {
        _caching_runtime->stop().get();
        _fake_runtime = nullptr;
        _caching_runtime = nullptr;
    }

    model::transform_metadata random_metadata() {
        _offset = model::next_offset(_offset);
        return {
          .name = tests::random_named_string<model::transform_name>(),
          .input_topic = model::random_topic_namespace(),
          .output_topics = {model::random_topic_namespace()},
          .environment = {},
          .source_ptr = _offset,
        };
    }

    ss::shared_ptr<factory> make_factory(model::transform_metadata metadata) {
        return make_factory_async(std::move(metadata)).get();
    }
    ss::future<ss::shared_ptr<factory>>
    make_factory_async(model::transform_metadata metadata) {
        return _caching_runtime->make_factory(
          std::move(metadata),
          model::wasm_binary_iobuf(
            std::make_unique<iobuf>(_wasm_module.copy())));
    }

    template<typename Func>
    void invoke_on_all(Func&& func) {
        ss::smp::invoke_on_all([func = std::forward<Func>(func)]() mutable {
            return ss::async(std::forward<Func>(func));
        }).get();
    }

    int64_t gc() { return _caching_runtime->do_gc().get(); }
    auto* state() { return _fake_runtime->get_state(); }

    model::record_batch random_batch() const {
        return model::test::make_random_batch(model::test::record_batch_spec{});
    }

private:
    iobuf _wasm_module = random_generators::make_iobuf();
    model::offset _offset = model::offset(0);
    fake_runtime* _fake_runtime;
    std::unique_ptr<caching_runtime> _caching_runtime;
};

void PrintTo(const ss::shared_ptr<factory>& f, std::ostream* os) {
    *os << "factory{" << f.get() << "}";
}

void PrintTo(const ss::shared_ptr<engine>& f, std::ostream* os) {
    *os << "engine{" << f.get() << "}";
}

TEST_F(WasmCacheTest, CachesFactories) {
    auto meta = random_metadata();
    auto factory_one = make_factory_async(meta);
    auto factory_two = make_factory_async(meta);
    EXPECT_EQ(factory_one.get(), factory_two.get());
    EXPECT_EQ(state()->factories, 1);
}

TEST_F(WasmCacheTest, CachesEngines) {
    auto meta = random_metadata();
    auto factory = ss::make_foreign(make_factory(meta));
    static thread_local ss::shared_ptr<engine> live_engine;
    invoke_on_all([&factory] {
        auto engine_one = factory->make_engine(std::make_unique<fake_logger>());
        auto engine_two = factory->make_engine(std::make_unique<fake_logger>());
        auto engine = engine_one.get();
        EXPECT_EQ(engine, engine_two.get());
        live_engine = engine;
    });
    EXPECT_EQ(state()->engines, ss::smp::count);

    // This engine doesn't actually create new instances under the hood.
    auto engine = factory->make_engine(std::make_unique<fake_logger>()).get();
    EXPECT_EQ(state()->engines, ss::smp::count);
    engine = nullptr;
    EXPECT_EQ(state()->engines, ss::smp::count);

    invoke_on_all([] { live_engine = nullptr; });
    EXPECT_EQ(state()->engines, 0);
}

TEST_F(WasmCacheTest, CanMultiplexEngines) {
    auto meta = random_metadata();
    auto factory = ss::make_foreign(make_factory(meta));
    auto engine_one
      = factory->make_engine(std::make_unique<fake_logger>()).get();
    auto engine_two
      = factory->make_engine(std::make_unique<fake_logger>()).get();
    auto engine_three
      = factory->make_engine(std::make_unique<fake_logger>()).get();
    EXPECT_EQ(state()->engines, 1);
    ss::when_all_succeed(
      [&engine_two] { return engine_two->start(); },
      [&engine_three] { return engine_three->start(); })
      .get();
    EXPECT_EQ(state()->running_engines, 1);
    engine_one->start().get();
    EXPECT_EQ(state()->running_engines, 1);
    engine_one->stop().get();
    EXPECT_EQ(state()->running_engines, 1);
    ss::when_all_succeed(
      [&engine_two] { return engine_two->stop(); },
      [&engine_three] { return engine_three->stop(); })
      .get();
    EXPECT_EQ(state()->running_engines, 0);
}

TEST_F(WasmCacheTest, CanMultiplexTransforms) {
    auto meta = random_metadata();
    auto factory = ss::make_foreign(make_factory(meta));
    auto engine_one
      = factory->make_engine(std::make_unique<fake_logger>()).get();
    auto engine_two
      = factory->make_engine(std::make_unique<fake_logger>()).get();
    engine_one->start().get();
    engine_two->start().get();
    state()->engine_transform_should_throw = true;
    EXPECT_THROW(
      engine_one
        ->transform(
          random_batch(),
          nullptr,
          [](auto, auto) { return ssx::now(write_success::yes); })
        .get(),
      std::runtime_error);
    EXPECT_EQ(state()->engine_restarts, 1);
    state()->engine_transform_should_throw = false;
    EXPECT_NO_THROW(engine_two
                      ->transform(
                        random_batch(),
                        nullptr,
                        [](auto, auto) { return ssx::now(write_success::yes); })
                      .get());
    EXPECT_EQ(state()->engine_restarts, 1);
    engine_one->stop().get();
    engine_two->stop().get();
    EXPECT_EQ(state()->running_engines, 0);
}

TEST_F(WasmCacheTest, GC) {
    auto meta = random_metadata();
    auto factory = ss::make_foreign(make_factory(meta));
    // Create an engine and destroy it
    invoke_on_all([&factory] {
        factory->make_engine(std::make_unique<fake_logger>()).get();
    });
    EXPECT_EQ(state()->engines, 0);
    // We should GC each engine for each core
    EXPECT_EQ(gc(), ss::smp::count);
    factory = nullptr;
    // Now we should GC the factory
    EXPECT_EQ(gc(), 1);
}

TEST_F(WasmCacheTest, FactoryReplacementBeforeGC) {
    auto meta = random_metadata();
    auto factory = make_factory_async(meta).get();
    EXPECT_EQ(state()->factories, 1);
    factory = nullptr;
    EXPECT_EQ(state()->factories, 0);
    // Catches a bug when we were asserting incorrectly because a factory was
    // replaced in the cache instead of being inserted.
    factory = make_factory_async(meta).get();
    EXPECT_EQ(state()->factories, 1);
}

TEST_F(WasmCacheTest, EngineReplacementBeforeGC) {
    auto meta = random_metadata();
    auto factory = ss::make_foreign(make_factory(meta));
    auto engine = factory->make_engine(std::make_unique<fake_logger>()).get();
    EXPECT_EQ(state()->engines, 1);
    engine = nullptr;
    EXPECT_EQ(state()->engines, 0);
    // Catches a bug when we were asserting incorrectly because an engine was
    // replaced in the cache instead of being inserted.
    engine = factory->make_engine(std::make_unique<fake_logger>()).get();
    EXPECT_EQ(state()->engines, 1);
}

} // namespace wasm

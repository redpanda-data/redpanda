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

#include "wasm/cache.h"

#include "wasm/logger.h"

#include <seastar/coroutine/as_future.hh>

namespace wasm {

struct cached_factory {
    ss::shared_ptr<factory> factory;
};

namespace {

/**
 * Allows sharing an engine between multiple uses.
 *
 * Must live on a single core.
 */
class shared_engine : public engine {
public:
    explicit shared_engine(std::unique_ptr<engine> underlying)
      : _underlying(std::move(underlying)) {}

    ss::future<model::record_batch>
    transform(model::record_batch batch, transform_probe* probe) override {
        auto u = co_await _mu.get_units();
        auto fut = co_await ss::coroutine::as_future<model::record_batch>(
          _underlying->transform(std::move(batch), probe));
        if (!fut.failed()) {
            co_return fut.get();
        }
        // Restart the engine
        try {
            co_await _underlying->stop();
            co_await _underlying->start();
        } catch (...) {
            vlog(
              wasm_log.warn,
              "failed to restart wasm engine: {}",
              std::current_exception());
        }
        std::rethrow_exception(fut.get_exception());
    }

    ss::future<> start() override {
        auto u = co_await _mu.get_units();
        if (_ref_count++ == 0) {
            co_await _underlying->start();
        }
    }
    ss::future<> stop() override {
        vassert(
          _ref_count > 0, "expected a call to start before a call to stop");
        auto u = co_await _mu.get_units();
        if (--_ref_count == 0) {
            co_await _underlying->stop();
        }
    }

    uint64_t memory_usage_size_bytes() const override {
        return _underlying->memory_usage_size_bytes();
    }

private:
    mutex _mu;
    size_t _ref_count = 0;
    std::unique_ptr<engine> _underlying;
};

/**
 * A RAII scoped lock that ensures factory locks are deleted when there are no
 * waiters.
 */
class factory_creation_lock_guard {
public:
    factory_creation_lock_guard(const factory_creation_lock_guard&) = delete;
    factory_creation_lock_guard& operator=(const factory_creation_lock_guard&)
      = delete;
    factory_creation_lock_guard(factory_creation_lock_guard&&) noexcept
      = default;
    factory_creation_lock_guard&
    operator=(factory_creation_lock_guard&&) noexcept
      = default;

    static ss::future<factory_creation_lock_guard> acquire(
      absl::btree_map<model::offset, std::unique_ptr<mutex>>* mu_map,
      model::offset offset) {
        auto it = mu_map->find(offset);
        mutex* mu = nullptr;
        if (it == mu_map->end()) {
            auto inserted = mu_map->emplace(offset, std::make_unique<mutex>());
            vassert(inserted.second, "expected mutex to be inserted");
            mu = inserted.first->second.get();
        } else {
            mu = it->second.get();
        }
        mutex::units units = co_await mu->get_units();
        co_return factory_creation_lock_guard(
          offset, mu_map, mu, std::move(units));
    }

    ~factory_creation_lock_guard() {
        _underlying.return_all();
        // If nothing is waiting on or holding the mutex, we can remove the lock
        // from the map.
        if (_mu->ready()) {
            _mu_map->erase(_offset);
        }
    }

private:
    factory_creation_lock_guard(
      model::offset offset,
      absl::btree_map<model::offset, std::unique_ptr<mutex>>* mu_map,
      mutex* mu,
      mutex::units underlying)
      : _offset(offset)
      , _mu_map(mu_map)
      , _mu(mu)
      , _underlying(std::move(underlying)) {}

    model::offset _offset;
    absl::btree_map<model::offset, std::unique_ptr<mutex>>* _mu_map;
    mutex* _mu;
    mutex::units _underlying;
};
} // namespace

caching_runtime::caching_runtime(std::unique_ptr<runtime> u)
  : _underlying(std::move(u)) {}

caching_runtime::~caching_runtime() = default;

ss::future<> caching_runtime::start() {
    co_await _underlying->start();
    // TODO: Start a periodic timer to GC unused factories
}

ss::future<> caching_runtime::stop() { co_await _underlying->stop(); }

ss::future<ss::shared_ptr<factory>> caching_runtime::make_factory(
  model::transform_metadata meta, iobuf binary, ss::logger* logger) {
    model::offset offset = meta.source_ptr;
    // Look in the cache outside the lock
    auto it = _factory_cache.find(offset);
    if (it != _factory_cache.end()) {
        // TODO: Use a factory that caches engines
        co_return it->second->factory;
    }
    auto lock = co_await factory_creation_lock_guard::acquire(
      &_factory_creation_mu_map, offset);
    // Look again in the cache with the lock
    it = _factory_cache.find(offset);
    if (it != _factory_cache.end()) {
        // TODO: Use a factory that caches engines
        co_return it->second->factory;
    }
    // There is no factory and we're holding the lock,
    // time to create a new one.
    auto factory = co_await _underlying->make_factory(
      std::move(meta), std::move(binary), logger);

    // Now cache the factory and return the result.
    auto cached = ss::make_shared<cached_factory>(std::move(factory));
    auto [_, inserted] = _factory_cache.emplace(offset, cached);
    vassert(inserted, "expected factory to be inserted");

    // TODO: Use a factory that caches engines
    co_return cached->factory;
}

} // namespace wasm

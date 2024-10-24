/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "base/seastarx.h"

#include <seastar/core/future.hh>
#include <seastar/core/shard_id.hh>
#include <seastar/core/sharded.hh>

#include <limits>
#include <optional>
#include <tuple>
#include <utility>

namespace ssx {

/// Template helper to run a real service service on one core and an small dummy
/// on other cores.
///
/// \tparam Service a class to be instantiated on each core.  Must expose
///         a \c stop() method that returns a \c future<>, to be called when
///         the service is stopped.
template<typename Service>
class maybe_service {
    using underlying_t = std::optional<Service>;
    underlying_t _underlying;

public:
    maybe_service() = default;

    template<class... Args>
    explicit maybe_service(bool start, Args&&... args) {
        if (start) {
            _underlying.emplace(std::forward<Args>(args)...);
        }
    }

    ss::future<> stop() {
        return _underlying ? _underlying->stop() : ss::make_ready_future();
    }

    // the ones below are only safe to be called on the shards where the service
    // has started

    constexpr const Service* operator->() const noexcept {
        return &*_underlying;
    }
    constexpr Service* operator->() noexcept { return &*_underlying; }

    constexpr const Service& operator*() const& noexcept {
        return *_underlying;
    }
    constexpr Service& operator*() & noexcept { return *_underlying; }
    constexpr const Service&& operator*() const&& noexcept {
        return &*_underlying;
    }
    constexpr Service&& operator*() && noexcept { return &*_underlying; }
};

/// Template helper to run a real service service on one core and an small dummy
/// on other cores.
///
/// \tparam Service a class to be instantiated on the core.  Must expose
///         a \c stop() method that returns a \c future<>, to be called when
///         the service is stopped.
template<typename Service>
class single_sharded : ss::sharded<maybe_service<Service>> {
    using base = ss::sharded<maybe_service<Service>>;
    // init with most insane value to maximize the chances to blow up with
    // segfault/asan if used; it shouldn't be used though
    ss::shard_id _shard{std::numeric_limits<ss::shard_id>::max()};

public:
    /// Constructs an empty \c single_sharded object.  No instances of the
    /// service are created.
    single_sharded() noexcept = default;
    single_sharded(const single_sharded& other) = delete;
    single_sharded& operator=(const single_sharded& other) = delete;
    /// Sharded object with T that inherits from peering_sharded_service
    /// cannot be moved safely, so disable move operations.
    single_sharded(single_sharded&& other) = delete;
    single_sharded& operator=(single_sharded&& other) = delete;
    /// Destroys a \c single_sharded object.  Must not be in a started state.
    ~single_sharded() = default;

    /// Starts \c Service by constructing an instance on the specified logical
    /// core with a copy of \c args passed to the constructor.
    ///
    /// \param shard Which shard to start on
    /// \param args Arguments to be forwarded to \c Service constructor
    /// \return a \ref seastar::future<> that becomes ready when the instance
    /// has been constructed.
    template<typename... Args>
    ss::future<> start_on(ss::shard_id shard, Args&&... args) noexcept {
        _shard = shard;
        return base::start(
          ss::sharded_parameter(
            [shard]() { return ss::this_shard_id() == shard; }),
          std::forward<Args>(args)...);
    }

    /// Stops the started instance and destroys it.
    ///
    /// For the instance, if it has started, its \c stop() method is called,
    /// and then it is destroyed.
    using base::stop;

    const Service& local() const {
        if (_shard != ss::this_shard_id()) [[unlikely]] {
            throw ss::no_sharded_instance_exception(
              ss::pretty_type_name(typeid(Service)));
        }
        return *base::local();
    }

    Service& local() {
        if (_shard != ss::this_shard_id()) [[unlikely]] {
            throw ss::no_sharded_instance_exception(
              ss::pretty_type_name(typeid(Service)));
        }
        return *base::local();
    }

    /// Invoke a callable on the instance of `Service`.
    ///
    /// \param options the options to forward to the \ref smp::submit_to()
    ///         called behind the scenes.
    /// \param func a callable with signature `Value (Service&, Args...)` or
    ///        `future<Value> (Service&, Args...)` (for some `Value` type), or
    ///        a pointer to a member function of Service
    /// \param args parameters to the callable; will be copied or moved. To
    /// pass by reference, use std::ref().
    ///
    /// \return result of calling `func(instance)` on the instance
    template<
      typename Func,
      typename... Args,
      typename Ret
      = ss::futurize_t<std::invoke_result_t<Func, Service&, Args...>>>
    requires std::invocable<Func, Service&, Args&&...>
    Ret invoke_on_instance(
      ss::smp_submit_to_options options, Func&& func, Args&&... args) {
        return base::invoke_on(
          _shard,
          options,
          // After smp::sumbit_to stores this lambda in the queue and we return
          // to the caller, any references provided may become dangling. So the
          // lambda must keep parameters and function values, not references.
          [func = std::forward<Func>(func),
           // This is wild, as it moves even from lvalues. However, that's how
           // sharded<>::invoke_on behaves, and we mimic it.
           args_tup = std::tuple(std::move(args)...)](
            maybe_service<Service>& maybe_service) mutable {
              return std::apply(
                // move the captured func (and args below), as we use them once
                std::move(func),
                // caller responsible to make sure the service is still there
                std::tuple_cat(
                  std::forward_as_tuple(*maybe_service), std::move(args_tup)));
          });
    }

    /// Invoke a callable on the instance of `Service`.
    ///
    /// \param func a callable with signature `Value (Service&)` or
    ///        `future<Value> (Service&)` (for some `Value` type), or a
    ///        pointer to a member function of Service
    /// \param args parameters to the callable
    /// \return result of calling `func(instance)` on the instance
    template<
      typename Func,
      typename... Args,
      typename Ret
      = ss::futurize_t<std::invoke_result_t<Func, Service&, Args&&...>>>
    requires std::invocable<Func, Service&, Args&&...>
    Ret invoke_on_instance(Func&& func, Args&&... args) {
        return invoke_on_instance(
          ss::smp_submit_to_options(),
          std::forward<Func>(func),
          std::forward<Args>(args)...);
    }
};

} // namespace ssx

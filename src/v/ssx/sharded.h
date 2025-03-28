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

#pragma once

#include "base/vlog.h"
#include "ssx/watchdog.h"

#include <seastar/core/sharded.hh>
#include <seastar/util/log.hh>

#include <chrono>

namespace ssx {

template<typename Service>
class sharded;

namespace detail {
inline seastar::logger shutdown_log{"shutdown"};
constexpr auto short_shutdown_timeout = std::chrono::seconds(10);
constexpr auto long_shutdown_timeout = std::chrono::seconds(180);

template<typename T>
struct is_reference_wrapper_of_ssx_sharded : std::false_type {};

template<typename T>
struct is_reference_wrapper_of_ssx_sharded<
  std::reference_wrapper<ssx::sharded<T>>> : std::true_type {};

template<typename T>
struct is_pointer_to_ssx_sharded : std::false_type {};

template<typename T>
struct is_pointer_to_ssx_sharded<ssx::sharded<T>*> : std::true_type {};

/// Convert std::reference_wrapper<ssx::sharded<T>> to
/// std::reference_wrapper<seastar::sharded<T>> and
/// ssx::sharded<T>* to seastar::sharded<T>*.
template<typename T>
auto unwrap_ssx_sharded_ref(T&& arg) {
    if constexpr (detail::is_reference_wrapper_of_ssx_sharded<
                    std::decay_t<T>>::value) {
        return std::ref(arg.get().underlying());
    } else if constexpr (detail::is_pointer_to_ssx_sharded<
                           std::decay_t<T>>::value) {
        return &arg->underlying();
    } else {
        return std::forward<T>(arg);
    }
}

} // namespace detail

/// The 'ssx::sharded' is a wrapper around seastar::sharded that provides
/// additional functionality for managing the shutdown of the service.
/// The difference between 'ssx::sharded' and 'seastar::sharded' is that
/// 'ssx::sharded' provides a 'stop' method that waits for the service to
/// stop and logs a warning if the shutdown takes too long.
///
/// The c-tor of 'ssx::sharded' takes the name of the service as an argument.
/// Alternatively, the name could be inferred using typeid.
///
/// The 'ssx::sharded' is not a drop in replacement for 'seastar::sharded'. It
/// implements the same interface as 'seastar::sharded' but it doesn't inherit
/// from it so that it can't be used in places where 'seastar::sharded' is
/// expected.
template<typename Service>
class sharded {
public:
    // Default constructor.
    // Uses type name as the name of the service.
    sharded()
      : _service_name(typeid(Service).name()) {}
    // C-tor with explicit service name.
    explicit sharded(ss::sstring service_name)
      : _service_name(std::move(service_name)) {}
    ~sharded() = default;
    sharded(const sharded& other) = delete;
    sharded& operator=(const sharded& other) = delete;

    /// Sharded object with T that inherits from peering_sharded_service
    /// cannot be moved safely, so disable move operations.
    sharded(sharded&& other) = delete;
    sharded& operator=(sharded&& other) = delete;

    template<typename... Args>
    seastar::future<> start(Args&&... args) noexcept {
        if constexpr (sizeof...(Args) > 0) {
            return _service.start(detail::unwrap_ssx_sharded_ref<Args>(
              std::forward<Args>(args))...);
        } else {
            return _service.start();
        }
    }

    template<typename... Args>
    seastar::future<> start_single(Args&&... args) noexcept {
        if constexpr (sizeof...(Args) > 0) {
            return _service.start_single(detail::unwrap_ssx_sharded_ref<Args>(
              std::forward<Args>(args))...);
        } else {
            return _service.start_single();
        }
    }

    /// Stop service on all shards.
    /// Log warning in case if service stop takes too long.
    /// Log shutdown sequence messages (stopping/stopped).
    seastar::future<> stop() { return do_stop(); }

    seastar::future<> invoke_on_all(
      seastar::smp_submit_to_options options,
      std::function<seastar::future<>(Service&)> func) noexcept {
        return _service.invoke_on_all(options, std::move(func));
    }

    seastar::future<>
    invoke_on_all(std::function<seastar::future<>(Service&)> func) noexcept {
        return _service.invoke_on_all(std::move(func));
    }

    template<typename Func, typename... Args>
    seastar::future<> invoke_on_all(
      seastar::smp_submit_to_options options,
      Func func,
      Args... args) noexcept {
        return _service.invoke_on_all(
          options, std::move(func), std::forward<Args>(args)...);
    }

    template<typename Func, typename... Args>
    seastar::future<> invoke_on_all(Func func, Args... args) noexcept {
        return _service.invoke_on_all(
          std::move(func), std::forward<Args>(args)...);
    }

    template<typename Func, typename... Args>
    seastar::future<> invoke_on_others(
      seastar::smp_submit_to_options options,
      Func func,
      Args... args) noexcept {
        return _service.invoke_on_others(
          options, std::move(func), std::forward<Args>(args)...);
    }

    template<typename Func, typename... Args>
    seastar::future<> invoke_on_others(Func func, Args... args) noexcept {
        return _service.invoke_on_others(
          std::move(func), std::forward<Args>(args)...);
    }

    template<typename Reducer, typename Func, typename... Args>
    auto map_reduce(Reducer&& r, Func&& func, Args&&... args) {
        return _service.map_reduce(
          std::forward<Reducer>(r),
          std::forward<Func>(func),
          std::forward<Args>(args)...);
    }

    template<typename Reducer, typename Func, typename... Args>
    auto map_reduce(Reducer&& r, Func&& func, Args&&... args) const {
        return _service.map_reduce(
          std::forward<Reducer>(r),
          std::forward<Func>(func),
          std::forward<Args>(args)...);
    }

    template<typename Mapper, typename Initial, typename Reduce>
    seastar::future<Initial>
    map_reduce0(Mapper map, Initial initial, Reduce reduce) {
        return _service.map_reduce0(
          std::move(map), std::move(initial), std::move(reduce));
    }

    template<typename Mapper, typename Initial, typename Reduce>
    seastar::future<Initial>
    map_reduce0(Mapper map, Initial initial, Reduce reduce) const {
        return _service.map_reduce0(
          std::move(map), std::move(initial), std::move(reduce));
    }

    template<typename Mapper>
    auto map(Mapper mapper) {
        return _service.map(std::move(mapper));
    }

    template<typename Func, typename... Args>
    auto invoke_on(
      unsigned id,
      seastar::smp_submit_to_options options,
      Func&& func,
      Args&&... args) {
        return _service.invoke_on(
          id, options, std::forward<Func>(func), std::forward<Args>(args)...);
    }

    template<typename Func, typename... Args>
    auto invoke_on(unsigned id, Func&& func, Args&&... args) {
        return _service.invoke_on(
          id,
          seastar::smp_submit_to_options(),
          std::forward<Func>(func),
          std::forward<Args>(args)...);
    }

    /// Gets a reference to the local instance.
    const Service& local() const noexcept { return _service.local(); }

    /// Gets a reference to the local instance.
    Service& local() noexcept { return _service.local(); }

    /// Gets a shared pointer to the local instance.
    seastar::shared_ptr<Service> local_shared() noexcept {
        return _service.local_shared();
    }

    /// Checks whether the local instance has been initialized.
    bool local_is_initialized() const noexcept {
        return _service.local_is_initialized();
    }

    operator seastar::sharded<Service>&() noexcept {
        return _service;
    } // NOLINT

    seastar::sharded<Service>& underlying() noexcept { return _service; }

private:
    seastar::future<> do_stop() {
        watchdog short_wd(detail::short_shutdown_timeout, [this] {
            vlog(
              detail::shutdown_log.info,
              "Service {} stop is taking more than {} seconds.",
              _service_name,
              std::chrono::duration_cast<std::chrono::seconds>(
                detail::short_shutdown_timeout)
                .count());
        });
        watchdog long_wd(detail::long_shutdown_timeout, [this] {
            vlog(
              detail::shutdown_log.warn,
              "Service {} stop is taking more than {} seconds!",
              _service_name,
              std::chrono::duration_cast<std::chrono::seconds>(
                detail::long_shutdown_timeout)
                .count());
        });
        vlog(detail::shutdown_log.info, "Stopping service: {}", _service_name);
        co_await _service.stop();
        vlog(detail::shutdown_log.info, "Stopped service: {}", _service_name);
    }

    seastar::sstring _service_name;
    seastar::sharded<Service> _service;
};

} // namespace ssx

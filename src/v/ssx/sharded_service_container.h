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

#include "base/seastarx.h"
#include "base/type_traits.h"
#include "base/vlog.h"
#include "ssx/watchdog.h"

#include <seastar/core/sharded.hh>
#include <seastar/core/sstring.hh>
#include <seastar/util/defer.hh>

using namespace std::chrono_literals;

namespace ssx {

// Encapsulates metods for starting and stopping sharded services.
// Child classes own the sharded services, but should construct them using the
// appropriate construct_*_service call.
//
// NOTE: the destructor of this class and all shutdown methods must be run in a
// Seastar thread.
class sharded_service_container {
public:
    // First warning timeout
    static constexpr auto short_shutdown_warning_timeout = 15s;
    // Time after which we will print the warning about the service shutdown
    // taking too long.
    static constexpr auto long_shutdown_warning_timeout = 120s;
    using deferred_actions
      = std::deque<ss::deferred_action<std::function<void()>>>;

    sharded_service_container(const sharded_service_container&) = delete;
    sharded_service_container(sharded_service_container&&) = delete;
    sharded_service_container
    operator=(const sharded_service_container&) = delete;
    sharded_service_container operator=(sharded_service_container&&) = delete;
    explicit sharded_service_container(ss::sstring s)
      : _log(s) {}
    ~sharded_service_container() { shutdown(); }

    void shutdown() {
        // Shut down services in reverse order to which they were registered.
        while (!_deferred.empty()) {
            _deferred.pop_back();
        }
    }

    // Stop the service.
    // The method should be invoked in the ss::thread context.
    template<class Service>
    void stop_service(
      Service& s,
      ss::sstring name,
      std::optional<ss::sstring> next_to_stop = std::nullopt) {
        // This watchdog is triggered after short period of time (30s). It
        // adds message to the log that service is taking a long time to
        // shutdown on INFO level.
        ssx::watchdog short_wd(short_shutdown_warning_timeout, [this, name] {
            vlog(
              _log.info,
              "Service {} is taking more than {} seconds to shut down.",
              name,
              std::chrono::duration_cast<std::chrono::seconds>(
                short_shutdown_warning_timeout)
                .count());
        });
        // This watchdog is triggered after long period of time. This indicates
        // a bug (most likely).
        ssx::watchdog long_wd(long_shutdown_warning_timeout, [this, name] {
            vlog(
              _log.info,
              "Service {} is taking more than {} seconds to shut down!",
              name,
              std::chrono::duration_cast<std::chrono::seconds>(
                long_shutdown_warning_timeout)
                .count());
        });
        if (next_to_stop.has_value()) {
            vlog(
              _log.info,
              "Stopping {}, ..next to shutdown is {}",
              name,
              *next_to_stop);
        } else {
            vlog(_log.info, "Stopping {}", name);
        }
        s.stop().get();
        if (!next_to_stop.has_value()) {
            vlog(_log.info, "Stopped {}", name);
        }
    }

    template<class T>
    static ss::sstring service_type_name() {
        if constexpr (::detail::is_specialization_of_v<T, std::unique_ptr>) {
            return service_type_name<typename T::element_type>();
        } else if constexpr (::detail::is_specialization_of_v<T, std::vector>) {
            return fmt::format(
              "std::vector<{}>", service_type_name<typename T::value_type>());
        }
        return ss::pretty_type_name(typeid(T));
    }

    // Shutdown the service with custom method.
    // The method should be invoked in the ss::thread context.
    template<class Service, class StopFunc>
    void shutdown_with_watchdog(
      Service& s,
      StopFunc stop_func,
      const ss::sstring& name = service_type_name<Service>(),
      std::source_location location = std::source_location::current()) {
        auto start_watchdog = [&name, this](
                                std::chrono::milliseconds timeout,
                                ss::log_level log_level) {
            return ssx::watchdog(timeout, [this, timeout, &name, log_level] {
                vlogl(
                  _log,
                  log_level,
                  "Service {} is taking more than {} seconds to shutdown",
                  name,
                  timeout / 1s);
            });
        };
        // This watchdog is triggered after short period of time (30s). It
        // adds message to the log that service is taking a long time to
        // shutdown on INFO level.
        ssx::watchdog short_wd = start_watchdog(
          short_shutdown_warning_timeout, ss::log_level::info);
        // This watchdog is triggered after long period of time. This indicates
        // a bug (most likely).
        ssx::watchdog long_wd = start_watchdog(
          long_shutdown_warning_timeout, ss::log_level::error);

        vlog(
          _log.info,
          "Shutting down: {} at {}:{}",
          name,
          location.file_name(),
          location.line());
        stop_func(s).get();
        vlog(
          _log.info,
          "Shutdown completed: {} started at {}:{}",
          name,
          location.file_name(),
          location.line());
    }

    /**
     * @brief Construct service boilerplate.
     *
     * Construct the given service s, calling start with the given arguments
     * and set up a shutdown callback to stop it.
     *
     * Returns the future from start(), typically you'll call get() on it
     * immediately to wait for creation to complete.
     *
     * @return the future returned by start()
     */
    template<typename Service, typename... Args>
    ss::future<> construct_service(ss::sharded<Service>& s, Args&&... args) {
        auto name = ss::pretty_type_name(typeid(Service));
        _deferred.emplace_back(
          [this, &s, name, next_to_stop = _last_constructed_service_name] {
              stop_service(s, name, next_to_stop);
          });
        _last_constructed_service_name = ss::sstring(name);
        return s.start(std::forward<Args>(args)...);
    }

    template<typename Service, typename... Args>
    void construct_single_service(std::unique_ptr<Service>& s, Args&&... args) {
        auto name = ss::pretty_type_name(typeid(Service));
        s = std::make_unique<Service>(std::forward<Args>(args)...);
        _deferred.emplace_back(
          [this, &s, name, next_to_stop = _last_constructed_service_name] {
              stop_service(*s, name, next_to_stop);
              s.reset();
          });
        _last_constructed_service_name = name;
    }

    template<typename Service, typename... Args>
    ss::future<>
    construct_single_service_sharded(ss::sharded<Service>& s, Args&&... args) {
        auto name = ss::pretty_type_name(typeid(Service));
        auto f = s.start_single(std::forward<Args>(args)...);
        _deferred.emplace_back(
          [this, &s, name, next_to_stop = _last_constructed_service_name] {
              stop_service(s, name, next_to_stop);
          });
        _last_constructed_service_name = name;
        return f;
    }

    template<typename ShardedComponent>
    auto ref_to_local(ss::sharded<ShardedComponent>& component) {
        return ss::sharded_parameter(
          [&component] { return std::ref(component.local()); });
    }

protected:
    ss::logger _log;
    deferred_actions _deferred;

private:
    std::optional<ss::sstring> _last_constructed_service_name;
};

} // namespace ssx

/*
 * Copyright 2020 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "base/likely.h"
#include "base/seastarx.h"

#include <seastar/util/backtrace.hh>
#include <seastar/util/log.hh>
#include <seastar/util/noncopyable_function.hh>

#include <atomic>

namespace detail {
struct dummyassert {
    static inline ss::logger l{"assert"};
};
inline dummyassert g_assert_log;
/**
 * @brief Class used to format assert messages
 *
 * This class will format the provided assert message and produce a backtrace
 * caused by an assert.  It also provides a means of registering a callback
 * function that will be called when an assert is triggered.
 */
class assert_log_holder {
public:
    // We want to enforce using a non-capturing function for callbacks to ensure
    // a static lifetime for the callback as we will not permit unregistering
    // a callback to prevent a race condition between unregistering the callback
    // on one thread and having the callback called by a different thread.
    using assert_cb_func = void (*)(std::string_view);
    /**
     * @brief Registers a vassert event
     *
     * @tparam Args The argument template
     * @param bt The backtrace from the assert
     * @param fmt The format string for the log
     * @param args The arguments to @p fmt
     */
    template<typename... Args>
    void register_event(
      ss::saved_backtrace bt,
      ss::logger::format_info_t<Args...> fmt,
      Args&&... args) noexcept {
        auto buffer = fmt::format(
          fmt::runtime(fmt.format), std::forward<Args>(args)...);

        g_assert_log.l.error("{}", buffer);
        g_assert_log.l.error("Backtrace:\n{}", bt);

        auto cb_func = _cb_func.load(std::memory_order_relaxed);
        if (cb_func != nullptr) {
            cb_func(buffer);
        }
    }

    void register_cb(assert_cb_func cb) {
        assert_cb_func before = nullptr;
        _cb_func.compare_exchange_strong(before, cb, std::memory_order_relaxed);
    }

private:
    std::atomic<assert_cb_func> _cb_func{nullptr};
};
inline assert_log_holder g_assert_log_holder;
} // namespace detail

/** Meant to be used in the same way as assert(condition, msg);
 * which means we use the negative conditional.
 * i.e.:
 *
 * open_fileset::~open_fileset() noexcept {
 *   vassert(_closed, "fileset not closed");
 * }
 *
 */
// NOLINTNEXTLINE(cppcoreguidelines-macro-usage)
#define vassert(x, msg, args...)                                               \
    /* NOLINTNEXTLINE(cppcoreguidelines-avoid-do-while) */                     \
    do {                                                                       \
        /*The !(x) is not an error. see description above*/                    \
        if (unlikely(!(x))) {                                                  \
            ::detail::g_assert_log_holder.register_event(                      \
              ss::current_backtrace(),                                         \
              "Assert failure: ({}:{}) '{}' " msg,                             \
              __FILE__,                                                        \
              __LINE__,                                                        \
              #x,                                                              \
              ##args);                                                         \
            __builtin_trap();                                                  \
        }                                                                      \
    } while (0)

/**
 * same as vassert but only debug mode. Use over assert for better
 * error messages.
 */
#ifndef NDEBUG
// NOLINTNEXTLINE(cppcoreguidelines-macro-usage)
#define dassert(x, msg, args...) vassert(x, msg, ##args)
#else
// NOLINTNEXTLINE(cppcoreguidelines-macro-usage)
#define dassert(...)
#endif

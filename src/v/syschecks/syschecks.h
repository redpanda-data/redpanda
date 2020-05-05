#pragma once

#include "seastarx.h"

#include <seastar/core/future.hh>
#include <seastar/util/log.hh>

#include <fmt/format.h>
#include <fmt/ostream.h>

#include <cpuid.h>
#include <cstdint>
#include <filesystem>
#include <sstream>

namespace syschecks {

extern ss::logger checklog;

static inline void initialize_intrinsics() {
    // https://gcc.gnu.org/onlinedocs/gcc/x86-Built-in-Functions.html#index-_005f_005fbuiltin_005fcpu_005finit-1
    //
    // This built-in function needs to be invoked along with the built-in
    // functions to check CPU type and features, __builtin_cpu_is and
    // __builtin_cpu_supports, only when used in a function that is executed
    // before any constructors are called. The CPU detection code is
    // automatically executed in a very high priority constructor.
    __builtin_cpu_init();
}
static inline void cpu() {
    // Do not use the macros __SSE4_2__ because we need to detect at runtime
    if (!__builtin_cpu_supports("sse4.2")) {
        throw std::runtime_error("sse4.2 support is required to run");
    }
}

ss::future<> disk(const ss::sstring& path);

void memory(bool ignore);

void systemd_raw_message(const ss::sstring& out);

void systemd_notify_ready();

template<typename... Args>
void systemd_message(const char* fmt, Args&&... args) {
    ss::sstring s = fmt::format(
      "STATUS={}\n", fmt::format(fmt, std::forward<Args>(args)...));
    systemd_raw_message(s);
}

/*
 * write the pid lock file for this process at the given path. if the lock file
 * cannot be created or locked an exception is thrown.
 *
 * an atexit handler is installed to remove the lock file when the process
 * exits either through `exit()` or returning from `main()`.
 *
 * clean-up can also be done for non-normal exit paths such as fatal signal
 * handlers. for this to work expose pidfile.cc::pidfile_delete and call from
 * the appropriate signal handler.
 *
 * https://app.clubhouse.io/vectorized/story/428/clean-up-pid-file-for-non-normal-exit-paths
 */
void pidfile_create(std::filesystem::path path);

} // namespace syschecks

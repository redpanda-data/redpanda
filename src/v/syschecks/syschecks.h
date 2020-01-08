#pragma once

#include "seastarx.h"

#include <seastar/core/future.hh>
#include <seastar/core/memory.hh>
#include <seastar/core/seastar.hh>

#include <fmt/format.h>
#include <fmt/ostream.h>
#include <systemd/sd-daemon.h>

#include <cpuid.h>
#include <cstdint>
#include <sstream>

namespace syschecks {

inline logger& checklog() {
    static logger _syslgr{"syschecks"};
    return _syslgr;
}

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

static inline future<> disk(const sstring& path) {
    return check_direct_io_support(path).then([path] {
        return file_system_at(path).then([path](auto fs) {
            if (fs != fs_type::xfs) {
                checklog().error(
                  "Path: `{}' is not on XFS. This is a non-supported setup. "
                  "Expect poor performance.",
                  path);
            }
        });
    });
}

static inline void memory(bool ignore) {
    static const uint64_t kMinMemory = 1 << 30;
    const auto shard_mem = memory::stats().total_memory();
    if (shard_mem >= kMinMemory) {
        return;
    }
    auto line = fmt::format(
      "Memory: '{}' below recommended: '{}'", kMinMemory, shard_mem);
    checklog().error(line.c_str());
    if (!ignore) {
        throw std::runtime_error(line);
    }
}

template<typename... Args>
void systemd_message(const char* fmt, Args&&... args) {
    auto s = fmt::format(
      "STATUS={}\n", fmt::format(fmt, std::forward<Args>(args)...));
    auto r = sd_notify(0, s.c_str());
    checklog().debug("sd_noify: {}", s);
    if (__builtin_expect(r < 0, false)) {
        checklog().trace(
          "Could not notify systemd sd_notify ready, error:{}", r);
    }
}

static inline void systemd_notify_ready() {
    auto r = sd_notify(0, "READY=1\nSTATUS=redpanda is ready; let's go!");
    checklog().info("sd_notify() READY=1");
    if (__builtin_expect(r < 0, false)) {
        checklog().trace(
          "Could not notify systemd sd_notify ready, error:{}", r);
    }
}

} // namespace syschecks

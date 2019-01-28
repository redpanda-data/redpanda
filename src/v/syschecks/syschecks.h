#pragma once
#include <sstream>

#include <seastar/core/reactor.hh>
#include <smf/human_bytes.h>
#include <smf/log.h>

namespace v {
namespace syschecks {
static inline void
cpu() {
  LOG_THROW_IF(!__builtin_cpu_supports("sse4.2"),
               "sse4.2 support is required to run");
}

static inline seastar::future<>
disk(seastar::sstring path) {
  return check_direct_io_support(path).then([path] {
    return file_system_at(path).then([path](auto fs) {
      if (fs != seastar::fs_type::xfs) {
        LOG_ERROR("Path: `{}' is not on XFS. This is a non-supported setup. "
                  "Expect poor performance.",
                  path);
      }
    });
  });
}

static inline void
memory(bool ignore) {
  static const uint64_t kMinMemory = 1 << 30;
  const auto shard_mem = seastar::memory::stats().total_memory();
  if (shard_mem >= kMinMemory) { return; }

  std::stringstream ss;
  ss << "Memory below recommended: `" << smf::human_bytes(kMinMemory)
     << "'. Actual is: `" << smf::human_bytes(shard_mem) << "'";

  LOG_THROW_IF(!ignore, "{}", ss.str());
  LOG_ERROR_IF(ignore, "{}", ss.str());
}

}  // namespace syschecks
}  // namespace v

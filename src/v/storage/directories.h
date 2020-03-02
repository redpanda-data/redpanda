#pragma once

#include "seastarx.h"
#include "storage/logger.h"
#include "syschecks/syschecks.h"
#include "vlog.h"

#include <seastar/core/future.hh>
#include <seastar/core/seastar.hh>
#include <seastar/core/sstring.hh>

namespace storage::directories {

static ss::future<> initialize(ss::sstring dir) {
    return recursive_touch_directory(dir)
      .handle_exception([dir](std::exception_ptr ep) {
          stlog.error(
            "Directory `{}` cannot be initialized. Failed with {}", dir, ep);
          return ss::make_exception_future<>(std::move(ep));
      })
      .then([dir] {
          vlog(stlog.info, "Checking `{}` for supported filesystems", dir);
          return syschecks::disk(dir);
      });
}

} // namespace storage::directories

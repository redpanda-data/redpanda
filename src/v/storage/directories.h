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

#include "seastarx.h"
#include "storage/logger.h"
#include "syschecks/syschecks.h"
#include "vlog.h"

#include <seastar/core/future.hh>
#include <seastar/core/seastar.hh>
#include <seastar/core/sstring.hh>

namespace storage::directories {

inline ss::future<> initialize(ss::sstring dir) {
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

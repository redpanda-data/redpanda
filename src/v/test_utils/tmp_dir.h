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

#include "base/seastarx.h"
#include "base/vlog.h"

#include <seastar/util/log.hh>
#include <seastar/util/tmp_file.hh>

#include <exception>

namespace details {
inline ss::logger tmpdir_logger("tmpdir-log");
}

class temporary_dir {
public:
    explicit temporary_dir(const char* test_name)
      : temporary_dir(std::filesystem::path("."), test_name) {}
    temporary_dir(const std::filesystem::path& root, const char* test_name) {
        auto path = root / fmt::format("{}-XXXX", test_name);
        try {
            vlog(
              details::tmpdir_logger.info,
              "Creating temporary directory {}",
              path.native());
            _dir = ss::make_tmp_dir(path.native()).get();
        } catch (...) {
            vlog(
              details::tmpdir_logger.error,
              "Can't create temporary directory at {}, Error: {}",
              path,
              std::current_exception());
            throw;
        }
    }
    temporary_dir(const temporary_dir&) = delete;
    temporary_dir& operator=(const temporary_dir&) = delete;
    temporary_dir(temporary_dir&&) = delete;
    temporary_dir& operator=(temporary_dir&&) = delete;
    ~temporary_dir() noexcept {
        try {
            vlog(details::tmpdir_logger.info, "About to call remove");
            if (_dir.has_path()) {
                _dir.remove().get();
            }
        } catch (...) {
            vlog(
              details::tmpdir_logger.error,
              "Can't remove temporary directory. Error: {}",
              std::current_exception());
        }
    }
    std::filesystem::path get_path() const { return _dir.get_path(); }
    ss::future<> remove() { return _dir.remove(); }

private:
    ss::tmp_dir _dir;
};

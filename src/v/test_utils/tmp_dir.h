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

struct temporary_dir_ctor_tag {};

class temporary_dir {
public:
    explicit temporary_dir(const char* test_name)
      : temporary_dir(std::filesystem::path("."), test_name) {}
    temporary_dir(const std::filesystem::path& root, const char* test_name) {
        _path = root / fmt::format("{}-XXXX", test_name);
        try {
            vlog(
              details::tmpdir_logger.info,
              "Creating temporary directory {}",
              _path.native());
            _dir = ss::make_tmp_dir(_path.native()).get();
        } catch (...) {
            vlog(
              details::tmpdir_logger.error,
              "Can't create temporary directory at {}, Error: {}",
              _path,
              std::current_exception());
            throw;
        }
    }
    // C-tor for the non thread context
    explicit temporary_dir(const char* test_name, temporary_dir_ctor_tag t)
      : temporary_dir(std::filesystem::path("."), test_name, t) {}
    // C-tor for the non thread context
    temporary_dir(
      const std::filesystem::path& root,
      const char* test_name,
      temporary_dir_ctor_tag) {
        _path = root / fmt::format("{}-XXXX", test_name);
    }
    ss::future<> create() {
        vlog(
          details::tmpdir_logger.info,
          "Creating temporary directory {}",
          _path.native());
        _dir = co_await ss::make_tmp_dir(_path.native());
    }
    temporary_dir(const temporary_dir&) = delete;
    temporary_dir& operator=(const temporary_dir&) = delete;
    temporary_dir(temporary_dir&&) = delete;
    temporary_dir& operator=(temporary_dir&&) = delete;
    ~temporary_dir() noexcept {
        try {
            vlog(details::tmpdir_logger.info, "About to call remove");
            if (_dir.has_path() && !_removed) {
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
    ss::future<> remove() {
        vlog(details::tmpdir_logger.info, "About to call remove");
        _removed = true;
        return _dir.remove();
    }

private:
    std::filesystem::path _path;
    ss::tmp_dir _dir;
    bool _removed{false};
};

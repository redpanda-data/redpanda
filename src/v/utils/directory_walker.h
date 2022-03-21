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

#include <seastar/core/file.hh>
#include <seastar/core/seastar.hh>
#include <seastar/core/sstring.hh>

#include <memory>
#include <utility>

/// \brief map over all entries of a directory
///
/// sstring ns = de.name;
/// return directory_walker::walk(ns, [ns, this](directory_entry de) {
///   return visit_topic(ns, std::move(de));
/// });
struct directory_walker {
    using walker_type = std::function<ss::future<>(ss::directory_entry)>;

    class stop_walk final : public std::exception {
    public:
        const char* what() const noexcept final {
            return "stop directory walk signal";
        }
    };

    static ss::future<> walk(ss::sstring dirname, walker_type walker_func) {
        return open_directory(std::move(dirname))
          .then([walker_func = std::move(walker_func)](ss::file f) mutable {
              auto s = f.list_directory(std::move(walker_func));
              return s.done().finally([f = std::move(f)]() mutable {
                  return f.close().finally([f = std::move(f)] {});
              });
          });
    }

    static ss::future<bool> empty(const std::filesystem::path& dir) {
        return directory_walker::walk(
                 dir.string(),
                 [](const ss::directory_entry&) {
                     return ss::make_exception_future<>(
                       directory_walker::stop_walk());
                 })
          .then([] { return true; })
          .handle_exception_type(
            [](const directory_walker::stop_walk&) { return false; });
    }
};

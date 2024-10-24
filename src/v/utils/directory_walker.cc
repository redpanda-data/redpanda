// Copyright 2022 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0
#include "directory_walker.h"

#include <seastar/core/seastar.hh>

class stop_walk final : public std::exception {
public:
    const char* what() const noexcept final {
        return "stop directory walk signal";
    }
};

ss::future<>
directory_walker::walk(std::string_view dirname, walker_type walker_func) {
    return ss::open_directory(dirname).then(
      [walker_func = std::move(walker_func)](ss::file f) mutable {
          auto s = f.list_directory(std::move(walker_func));
          return s.done().finally(
            [f = std::move(f)]() mutable { return f.close().finally([f] {}); });
      });
}

ss::future<bool> directory_walker::empty(const std::filesystem::path& dir) {
    return directory_walker::walk(
             dir.string(),
             [](const ss::directory_entry&) {
                 return ss::make_exception_future<>(stop_walk());
             })
      .then([] { return true; })
      .handle_exception_type([](const stop_walk&) { return false; });
}

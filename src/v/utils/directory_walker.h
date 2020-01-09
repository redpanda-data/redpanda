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

    static ss::future<> walk(ss::sstring dirname, walker_type walker_func) {
        return open_directory(std::move(dirname))
          .then([walker_func = std::move(walker_func)](ss::file f) mutable {
              auto s = std::make_unique<ss::subscription<ss::directory_entry>>(
                f.list_directory(std::move(walker_func)));
              return s->done().finally(
                [f = std::move(f), s = std::move(s)]() mutable {
                    return f.close().finally([f = std::move(f)] {});
                });
          });
    }
};

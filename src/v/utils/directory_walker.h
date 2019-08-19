#pragma once

#include "seastarx.h"

#include <seastar/core/file.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/sstring.hh>

#include <utility>

/// \brief map over all entries of a directory
///
/// sstring ns = de.name;
/// return directory_walker::walk(ns, [ns, this](directory_entry de) {
///   return visit_topic(ns, std::move(de));
/// });
///
struct directory_walker {
    using func_t = std::function<future<>(directory_entry)>;

    template<typename Func>
    static future<> walk(sstring dirname, Func f) {
        return open_directory(dirname).then(
          [f = std::move(f)](auto d) mutable {
              auto w = make_lw_shared<directory_walker>(
                std::move(d), std::move(f));
              return w->done().finally([w] {});
          });
    }

    explicit directory_walker(file d, func_t f)
      : directory(make_lw_shared<file>(std::move(d)))
      , listing(directory->list_directory(std::move(f))) {
    }

    ~directory_walker() = default;

    future<> done() {
        auto l = directory;
        return listing.done().then([l] { return l->close(); }).finally([l] {});
    }

    lw_shared_ptr<file> directory;
    subscription<directory_entry> listing;
};

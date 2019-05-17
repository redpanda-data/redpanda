#pragma once

#include <utility>

#include <seastar/core/file.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/sstring.hh>


/// \brief map over all entries of a directory
///
/// seastar::sstring ns = de.name;
/// return directory_walker::walk(ns, [ns, this](seastar::directory_entry de) {
///   return visit_topic(ns, std::move(de));
/// });
///
struct directory_walker {
  using func_t = std::function<seastar::future<>(seastar::directory_entry)>;

  template <typename Func>
  static seastar::future<>
  walk(seastar::sstring dirname, Func f) {
    return seastar::open_directory(dirname).then(
      [f = std::move(f)](auto d) mutable {
        auto w =
          seastar::make_lw_shared<directory_walker>(std::move(d), std::move(f));
        return w->done().finally([w] {});
      });
  }

  explicit directory_walker(seastar::file d, func_t f)
    : directory(seastar::make_lw_shared<seastar::file>(std::move(d))),
      listing(directory->list_directory(std::move(f))) {}

  ~directory_walker() = default;

  seastar::future<>
  done() {
    auto l = directory;
    return listing.done().then([l] { return l->close(); }).finally([l] {});
  }

  seastar::lw_shared_ptr<seastar::file> directory;
  seastar::subscription<seastar::directory_entry> listing;
};


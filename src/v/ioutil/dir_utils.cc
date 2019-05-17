#include "dir_utils.h"

#include <vector>

#include <seastar/core/file.hh>
#include <seastar/core/reactor.hh>

/// \brief creates directory tree
seastar::future<>
dir_utils::create_dir_tree(seastar::sstring name) {
  std::vector<std::size_t> idxs;
  for (auto i = 1u /* no need to create `/` */; i < name.size(); ++i) {
    if (name[i] == '/') idxs.push_back(i);
  }

  auto do_create_fn = [](seastar::sstring dname) {
    static thread_local seastar::semaphore dirsem{1};
    return seastar::with_semaphore(dirsem, 1, [dname] {
      return seastar::file_exists(dname).then([dname](bool exists) {
        if (exists) { return seastar::make_ready_future<>(); }
        return seastar::make_directory(dname);
      });
    });
  };

  // the simple case - most common
  if (idxs.empty()) { return do_create_fn(name); }

  return seastar::do_with(std::move(idxs),
                          [name, do_create_fn](auto &vec) {
                            return seastar::do_for_each(
                              vec.begin(), vec.end(),
                              [name, do_create_fn](std::size_t idx) {
                                return do_create_fn(name.substr(0, idx));
                              });
                          })
    .then([name, do_create_fn] { return do_create_fn(name); });
}

#pragma once

#include <seastar/core/future.hh>
#include <seastar/core/sstring.hh>

namespace v {
/// \brief creates directory tree
struct dir_utils {
  static seastar::future<> create_dir_tree(seastar::sstring name);
};
}  // namespace v

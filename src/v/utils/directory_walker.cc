#include "directory_walker.h"

ss::future<>
directory_walker::walk(std::string_view dirname, walker_type walker_func) {
    return ss::open_directory(dirname).then(
      [walker_func = std::move(walker_func)](ss::file f) mutable {
          auto s = f.list_directory(std::move(walker_func));
          return s.done().finally([f = std::move(f)]() mutable {
              return f.close().finally([f = std::move(f)] {});
          });
      });
}

ss::future<bool> directory_walker::empty(const std::filesystem::path& dir) {
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

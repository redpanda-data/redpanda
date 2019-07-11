#include "ioutil/dir_utils.h"

#include <seastar/core/file.hh>
#include <seastar/core/reactor.hh>

#include <vector>

// clang-format on
/// \brief creates directory tree
future<> dir_utils::create_dir_tree(sstring name) {
    std::vector<std::size_t> idxs;
    for (auto i = 1u /* no need to create `/` */; i < name.size(); ++i) {
        if (name[i] == '/') {
            idxs.push_back(i);
        }
    }

    auto do_create_fn = [](sstring dname) {
        static thread_local semaphore dirsem{1};
        return with_semaphore(dirsem, 1, [dname] {
            return file_exists(dname).then([dname](bool exists) {
                if (exists) {
                    return make_ready_future<>();
                }
                return make_directory(dname);
            });
        });
    };

    // the simple case - most common
    if (idxs.empty()) {
        return do_create_fn(name);
    }

    return do_with(
             std::move(idxs),
             [name, do_create_fn](auto& vec) {
                 return do_for_each(
                   vec.begin(),
                   vec.end(),
                   [name, do_create_fn](std::size_t idx) {
                       return do_create_fn(name.substr(0, idx));
                   });
             })
      .then([name, do_create_fn] { return do_create_fn(name); });
}
// clang-format on

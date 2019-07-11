#pragma once

#include "seastarx.h"

#include <seastar/core/future.hh>
#include <seastar/core/sstring.hh>

/// \brief creates directory tree
struct dir_utils {
    static future<> create_dir_tree(sstring name);
};

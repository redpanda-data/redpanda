#pragma once

#include "seastarx.h"

#include <seastar/core/sstring.hh>

#include <cstdlib>
#include <filesystem>

namespace config {

struct data_directory_path {
    std::filesystem::path path;
    ss::sstring as_sstring() const { return path.string(); }
};

} // namespace config
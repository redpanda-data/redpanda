#pragma once

#include <optional>
#include <string>
#include <string_view>

namespace test_utils {

/*
 * Usage:
 *  - Add a `data = [path/to/some.file]` to the test target
 *  - Call get_runfile_path with "root/.../path/to/some.file"
 *
 * If the return value is std::nullopt then it means that the test is being
 * compiled with CMake. Otherwise, it is compiled with Bazel and the return
 * value contains the path to the data file.
 */
std::optional<std::string> get_runfile_path(std::string_view);

} // namespace test_utils

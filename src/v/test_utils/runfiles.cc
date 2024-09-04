#include "test_utils/runfiles.h"

#ifdef BAZEL_TEST
#include "tools/cpp/runfiles/runfiles.h"
#endif

#include <fmt/format.h>

#include <memory>

namespace test_utils {

std::optional<std::string>
get_runfile_path([[maybe_unused]] std::string_view path) {
#ifdef BAZEL_TEST
    using bazel::tools::cpp::runfiles::Runfiles;
    std::string error;
    std::unique_ptr<Runfiles> runfiles(
      Runfiles::CreateForTest(BAZEL_CURRENT_REPOSITORY, &error));
    if (runfiles == nullptr) {
        throw std::runtime_error(error);
    }
    return runfiles->Rlocation(fmt::format("_main/{}", path));
#else
    return std::nullopt;
#endif
}

} // namespace test_utils

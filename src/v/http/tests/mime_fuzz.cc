#include "http/mime.h"

#include <cstdint>
#include <stdexcept>
#include <string_view>
#include <tuple>

extern "C" int LLVMFuzzerTestOneInput(const uint8_t* data, size_t size) {
    // NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast)
    std::string_view in(reinterpret_cast<const char*>(data), size);

    try {
        std::ignore = http::parse_media_type(in);
    } catch (std::runtime_error&) {
        // Ignore runtime_error exceptions. Nothing guarantees valid input.
    }

    return 0;
}

#pragma once

#include <cstdint>

namespace pandaproxy {

enum class serialization_format : uint8_t { none = 0, binary_v2, unsupported };

} // namespace pandaproxy

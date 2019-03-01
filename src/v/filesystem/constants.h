#pragma once
#include <cstdint>

namespace v {
/// \brief useful for failure recovery of invalid log segments
static constexpr int16_t kWalHeaderMagicNumber = 0xcafe;
static constexpr int8_t kWalHeaderVersion = 1;
}  // namespace v

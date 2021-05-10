#pragma once

#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wunused-parameter"

#include "libplatform/libplatform.h"
#include "v8.h"

#pragma clang diagnostic pop


namespace coproc {

std::unique_ptr<v8::Platform> init_v8_platform();

void stop_v8_platform();

} // namespace coproc
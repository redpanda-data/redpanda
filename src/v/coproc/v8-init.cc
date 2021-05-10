#include "coproc/v8-init.h"

namespace coproc {

std::unique_ptr<v8::Platform> init_v8_platform() {
    auto platform = v8::platform::NewDefaultPlatform();
    v8::V8::InitializePlatform(platform.get());
    v8::V8::Initialize();
    return platform;
}

void stop_v8_platform() {
    v8::V8::Dispose();
    v8::V8::ShutdownPlatform();
}

} // namespace coproc
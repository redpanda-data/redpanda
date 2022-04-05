/*
 * Copyright 2021 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#pragma once

#include <libplatform/libplatform.h>

#include <v8.h>

namespace v8_engine {

// This class contains environment for v8.
// Can be used only one time per application.
class enviroment {
public:
    // Init single thread v8::Platform. If we use NewDefaultPlatform platform
    // it will create threadpool for background tasks.
    // Environment must be created before all v8 instance are starting!
    enviroment();

    enviroment(const enviroment& other) = delete;
    enviroment& operator=(const enviroment& other) = delete;
    enviroment(enviroment&& other) = default;
    enviroment& operator=(enviroment&& other) = default;

    // Stop v8 envoroment and release any resources used by v8.
    // Must be called after all v8 instances are finished!
    ~enviroment();

private:
    std::unique_ptr<v8::Platform> _platform;
};

} // namespace v8_engine

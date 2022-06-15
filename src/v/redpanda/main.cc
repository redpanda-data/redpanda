// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "redpanda/application.h"
#include "syschecks/syschecks.h"

namespace debug {
application* app;
}

int main(int argc, char** argv, char** /*env*/) {
    // must be the first thing called
    syschecks::initialize_intrinsics();
    application app;
    debug::app = &app;
    return app.run(argc, argv);
}

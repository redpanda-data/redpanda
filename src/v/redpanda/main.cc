// Copyright 2020 Vectorized, Inc.
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

static constexpr std::string_view community_msg = R"banner(

Welcome to the Redpanda community!

Documentation: https://docs.redpanda.com - Product documentation site
GitHub Discussion: https://github.com/redpanda-data/redpanda/discussions - Longer, more involved discussions
GitHub Issues: https://github.com/redpanda-data/redpanda/issues - Report and track issues with the codebase
Support: https://support.redpanda.com - Contact the support team privately
Product Feedback: https://redpanda.com/feedback - Let us know how we can improve your experience
Slack: https://redpanda.com/slack - Chat about all things Redpanda. Join the conversation!
Twitter: https://twitter.com/redpandadata - All the latest Redpanda news!

)banner";

int main(int argc, char** argv, char** /*env*/) {
    // using endl explicitly for flushing
    std::cout << community_msg << std::endl;
    // must be the first thing called
    syschecks::initialize_intrinsics();
    application app;
    debug::app = &app;
    return app.run(argc, argv);
}

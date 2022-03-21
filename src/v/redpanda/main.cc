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

Documentation: https://docs.redpanda.com - official docs site
Github Discussion: https://github.com/redpanda-data/redpanda/discussions - longer, async, thoughtful discussions
GitHub Issues: https://github.com/redpanda-data/redpanda/issues - reporting and tracking issues with the codebase
Support: https://support.redpanda.com - share private information with the production support team
Product Feedback: https://redpanda.com/feedback - let us know how we can improve your experience
Slack: https://redpanda.com/slack - the main way the community interacts with one another in real time :)
Twitter: https://twitter.com/redpandadata - come say hi!

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

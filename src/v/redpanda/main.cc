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

Slack: https://vectorized.io/slack - is the main way the community interacts with one another in real time :)
Twitter: https://twitter.com/vectorizedio - come say hi!
Github Discussion: https://github.com/redpanda-data/redpanda/discussions - is preferred for longer, async, thoughtful discussions
GitHub Issues: https://github.com/redpanda-data/redpanda/issues - is reserved only for actual issues. Please use the GitHub for discussions.
Documentation: https://vectorized.io/docs/ - official docs site
Support: https://support.vectorized.io/ - to share private information with the production support vectorized team
Product Feedback: https://vectorized.io/feedback/ - let us know how we can improve your experience

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

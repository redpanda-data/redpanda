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

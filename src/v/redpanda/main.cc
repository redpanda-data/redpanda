#include "redpanda/application.h"
#include "syschecks/syschecks.h"

int main(int argc, char** argv, char** env) {
    // must be the first thing called
    syschecks::initialize_intrinsics();
    application app;
    return app.run(argc, argv);
}

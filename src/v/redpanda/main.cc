#include "redpanda/application.h"

int main(int argc, char** argv, char** env) {
    application app;
    return app.run(argc, argv);
}

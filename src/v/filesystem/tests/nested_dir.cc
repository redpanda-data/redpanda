#include "ioutil/dir_utils.h"

#include <seastar/core/app-template.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/sleep.hh>

#include <smf/log.h>

#include <iostream>

int main(int argc, char** argv, char** env) {
    // flush every log line
    std::cout.setf(std::ios::unitbuf);
    app_template app;
    return app.run(argc, argv, [&] {
        smf::app_run_log_level(log_level::trace);
        sstring test = "foo/bar/baz/w00t";
        return dir_utils::create_dir_tree(test).then([test] {
            return file_exists(test).then([test](bool exists) {
                LOG_THROW_IF(!exists, "Could not create directory: {}", test);
            });
        });
    });
}

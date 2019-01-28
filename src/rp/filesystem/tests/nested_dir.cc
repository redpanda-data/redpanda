#include <iostream>

#include <seastar/core/app-template.hh>
#include <seastar/core/sleep.hh>
#include <smf/log.h>

#include "ioutil/dir_utils.h"

int
main(int argc, char **argv, char **env) {
  // flush every log line
  std::cout.setf(std::ios::unitbuf);
  seastar::app_template app;
  return app.run(argc, argv, [&] {
    smf::app_run_log_level(seastar::log_level::trace);
    seastar::sstring test = "foo/bar/baz/w00t";
    return rp::dir_utils::create_dir_tree(test).then([test] {
      return seastar::file_exists(test).then([test](bool exists) {
        LOG_THROW_IF(!exists, "Could not create directory: {}", test);
      });
    });
  });
}

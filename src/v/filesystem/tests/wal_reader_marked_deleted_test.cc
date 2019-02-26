#include <seastar/core/sleep.hh>
#include <seastar/core/app-template.hh>
#include <seastar/core/distributed.hh>
#include <seastar/core/lowres_clock.hh>
#include <seastar/core/sstring.hh>
#include <smf/log.h>

#include "ioutil/priority_manager.h"
#include "wal_opts.h"
#include "wal_reader_node.h"
#include "wal_writer_node.h"

// test dir only
#include "wal_topic_test_input.h"

void
add_opts(boost::program_options::options_description_easy_init o) {
  namespace po = boost::program_options;
  o("write-ahead-log-dir", po::value<std::string>(), "log directory");
}

int
main(int args, char **argv, char **env) {
  std::cout.setf(std::ios::unitbuf);
  seastar::app_template app;

  try {
    add_opts(app.add_options());

    return app.run(args, argv, [&] {
      smf::app_run_log_level(seastar::log_level::trace);
      auto &config = app.configuration();
      auto dir = config["write-ahead-log-dir"].as<std::string>();

      const seastar::sstring target_filename = dir + "/0.wal";

      LOG_THROW_IF(dir.empty(), "Empty write-ahead-log-dir");
      LOG_INFO("log_segment test dir: {}", dir);

      return seastar::do_with(
               v::wal_topic_test_input(
                 "empty_namespace", "dummy_topic",
                 // partitions
                 seastar::smp::count * 2,
                 // type (can be compaction)
                 v::wal_topic_type::wal_topic_type_regular,
                 // map of properties for topic
                 {{"prop-for-topic", "maybe-store-access-keys"}}),
               v::wal_opts(dir.c_str(), std::chrono::milliseconds(5)),
               [](auto &input, auto &wopts) {
                 auto writer = seastar::make_lw_shared<v::wal_writer_node>(
                   v::wal_writer_node_opts(
                     wopts, input.get_create(), wopts.directory,
                     v::priority_manager::get().default_priority(),
                     [](auto name) { return seastar::make_ready_future<>(); },
                     [](auto name, auto sz) {
                       return seastar::make_ready_future<>();
                     }));

                 return writer->open().then([writer, &input] {
                   return seastar::do_with(
                            input.write_requests(), std::size_t(0),
                            [writer](auto &reqs, std::size_t &idx) {
                              auto w = std::move(reqs[idx++]);
                              if (w.runner_core != 0) {
                                // skip writes not for this core
                                return seastar::make_ready_future<>();
                              }

                              return writer->append(std::move(w))
                                .then([&](auto write_reply) {
                                  return seastar::make_ready_future<>();
                                });
                            })
                     .then([writer] {
                       return writer->close().finally([writer] {});
                     });
                 });
               })
        .then([target_filename] { return seastar::file_size(target_filename); })
        .then([target_filename](auto sz) {
          auto reader = seastar::make_lw_shared<v::wal_reader_node>(
            0, sz, seastar::lowres_system_clock::now(), target_filename);
          return reader->open()
            .then([reader] {
              reader->mark_for_deletion();
              return reader->close().finally([reader] {});
            })
            .then([] { return seastar::sleep(std::chrono::milliseconds(15)); });
        })
        .then(
          [target_filename] { return seastar::file_exists(target_filename); })
        .then([target_filename](bool exists) {
          LOG_THROW_IF(exists, "Filename: {} should not exist",
                       target_filename);
          LOG_INFO("Sucess! {} does not exist", target_filename);
          return seastar::make_ready_future<int>(0);
        });
    });
  } catch (const std::exception &e) {
    std::cerr << "Fatal exception: " << e.what() << std::endl;
  }
}

#include "filesystem/tests/wal_topic_test_input.h"
#include "filesystem/wal_opts.h"
#include "filesystem/wal_reader_node.h"
#include "filesystem/wal_writer_node.h"
#include "ioutil/priority_manager.h"

#include <seastar/core/app-template.hh>
#include <seastar/core/distributed.hh>
#include <seastar/core/lowres_clock.hh>
#include <seastar/core/sleep.hh>
#include <seastar/core/sstring.hh>

#include <smf/log.h>

void add_opts(boost::program_options::options_description_easy_init o) {
    namespace po = boost::program_options;
    o("write-ahead-log-dir", po::value<std::string>(), "log directory");
}

int main(int args, char** argv, char** env) {
    std::cout.setf(std::ios::unitbuf);
    app_template app;
    config::configuration cfg;

    try {
        add_opts(app.add_options());

        return app.run(args, argv, [&] {
            smf::app_run_log_level(log_level::trace);
            auto& config = app.configuration();
            auto dir = config["write-ahead-log-dir"].as<std::string>();

            const sstring target_filename = dir + "/0.0.wal";

            LOG_THROW_IF(dir.empty(), "Empty write-ahead-log-dir");
            LOG_INFO("log_segment test dir: {}", dir);

            cfg.data_directory(dir);
            cfg.writer_flush_period_ms(5);
            return do_with(
                     wal_topic_test_input(
                       "empty_namespace",
                       "dummy_topic",
                       // partitions
                       smp::count * 2,
                       // type (can be compaction)
                       wal_topic_type::wal_topic_type_regular,
                       // map of properties for topic
                       {{"prop-for-topic", "maybe-store-access-keys"}}),
                     wal_opts(cfg),
                     [](auto& input, auto& wopts) {
                         auto writer = make_lw_shared<wal_writer_node>(
                           wal_writer_node_opts(
                             wopts,
                             0 /*epoch*/,
                             0 /*term*/,
                             input.get_create(),
                             wopts.directory(),
                             priority_manager::get().default_priority(),
                             [](auto name) { return make_ready_future<>(); },
                             [](auto name, auto sz) {
                                 return make_ready_future<>();
                             }));

                         return writer->open().then([writer, &input] {
                             return do_with(
                                      input.write_requests(),
                                      std::size_t(0),
                                      [writer](auto& reqs, std::size_t& idx) {
                                          auto w = std::move(reqs[idx++]);
                                          if (w.runner_core != 0) {
                                              // skip writes not for this core
                                              return make_ready_future<>();
                                          }

                                          return writer->append(std::move(w))
                                            .then([&](auto write_reply) {
                                                return make_ready_future<>();
                                            });
                                      })
                               .then([writer] {
                                   return writer->close().finally([writer] {});
                               });
                         });
                     })
              .then([target_filename] { return file_size(target_filename); })
              .then([target_filename](auto sz) {
                  auto reader = make_lw_shared<wal_reader_node>(
                    0 /*epoch*/,
                    0 /*term*/,
                    sz,
                    lowres_system_clock::now(),
                    target_filename);
                  return reader->open()
                    .then([reader] {
                        reader->mark_for_deletion();
                        return reader->close().finally([reader] {});
                    })
                    .then([] { return sleep(std::chrono::milliseconds(15)); });
              })
              .then([target_filename] { return file_exists(target_filename); })
              .then([target_filename](bool exists) {
                  LOG_THROW_IF(
                    exists, "Filename: {} should not exist", target_filename);
                  LOG_INFO("Sucess! {} does not exist", target_filename);
                  return make_ready_future<int>(0);
              });
        });
    } catch (const std::exception& e) {
        std::cerr << "Fatal exception: " << e.what() << std::endl;
    }
}

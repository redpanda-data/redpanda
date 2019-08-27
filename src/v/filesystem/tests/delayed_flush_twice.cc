#include "filesystem/tests/wal_smash.h"
#include "redpanda/config/configuration.h"

#include <seastar/core/app-template.hh>
#include <seastar/core/distributed.hh>
#include <seastar/core/sleep.hh>
#include <seastar/core/sstring.hh>

#include <smf/log.h>

#include <chrono>

int main(int args, char** argv, char** env) {
    std::cout.setf(std::ios::unitbuf);
    app_template app;
    distributed<write_ahead_log> w;
    distributed<wal_smash> smash;
    config::configuration cfg;

    return app.run(args, argv, [&] {
        smf::app_run_log_level(log_level::trace);
        engine().at_exit([&] { return smash.stop(); });
        engine().at_exit([&] { return w.stop(); });
        cfg.data_directory(".");
        cfg.writer_flush_period_ms(2);
        return w.start(wal_opts(cfg))
          .then([&] { return w.invoke_on_all(&write_ahead_log::open); })
          .then([&] { return w.invoke_on_all(&write_ahead_log::index); })
          .then([&] {
              wal_smash_opts o;
              o.write_batch_size = 1;
              o.topic_namespace = "hulk";
              o.topic = "smash";
              return smash.start(std::move(o), &w);
          })
          .then([&] {
              return smash.invoke_on_all([](auto& smasher) {
                  // copy
                  auto vec
                    = smasher.core2partitions().find(engine().cpu_id())->second;
                  return smasher.create(vec).then([](auto _) { /*ignore*/ });
              });
          })
          .then([&] {
              return smash.invoke_on_all([](auto& smasher) {
                  // copy
                  auto vec
                    = smasher.core2partitions().find(engine().cpu_id())->second;
                  return do_with(std::move(vec), [&smasher](auto& v) {
                      return do_for_each(
                        v.begin(), v.end(), [&](auto partition) {
                            return smasher.write_one(partition).then(
                              [](auto _) { /*ignore*/ });
                        });
                  });
              });
          })
          .then([] { return sleep(std::chrono::milliseconds(10)); })
          .then([&] {
              return smash.invoke_on_all([](auto& smasher) {
                  // copy
                  auto vec
                    = smasher.core2partitions().find(engine().cpu_id())->second;
                  return do_with(std::move(vec), [&smasher](auto& v) {
                      return do_for_each(
                        v.begin(), v.end(), [&](auto partition) {
                            return smasher.read_one(partition).then([](auto _) {
                                /*ignore*/
                            });
                        });
                  });
              });
          });
    });
}

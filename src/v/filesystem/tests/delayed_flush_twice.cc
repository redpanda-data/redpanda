#include <chrono>

#include <seastar/core/app-template.hh>
#include <seastar/core/distributed.hh>
#include <seastar/core/sleep.hh>
#include <seastar/core/sstring.hh>
#include <smf/log.h>

// test dir only
#include "wal_smash.h"


int
main(int args, char **argv, char **env) {
  std::cout.setf(std::ios::unitbuf);
  seastar::app_template app;
  seastar::distributed<write_ahead_log> w;
  seastar::distributed<wal_smash> smash;

  return app.run(args, argv, [&] {
    smf::app_run_log_level(seastar::log_level::trace);
    seastar::engine().at_exit([&] { return smash.stop(); });
    seastar::engine().at_exit([&] { return w.stop(); });

    return w.start(wal_opts(".", std::chrono::milliseconds(5)))
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
        return smash.invoke_on_all([](auto &smasher) {
          // copy
          auto vec =
            smasher.core2partitions().find(seastar::engine().cpu_id())->second;
          return smasher.create(vec).then([](auto _) { /*ignore*/ });
        });
      })
      .then([&] {
        return smash.invoke_on_all([](auto &smasher) {
          // copy
          auto vec =
            smasher.core2partitions().find(seastar::engine().cpu_id())->second;
          return seastar::do_with(std::move(vec), [&smasher](auto &v) {
            return seastar::do_for_each(
              v.begin(), v.end(), [&](auto partition) {
                return smasher.write_one(partition).then(
                  [](auto _) { /*ignore*/ });
              });
          });
        });
      })
      .then([] { return seastar::sleep(std::chrono::milliseconds(10)); })
      .then([&] {
        return smash.invoke_on_all([](auto &smasher) {
          // copy
          auto vec =
            smasher.core2partitions().find(seastar::engine().cpu_id())->second;
          return seastar::do_with(std::move(vec), [&smasher](auto &v) {
            return seastar::do_for_each(
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

#include "filesystem/tests/wal_smash.h"

#include <seastar/core/app-template.hh>
#include <seastar/core/distributed.hh>
#include <seastar/core/sleep.hh>
#include <seastar/core/sstring.hh>

#include <smf/log.h>

#include <chrono>

int main(int args, char** argv, char** env) {
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
              return smash.invoke_on_all([](auto& smasher) {
                  // copy
                  auto vec = smasher.core2partitions()
                               .find(seastar::engine().cpu_id())
                               ->second;
                  return smasher.create(vec).then([](auto _) { /*ignore*/ });
              });
          })
          .then([&] {
              return smash.invoke_on(0, [](auto& smasher) {
                  auto& o = smasher.opts();
                  const int32_t out_of_bounds_partition = 100;
                  wal_nstpidx idx(o.ns_id, o.topic_id, out_of_bounds_partition);
                  auto core = jump_consistent_hash(
                    idx.id(), seastar::smp::count);
                  return seastar::smp::submit_to(
                    core, [&smasher, out_of_bounds_partition] {
                        return smasher.write_one(out_of_bounds_partition)
                          .then([](auto reply) {
                              // ALSO test that the error is propagated to the
                              // released ptr
                              std::unique_ptr<wal_put_replyT> ptr
                                = reply->release();

                              LOG_THROW_IF(
                                ptr->error
                                  != wal_put_errno::
                                    wal_put_errno_invalid_ns_topic_partition,
                                "Should have returned correct "
                                "invalid_ns_topic_partition "
                                "error. got: {}",
                                EnumNamewal_put_errno(ptr->error));

                              LOG_INFO("WRITE: Success. Correct error "
                                       "condition for out "
                                       "of bounds");
                          });
                    });
              });
          })
          .then([&] {
              return smash.invoke_on(0, [](auto& smasher) {
                  auto& o = smasher.opts();
                  const int32_t out_of_bounds_partition = 100;
                  wal_nstpidx idx(o.ns_id, o.topic_id, out_of_bounds_partition);
                  auto core = jump_consistent_hash(
                    idx.id(), seastar::smp::count);
                  return seastar::smp::submit_to(
                    core, [&smasher, out_of_bounds_partition] {
                        return smasher.read_one(out_of_bounds_partition)
                          .then([](auto reply) {
                              LOG_THROW_IF(
                                reply->reply().error
                                  != wal_read_errno::
                                    wal_read_errno_invalid_ns_topic_partition,
                                "Should have returned correct "
                                "invalid_ns_topic_partition "
                                "error. got: {}",
                                EnumNamewal_read_errno(reply->reply().error));

                              LOG_INFO("READ: Success. Correct error condition "
                                       "for out "
                                       "of bounds");
                          });
                    });
              });
          });
    });
}

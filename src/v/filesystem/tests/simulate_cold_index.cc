#include "filesystem/tests/gen_create_topic_buf.h"
#include "filesystem/tests/wal_topic_test_input.h"
#include "filesystem/wal_cold_boot.h"
#include "filesystem/wal_core_mapping.h"
#include "filesystem/wal_opts.h"
#include "filesystem/wal_pretty_print_utils.h"
#include "filesystem/wal_requests.h"
#include "filesystem/write_ahead_log.h"

#include <seastar/core/app-template.hh>
#include <seastar/core/distributed.hh>
#include <seastar/core/sleep.hh>
#include <seastar/core/sstring.hh>

#include <smf/fbs_typed_buf.h>
#include <smf/log.h>

#include <flatbuffers/minireflect.h>

#include <chrono>
#include <set>

// creating a namespace with `-` tests the regexes
static const sstring kNS = "empty-ns007";
// creating a topic with `_` tests the regexes
static const sstring kTopic = "dummy_topic";

future<> do_creates(uint32_t core, write_ahead_log& w) {
    return do_with(
      std::vector<int32_t>{} /*partitions*/,
      wal_topic_test_input(
        kNS,
        kTopic,
        // partitions
        16,
        // type (can be compaction)
        wal_topic_type::wal_topic_type_regular,
        // map of properties for topic
        {{"prop-for-topic", "maybe-store-access-keys"}}),
      [&w, core](auto& partitions, auto& input) {
          return do_with(
                   input.create_requests(),
                   std::size_t(0),
                   [&w, core, &partitions](auto& creqs, auto& cidx) {
                       wal_create_request c = std::move(creqs[cidx++]);
                       while (c.runner_core != core) {
                           c = std::move(creqs[cidx++]);
                       }
                       LOG_INFO(
                         "CREATE:: ({}) - runner core: {}, and core: {}",
                         creqs.size(),
                         c.runner_core,
                         core);
                       std::copy(
                         c.partition_assignments.begin(),
                         c.partition_assignments.end(),
                         std::back_inserter(partitions));
                       return w.create(std::move(c)).then([core](auto r) {
                           LOG_INFO("Topic created on core: {}", core);
                           return make_ready_future<>();
                       });
                   })
            .then([&partitions] {
                return wal_cold_boot::filesystem_lcore_index(".").then(
                  [&](auto cold) {
                      for (auto& kv : cold.fsidx) {
                          for (auto& tps : kv.second) {
                              std::vector<int32_t> tps_vec(
                                tps.second.begin(), tps.second.end());
                              LOG_INFO(
                                "Main Test :: Indexer partitioning :: "
                                "Partitions "
                                "found by indexer: {}, partitions expected: {}",
                                tps_vec,
                                partitions);
                              for (int32_t p : partitions) {
                                  LOG_THROW_IF(
                                    tps.second.find(p) == tps.second.end(),
                                    "Could not find partition: {}, on index: "
                                    "{}",
                                    p,
                                    partitions);
                              }
                          }
                      }
                  });
            });
      });
}

int main(int args, char** argv, char** env) {
    std::cout.setf(std::ios::unitbuf);
    app_template app;
    distributed<write_ahead_log> w;
    config::configuration cfg;
    cfg.data_directory(".");
    cfg.writer_flush_period_ms(2);
    return app.run(args, argv, [&] {
        smf::app_run_log_level(log_level::trace);
        engine().at_exit([&] { return w.stop(); });
        return w.start(wal_opts(cfg))
          .then([&] { return w.invoke_on_all(&write_ahead_log::open); })
          .then([&] { return w.invoke_on_all(&write_ahead_log::index); })
          .then([&] {
              return w.invoke_on_all([](write_ahead_log& w) {
                  return do_creates(engine().cpu_id(), w);
              });
          })
          .then([&] {
              LOG_INFO("Second Test - Double indexing!!!");
              LOG_INFO("Simulates RE-opening a folder after a crash.");
              LOG_INFO("The main test is directory walking - on ALL cores");
              return w.invoke_on_all(&write_ahead_log::index);
          })
          .then([] { return make_ready_future<int>(0); });
    });
}

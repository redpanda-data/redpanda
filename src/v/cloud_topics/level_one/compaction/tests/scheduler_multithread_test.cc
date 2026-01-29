/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_topics/level_one/compaction/tests/scheduler_fixture.h"
#include "container/chunked_hash_map.h"
#include "model/fundamental.h"
#include "random/generators.h"
#include "test_utils/scoped_config.h"
#include "test_utils/test.h"

#include <gtest/gtest.h>

using namespace cloud_topics;
using namespace std::chrono_literals;

// This test stresses concurrent addition and removal of ntps to the
// compaction_scheduler, as well as pausing and resuming of workers across
// multiple shards.
// TODO: combine this with TestScheduler when simple_metastore is usable in a
// multi-shard context OR when replicated_metastore is more practical in a
// fixture test?
TEST_F(SchedulerTestFixture, TestSchedulerMultithread) {
#ifdef NDEBUG
    int num_rounds = 100;
#else
    int num_rounds = 50;
#endif

    scoped_config cfg;
    cfg.get("cloud_topics_compaction_interval_ms").set_value(100ms);
    ss::abort_source as;
    chunked_hash_set<ss::shard_id> paused_workers;
    chunked_hash_map<model::ntp, model::topic_id_partition> managed_ntps;
    chunked_hash_map<model::topic_id_partition, model::offset> tidp_offsets;
    auto manage_partition_func =
      [this, &managed_ntps](model::ntp ntp, model::topic_id_partition tidp) {
          if (managed_ntps.contains(ntp)) {
              return;
          }

          static constexpr size_t max_managed_ntps = 50;
          if (managed_ntps.size() >= max_managed_ntps) {
              return;
          }
          managed_ntps.emplace(ntp, tidp);
          scheduler->manage_partition(ntp, tidp, "manage_partition_func");
      };
    auto unmanage_random_partition_func = [this, &managed_ntps]() {
        if (managed_ntps.empty()) {
            return;
        }
        auto ntp_to_remove
          = random_generators::random_choice(
              std::vector<std::pair<model::ntp, model::topic_id_partition>>(
                managed_ntps.begin(), managed_ntps.end()))
              .first;
        managed_ntps.erase(ntp_to_remove);
        scheduler->unmanage_partition(ntp_to_remove, "unmanage_partition_func");
    };
    auto pause_random_worker_func = [this, &paused_workers]() {
        auto random_shard = random_generators::get_int(ss::smp::count - 1);
        // Inserting _before_ the pause_worker() future resolves can help expose
        // deadlocks.
        paused_workers.insert(random_shard);
        return pause_worker(random_shard);
    };
    auto resume_random_worker_func = [this, &paused_workers]() {
        if (paused_workers.empty()) {
            return ss::now();
        }
        auto worker_to_resume = random_generators::random_choice(
          std::vector<ss::shard_id>(
            paused_workers.begin(), paused_workers.end()));
        paused_workers.erase(worker_to_resume);
        return resume_worker(worker_to_resume);
    };

    static constexpr auto abort_sleep = 200ms;
    auto abort_fut = ss::do_until(
                       [&] { return num_rounds == 0; },
                       [&]() {
                           --num_rounds;
                           return ss::sleep(abort_sleep);
                       })
                       .then([&]() {
                           as.request_abort();
                           return ss::now();
                       });

    static constexpr auto manage_sleep = 50ms;
    auto manage_ntp_fut = ss::do_until(
      [&] { return as.abort_requested(); },
      [&]() {
          auto [ntp, tidp] = make_ntidp(
            random_generators::gen_alphanum_string(8));
          manage_partition_func(std::move(ntp), tidp);
          return ss::sleep(manage_sleep);
      });

    static constexpr auto unmanage_sleep = 75ms;
    auto unmanage_ntp_fut = ss::do_until(
      [&] { return as.abort_requested(); },
      [&]() {
          unmanage_random_partition_func();
          return ss::sleep(unmanage_sleep);
      });

    static constexpr auto pause_worker_sleep = 50ms;
    auto pause_worker_fut = ss::do_until(
      [&] { return as.abort_requested(); },
      [&]() {
          return pause_random_worker_func().then(
            [&]() { return ss::sleep(pause_worker_sleep); });
      });

    static constexpr auto resume_worker_sleep = 75ms;
    auto resume_worker_fut = ss::do_until(
      [&] { return as.abort_requested(); },
      [&]() {
          return resume_random_worker_func().then(
            [&]() { return ss::sleep(resume_worker_sleep); });
      });

    ss::when_all(
      std::move(abort_fut),
      std::move(manage_ntp_fut),
      std::move(unmanage_ntp_fut),
      std::move(pause_worker_fut),
      std::move(resume_worker_fut))
      .get();
}

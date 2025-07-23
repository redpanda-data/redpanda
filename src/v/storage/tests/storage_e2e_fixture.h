// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "kafka/server/tests/produce_consume_utils.h"
#include "redpanda/tests/fixture.h"
#include "storage/disk_log_impl.h"
#include "storage/segment.h"
#include "test_utils/scoped_config.h"

#include <seastar/core/future.hh>
#include <seastar/core/shared_ptr.hh>

struct storage_e2e_fixture : public redpanda_thread_fixture {
    scoped_config test_local_cfg;

    // Produces to the given fixture's partition for 10 seconds.
    ss::future<> produce_to_fixture(model::topic topic_name, int* incomplete) {
        tests::kafka_produce_transport producer(co_await make_kafka_client());
        co_await producer.start();
        const int cardinality = 10;
        auto now = ss::lowres_clock::now();
        while (ss::lowres_clock::now() < now + 5s) {
            for (int i = 0; i < cardinality; i++) {
                co_await producer.produce_to_partition(
                  topic_name,
                  model::partition_id(0),
                  tests::kv_t::sequence(i, 1));
            }
        }
        *incomplete -= 1;
    }

    ss::future<> remove_segment_permanently(
      storage::disk_log_impl* log, ss::lw_shared_ptr<storage::segment> seg) {
        return log->remove_segment_permanently(seg, "storage_e2e_fixture")
          .then([&, log, seg]() {
              auto& segs = log->segments();
              auto it = std::find(segs.begin(), segs.end(), seg);
              if (it == segs.end()) {
                  return;
              }
              segs.erase(it, std::next(it));
          });
    }
};

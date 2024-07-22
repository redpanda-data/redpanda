/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "archival/archiver_operations_impl.h"
#include "archival/tests/service_fixture.h"
#include "bytes/iobuf.h"
#include "bytes/iostream.h"
#include "cloud_storage/fwd.h"
#include "config/configuration.h"
#include "config/property.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "model/namespace.h"
#include "model/record_batch_types.h"
#include "net/types.h"
#include "ssx/sformat.h"
#include "storage/disk_log_impl.h"
#include "storage/parser.h"
#include "storage/storage_resources.h"
#include "storage/tests/utils/disk_log_builder.h"
#include "test_utils/archival.h"
#include "test_utils/fixture.h"
#include "test_utils/scoped_config.h"
#include "utils/retry_chain_node.h"

#include <seastar/core/future-util.hh>
#include <seastar/core/sstring.hh>
#include <seastar/testing/thread_test_case.hh>

#include <boost/test/tools/old/interface.hpp>

using namespace std::chrono_literals;
using namespace archival;

static const auto expected_namespace = model::kafka_namespace;  // NOLINT
static const auto expected_topic = model::topic("test-topic");  // NOLINT
static const auto expected_partition = model::partition_id(42); // NOLINT
static const auto expected_ntp = model::ntp(                    // NOLINT
  expected_namespace,
  expected_topic,
  expected_partition);

inline ss::logger test_log("test-arch-impl"); // NOLINT

FIXTURE_TEST(test_archiver_operations_impl_upload_builder_1, archiver_fixture) {
    // The upload candidates should be aligned with term boundaries
    std::vector<segment_desc> segments = {
      {.ntp = expected_ntp,
       .base_offset = model::offset(0),
       .term = model::term_id(1),
       .num_records = 1000},
      {.ntp = expected_ntp,
       .base_offset = model::offset(1000),
       .term = model::term_id(2),
       .num_records = 1000},
    };
    init_storage_api_local(segments);
    vlog(test_log.info, "Initialized, start waiting for partition leadership");

    wait_for_partition_leadership(expected_ntp);

    auto part = app.partition_manager.local().get(expected_ntp);

    tests::cooperative_spin_wait_with_timeout(10s, [part]() mutable {
        return part->last_stable_offset() >= model::offset(1000);
    }).get();

    auto upload_builder
      = archival::detail::make_segment_upload_builder_wrapper();
    auto partition = archival::detail::make_cluster_partition_wrapper(part);
    auto min_size = 0x1000000;
    auto max_size = 0x10000000;
    size_limited_offset_range range(model::offset(0), max_size, min_size);
    auto type = archival::detail::upload_candidate_type::initial;

    auto upl_candidates = upload_builder
                            ->prepare_segment_upload(
                              partition,
                              range,
                              type,
                              0x8000,
                              ss::default_scheduling_group(),
                              model::timeout_clock::now() + 10s)
                            .get();

    BOOST_REQUIRE(upl_candidates.has_value());
    BOOST_REQUIRE(upl_candidates.value()->is_compacted == false);
    // We need to stop collecting data on the term's boundary even if the
    // segment is too small for normal upload.
    BOOST_REQUIRE(upl_candidates.value()->size_bytes < min_size);
    BOOST_REQUIRE(upl_candidates.value()->offsets.base == model::offset(0));
    BOOST_REQUIRE(upl_candidates.value()->offsets.last == model::offset(999));

    upl_candidates.value()->payload.close().get();
}

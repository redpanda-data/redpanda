/*
 * Copyright 2022 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "bytes/iobuf.h"
#include "bytes/iobuf_parser.h"
#include "bytes/iostream.h"
#include "cloud_storage/partition_manifest.h"
#include "cloud_storage/tx_range_manifest.h"
#include "cloud_storage/types.h"
#include "cluster/types.h"
#include "model/compression.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "model/record.h"
#include "seastarx.h"

#include <seastar/testing/test_case.hh>
#include <seastar/testing/thread_test_case.hh>

#include <boost/test/tools/old/interface.hpp>
#include <boost/test/unit_test.hpp>

#include <chrono>
#include <string>
#include <string_view>
#include <system_error>

using namespace cloud_storage;

static remote_segment_path
  segment_path("abcdef01/kafka/topic/0_1/0-1-v1.log.1");
static remote_manifest_path
  manifest_path("abcdef01/kafka/topic/0_1/0-1-v1.log.1.tx");

using tx_range_t = model::tx_range;

static std::vector<tx_range_t> ranges = {
  tx_range_t{
    .pid = model::producer_identity(1, 2),
    .first = model::offset(3),
    .last = model::offset(5),
  },
  tx_range_t{
    .pid = model::producer_identity(2, 3),
    .first = model::offset(4),
    .last = model::offset(6),
  }};

SEASTAR_THREAD_TEST_CASE(manifest_type_tx) {
    tx_range_manifest m(segment_path);
    BOOST_REQUIRE(m.get_manifest_type() == manifest_type::tx_range);
}

SEASTAR_THREAD_TEST_CASE(create_tx_manifest) {
    tx_range_manifest m(segment_path);
    auto path = m.get_manifest_path();
    BOOST_REQUIRE_EQUAL(path, manifest_path);
}

SEASTAR_THREAD_TEST_CASE(empty_serialization_roundtrip_test) {
    tx_range_manifest m(segment_path);
    auto [is, size] = m.serialize().get();
    iobuf buf;
    auto os = make_iobuf_ref_output_stream(buf);
    ss::copy(is, os).get();

    auto rstr = make_iobuf_input_stream(std::move(buf));
    tx_range_manifest restored(segment_path);
    restored.update(std::move(rstr)).get();
    BOOST_REQUIRE(m == restored);
}

SEASTAR_THREAD_TEST_CASE(serialization_roundtrip_test) {
    fragmented_vector<tx_range_t> tx_ranges;
    tx_ranges = ranges;
    tx_range_manifest m(segment_path, std::move(tx_ranges));
    auto [is, size] = m.serialize().get();
    iobuf buf;
    auto os = make_iobuf_ref_output_stream(buf);
    ss::copy(is, os).get();

    auto rstr = make_iobuf_input_stream(std::move(buf));
    tx_range_manifest restored(segment_path);
    restored.update(std::move(rstr)).get();
    BOOST_REQUIRE(m == restored);
}

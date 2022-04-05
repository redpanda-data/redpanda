// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "hashing/crc32c.h"
#include "hashing/fnv.h"
#include "hashing/twang.h"
#include "hashing/xx.h"
#include "random/generators.h"

#include <seastar/core/reactor.hh>
#include <seastar/testing/perf_tests.hh>

#include <boost/crc.hpp>

static constexpr size_t step_bytes = 57;

PERF_TEST(boost_crc16_fn, header_hash) {
    auto buffer = random_generators::gen_alphanum_string(step_bytes);
    perf_tests::start_measuring_time();
    boost::crc_16_type crc;
    crc.process_bytes(buffer.data(), buffer.size());
    auto o = crc.checksum();
    perf_tests::do_not_optimize(o);
    perf_tests::stop_measuring_time();
}

PERF_TEST(boost_ccitt16_fn, header_hash) {
    auto buffer = random_generators::gen_alphanum_string(step_bytes);
    perf_tests::start_measuring_time();
    boost::crc_ccitt_type crc;
    crc.process_bytes(buffer.data(), buffer.size());
    auto o = crc.checksum();
    perf_tests::do_not_optimize(o);
    perf_tests::stop_measuring_time();
}

PERF_TEST(fnv32_fn, header_hash) {
    auto buffer = random_generators::gen_alphanum_string(step_bytes);
    perf_tests::start_measuring_time();
    auto o = fnv32_buf(buffer.data(), buffer.size());
    perf_tests::do_not_optimize(o);
    perf_tests::stop_measuring_time();
}
PERF_TEST(crc32_fn, header_hash) {
    auto buffer = random_generators::gen_alphanum_string(step_bytes);
    crc::crc32c crc;
    perf_tests::start_measuring_time();
    crc.extend(buffer.data(), buffer.size());
    auto o = crc.value();
    perf_tests::do_not_optimize(o);
    perf_tests::stop_measuring_time();
}

PERF_TEST(xx32_fn, header_hash) {
    auto buffer = random_generators::gen_alphanum_string(step_bytes);
    perf_tests::start_measuring_time();
    auto o = xxhash_32(buffer.data(), buffer.size());
    perf_tests::do_not_optimize(o);
    perf_tests::stop_measuring_time();
}

PERF_TEST(xx64_twang_fn, header_hash) {
    auto buffer = random_generators::gen_alphanum_string(step_bytes);
    perf_tests::start_measuring_time();
    auto o = twang_32from64(xxhash_64(buffer.data(), buffer.size()));
    perf_tests::do_not_optimize(o);
    perf_tests::stop_measuring_time();
}

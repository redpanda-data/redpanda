/*
 * Copyright 2020 Vectorized, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/vectorizedio/redpanda/blob/master/licenses/rcl.md
 */

#include "coproc/errc.h"
#include "model/record_batch_reader.h"
#include "model/timeout_clock.h"

#include <seastar/testing/thread_test_case.hh>

#include <boost/test/tools/old/interface.hpp>

#include <variant>
#define BOOST_TEST_MODULE coproc

#include "coproc/tests/utils/wasm_event_generator.h"
#include "random/generators.h"

#include <boost/test/unit_test.hpp>

#include <optional>

using cp_errc = coproc::wasm_event_errc;

SEASTAR_THREAD_TEST_CASE(verify_make_event) {
    /// The generator only creates valid events by default
    auto rbr = coproc::make_wasm_event_record_batch_reader(
      model::offset(0), 5, 5);
    auto batches = model::consume_reader_to_memory(
                     std::move(rbr), model::no_timeout)
                     .get0();
    for (auto& record_batch : batches) {
        record_batch.for_each_record([](model::record r) {
            BOOST_CHECK_EQUAL(cp_errc::none, coproc::wasm_event_validate(r));
        });
    }
}

BOOST_AUTO_TEST_CASE(verify_make_event_failures) {
    {
        /// Empty event
        model::record r = create_wasm_record(coproc::wasm_event{});
        BOOST_CHECK(!coproc::wasm_event_get_name(r));
        BOOST_CHECK_EQUAL(
          cp_errc::empty_mandatory_field, coproc::wasm_event_validate(r));
    }
    {
        /// Missing 'script' field
        model::record r = create_wasm_record(coproc::wasm_event{
          .name = random_generators::gen_alphanum_string(15),
          .checksum = random_generators::get_bytes(32),
          .action = coproc::wasm_event_action::deploy});
        BOOST_CHECK_EQUAL(
          cp_errc::empty_mandatory_field,
          coproc::wasm_event_verify_checksum(r));
        BOOST_CHECK_EQUAL(
          cp_errc::empty_mandatory_field, coproc::wasm_event_validate(r));
    }
    {
        /// Erroneous checksum
        model::record r = create_wasm_record(coproc::wasm_event{
          .name = random_generators::gen_alphanum_string(15),
          .desc = random_generators::gen_alphanum_string(15),
          .script = random_generators::gen_alphanum_string(15),
          .checksum = random_generators::get_bytes(32),
          .action = coproc::wasm_event_action::deploy});
        BOOST_CHECK_EQUAL(
          cp_errc::mismatched_checksum, coproc::wasm_event_verify_checksum(r));
    }
}

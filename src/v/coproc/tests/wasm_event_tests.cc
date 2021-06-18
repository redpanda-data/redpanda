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
#include "coproc/tests/utils/coprocessor.h"
#include "model/record_batch_reader.h"
#include "model/timeout_clock.h"

#include <seastar/testing/thread_test_case.hh>

#include <boost/test/tools/old/interface.hpp>

#include <variant>
#define BOOST_TEST_MODULE coproc

#include "coproc/tests/utils/wasm_event_generator.h"
#include "random/generators.h"
#include "storage/parser_utils.h"

#include <boost/test/unit_test.hpp>

#include <optional>

using cp_errc = coproc::wasm::errc;

SEASTAR_THREAD_TEST_CASE(verify_make_event) {
    /// The generator only creates valid events by default
    auto rbr = coproc::wasm::make_random_event_record_batch_reader(
                 model::offset(0), 5, 5)
                 .for_each_ref(
                   storage::internal::decompress_batch_consumer(),
                   model::no_timeout)
                 .get0();
    auto batches = model::consume_reader_to_memory(
                     std::move(rbr), model::no_timeout)
                     .get0();
    for (auto& record_batch : batches) {
        record_batch.for_each_record([](model::record r) {
            BOOST_CHECK_EQUAL(cp_errc::none, coproc::wasm::validate_event(r));
        });
    }
}

BOOST_AUTO_TEST_CASE(verify_make_event_failures) {
    {
        /// Empty event
        model::record r = coproc::wasm::make_record(coproc::wasm::event{});
        BOOST_CHECK(!coproc::wasm::get_event_id(r));
        BOOST_CHECK_EQUAL(
          cp_errc::empty_mandatory_field, coproc::wasm::validate_event(r));
    }
    {
        /// Missing 'script' field
        coproc::wasm::event e;
        e.id = random_generators::get_int<uint64_t>(53339);
        e.checksum = random_generators::get_bytes(32);
        e.action = coproc::wasm::event_action::deploy;
        model::record r = coproc::wasm::make_record(e);
        BOOST_CHECK_EQUAL(
          cp_errc::empty_mandatory_field,
          coproc::wasm::verify_event_checksum(r));
        BOOST_CHECK_EQUAL(
          cp_errc::empty_mandatory_field, coproc::wasm::validate_event(r));
    }
    {
        /// Erroneous checksum
        coproc::wasm::event e;
        e.id = random_generators::get_int<uint64_t>(55555);
        e.desc = random_generators::get_bytes(64);
        e.script = random_generators::get_bytes(15);
        e.name = random_generators::get_bytes(15);
        e.checksum = random_generators::get_bytes(32);
        e.action = coproc::wasm::event_action::deploy;
        model::record r = coproc::wasm::make_record(e);
        BOOST_CHECK_EQUAL(
          cp_errc::mismatched_checksum, coproc::wasm::verify_event_checksum(r));
    }
}

SEASTAR_THREAD_TEST_CASE(verify_event_reconciliation) {
    const auto deploy = coproc::wasm::cpp_enable_payload{
      .tid = coproc::registry::type_identifier::identity_coprocessor,
      .topics = {std::make_pair<>(
        model::topic("ABC"), coproc::topic_ingestion_policy::earliest)}};
    std::vector<std::vector<coproc::wasm::event>> events{
      {{123, deploy}, {456, deploy}, {123}, {456, deploy}},
      {{789, deploy}, {123}}};

    auto rbr = make_event_record_batch_reader(std::move(events))
                 .for_each_ref(
                   storage::internal::decompress_batch_consumer(),
                   model::no_timeout)
                 .get0();
    auto batches = model::consume_reader_to_memory(
                     std::move(rbr), model::no_timeout)
                     .get0();

    auto results = coproc::wasm::reconcile_events(
      std::vector<model::record_batch>(
        std::make_move_iterator(batches.begin()),
        std::make_move_iterator(batches.end())));
    BOOST_CHECK_EQUAL(results.size(), 3);
    BOOST_CHECK(results.find(coproc::script_id(123))
                  ->second.source_code.empty()); /// 'remove' event
    BOOST_CHECK(results.find(coproc::script_id(123)) != results.end());
    BOOST_CHECK(results.find(coproc::script_id(456)) != results.end());
    BOOST_CHECK(results.find(coproc::script_id(789)) != results.end());
}

/*
 * Copyright 2020 Vectorized, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
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
            auto validate_res = coproc::wasm::validate_event(r);
            BOOST_CHECK(validate_res.has_value());
        });
    }
}

BOOST_AUTO_TEST_CASE(verify_make_event_failures) {
    {
        /// Empty event
        model::record r = coproc::wasm::make_record(coproc::wasm::event{});
        BOOST_CHECK(!coproc::wasm::get_event_id(r));
        auto validate_res = coproc::wasm::validate_event(r);
        BOOST_CHECK_EQUAL(cp_errc::empty_mandatory_field, validate_res.error());
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
        auto validate_res = coproc::wasm::validate_event(r);
        BOOST_CHECK_EQUAL(cp_errc::empty_mandatory_field, validate_res.error());
    }
    {
        /// Erroneous checksum
        coproc::wasm::event e;
        e.id = random_generators::get_int<uint64_t>(55555);
        e.desc = random_generators::get_bytes(64);
        e.script = random_generators::get_bytes(15);
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
      {{123, deploy},
       {456, deploy},
       {123},
       {456, deploy},
       {666, deploy, coproc::wasm::event_type::data_policy},
       {333, deploy, coproc::wasm::event_type::data_policy}},
      {{789, deploy},
       {123},
       {333, deploy, coproc::wasm::event_type::data_policy},
       {888, deploy, coproc::wasm::event_type::data_policy},
       {666, coproc::wasm::event_type::data_policy}}};

    auto rbr = make_event_record_batch_reader(std::move(events))
                 .for_each_ref(
                   storage::internal::decompress_batch_consumer(),
                   model::no_timeout)
                 .get0();
    auto batches = model::consume_reader_to_memory(
                     std::move(rbr), model::no_timeout)
                     .get0();

    auto results = coproc::wasm::reconcile_events_by_type(
      std::vector<model::record_batch>(
        std::make_move_iterator(batches.begin()),
        std::make_move_iterator(batches.end())));
    BOOST_CHECK_EQUAL(results.size(), 2);
    auto& res_async = results[coproc::wasm::event_type::async];
    BOOST_CHECK_EQUAL(res_async.size(), 3);
    BOOST_CHECK(
      res_async.find(coproc::script_id(123))->second.header.action
      == coproc::wasm::event_action::remove);
    BOOST_CHECK(res_async.find(coproc::script_id(123)) != res_async.end());
    BOOST_CHECK(res_async.find(coproc::script_id(456)) != res_async.end());
    BOOST_CHECK(res_async.find(coproc::script_id(789)) != res_async.end());
    BOOST_CHECK(res_async.find(coproc::script_id(333)) == res_async.end());

    auto& res_data_policy = results[coproc::wasm::event_type::data_policy];
    BOOST_CHECK_EQUAL(res_data_policy.size(), 3);
    BOOST_CHECK(
      res_data_policy.find(coproc::script_id(666))->second.header.action
      == coproc::wasm::event_action::remove);
    BOOST_CHECK(
      res_data_policy.find(coproc::script_id(888)) != res_data_policy.end());
    BOOST_CHECK(
      res_data_policy.find(coproc::script_id(333)) != res_data_policy.end());
}

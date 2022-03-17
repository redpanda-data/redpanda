/*
 * Copyright 2020 Vectorized, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "coproc/tests/fixtures/coproc_test_fixture.h"
#include "coproc/tests/utils/coprocessor.h"
#include "coproc/tests/utils/event_publisher_utils.h"
#include "coproc/tests/utils/wasm_event_generator.h"
#include "coproc/wasm_event.h"
#include "model/record_batch_reader.h"
#include "test_utils/async.h"

#include <boost/test/tools/old/interface.hpp>
#include <boost/test/unit_test_log.hpp>

FIXTURE_TEST(test_copro_internal_topic_do_undo, coproc_test_fixture) {
    model::topic input("input_topic");
    coproc::wasm::cpp_enable_payload deploy{
      .tid = coproc::registry::type_identifier::identity_coprocessor,
      .topics = {{input, coproc::topic_ingestion_policy::stored}}};
    setup({{input, 1}}).get();
    std::vector<std::vector<coproc::wasm::event>> events{
      {{444, deploy},
       {444, deploy},
       {444},
       {444, deploy},
       {444},
       {123, deploy}},
      {{444}, {444, deploy}, {123, deploy}}};

    auto rbr = make_event_record_batch_reader(std::move(events));

    /// Push and assert
    auto rset
      = coproc::wasm::publish_events(get_client(), std::move(rbr)).get0();
    BOOST_CHECK_EQUAL(rset.size(), 2);

    tests::cooperative_spin_wait_with_timeout(5s, [&] {
        return total_registered() == 2;
    }).get();
}

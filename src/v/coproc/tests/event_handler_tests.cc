/*
 * Copyright 2021 Vectorized, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "coproc/event_handler.h"
#include "coproc/wasm_event.h"
#include "hashing/xx.h"
#include "seastarx.h"

#include <seastar/core/temporary_buffer.hh>
#include <seastar/testing/thread_test_case.hh>

#include <absl/container/btree_map.h>

#include <optional>

SEASTAR_THREAD_TEST_CASE(data_policy_handler_test) {
    coproc::wasm::data_policy_event_handler handler;
    handler.start().get();

    ss::sstring name1 = "foo";
    coproc::wasm::parsed_event event1;
    event1.header.action = coproc::wasm::event_action::deploy;
    event1.header.type = coproc::wasm::event_type::data_policy;
    ss::temporary_buffer<char> data(name1.c_str(), name1.size());
    event1.data.append(std::move(data));
    auto script_id1 = xxhash_64(name1.c_str(), name1.size());

    absl::btree_map<coproc::script_id, coproc::wasm::parsed_event> events1;
    events1.emplace(script_id1, std::move(event1));

    handler.process(std::move(events1)).get();

    auto code = handler.get_code(name1);
    auto raw_value
      = iobuf_const_parser(code.value()).read_string(code->size_bytes());
    BOOST_CHECK_EQUAL(raw_value, name1);

    coproc::wasm::parsed_event event2;
    event2.header.action = coproc::wasm::event_action::remove;
    event2.header.type = coproc::wasm::event_type::data_policy;

    absl::btree_map<coproc::script_id, coproc::wasm::parsed_event> events2;
    events2.emplace(script_id1, std::move(event2));

    handler.process(std::move(events2)).get();

    code = handler.get_code(name1);

    BOOST_CHECK(code == std::nullopt);

    handler.stop().get();
}

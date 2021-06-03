/*
 * Copyright 2021 Vectorized, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/vectorizedio/redpanda/blob/master/licenses/rcl.md
 */

#include "coproc/tests/fixtures/coproc_slim_fixture.h"
#include "coproc/tests/fixtures/supervisor_test_fixture.h"
#include "coproc/types.h"

#include <vector>

class script_dispatcher_fixture
  : public coproc_slim_fixture
  , public supervisor_test_fixture {
public:
    ss::future<result<std::vector<coproc::script_id>>> enable_scripts(
      std::vector<std::tuple<uint64_t, coprocessor::input_set>> data) {
        std::vector<coproc::enable_copros_request::data> d;
        std::transform(
          data.begin(),
          data.end(),
          std::back_inserter(d),
          [](std::tuple<uint64_t, coprocessor::input_set>& set) {
              /// Generate a random coprocessor payload
              coproc::wasm::cpp_enable_payload payload{
                .tid = coproc::registry::type_identifier::identity_coprocessor,
                .topics = std::move(std::get<1>(set))};
              return coproc::enable_copros_request::data{
                .id = coproc::script_id(std::get<0>(set)),
                .source_code = reflection::to_iobuf(std::move(payload))};
          });
        return get_script_dispatcher()->enable_coprocessors(
          coproc::enable_copros_request{.inputs = std::move(d)});
    }

    ss::future<result<std::vector<coproc::script_id>>>
    disable_scripts(std::vector<coproc::script_id> ids) {
        return get_script_dispatcher()->disable_coprocessors(
          coproc::disable_copros_request{.ids = std::move(ids)});
    }

    ss::future<size_t> scripts_across_shards(coproc::script_id id) {
        return get_pacemaker().map_reduce0(
          [id](coproc::pacemaker& p) {
              return p.local_script_id_exists(id) ? 1 : 0;
          },
          size_t(0),
          std::plus<>());
    }
};

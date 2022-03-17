/*
 * Copyright 2021 Vectorized, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "v8_engine/data_policy.h"

namespace v8_engine {

std::ostream& operator<<(std::ostream& os, const data_policy& datapolicy) {
    fmt::print(
      os,
      "function_name: {} script_name: {}",
      datapolicy._function_name,
      datapolicy._script_name);
    return os;
}

} // namespace v8_engine

namespace reflection {

void reflection::adl<v8_engine::data_policy>::to(
  iobuf& out, v8_engine::data_policy&& dp) {
    reflection::serialize(
      out, dp.version, dp.function_name(), dp.script_name());
}

v8_engine::data_policy
reflection::adl<v8_engine::data_policy>::from(iobuf_parser& in) {
    auto version = reflection::adl<int8_t>{}.from(in);
    vassert(
      version == v8_engine::data_policy::version,
      "Unexpected data_policy version");
    auto function_name = reflection::adl<ss::sstring>{}.from(in);
    auto script_name = reflection::adl<ss::sstring>{}.from(in);
    return v8_engine::data_policy(
      std::move(function_name), std::move(script_name));
}

} // namespace reflection

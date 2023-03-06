/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "protocol_utils.h"

#include <boost/process.hpp>

namespace kafka {

bytes invoke_franz_harness(
  kafka::api_key key, kafka::api_version v, is_kafka_request is_request) {
    static const boost::filesystem::path generator_path = []() {
        const char* gen_cstr = std::getenv("GENERATOR_BIN");
        vassert(gen_cstr, "Missing generator binary path in test env");
        boost::filesystem::path p(gen_cstr);
        vassert(
          boost::filesystem::exists(p),
          "Harness error, provided GENERATOR_BIND not found: {}",
          p);
        return p;
    }();

    ss::sstring stdout;
    {
        boost::process::ipstream is;
        boost::process::child c(
          generator_path.string(),
          boost::process::args({
            "-api",
            std::to_string(key()),
            "-version",
            std::to_string(v()),
            (is_request == is_kafka_request::yes ? "-is-request=true"
                                                 : "-is-request=false"),
          }),
          boost::process::std_out > is);

        c.wait();
        vassert(
          c.exit_code() == 0,
          "Kafka-request-generator exited with non-zero status");

        std::stringstream ss;
        ss << is.rdbuf();
        stdout = ss.str();
    }

    auto result = ss::uninitialized_string<bytes>(stdout.size());
    std::copy_n(stdout.begin(), stdout.size(), result.begin());
    return result;
}
} // namespace kafka

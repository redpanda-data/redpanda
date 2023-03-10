
/*
 * Copyright 2022 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */
#pragma once

#include "cluster/types.h"
#include "compat/check.h"
#include "compat/get_node_health_generator.h"
#include "compat/json.h"

namespace compat {

GEN_COMPAT_CHECK_SERDE_ONLY(
  cluster::get_node_health_request,
  { json_write(filter); },
  { json_read(filter); });

template<>
struct compat_check<cluster::get_node_health_reply> {
    static constexpr std::string_view name = "cluster::get_node_health_reply";

    static std::vector<cluster::get_node_health_reply> create_test_cases() {
        return generate_instances<cluster::get_node_health_reply>();
    }

    static void to_json(
      cluster::get_node_health_reply obj,
      json::Writer<json::StringBuffer>& wr) {
        json_write(error);
        json_write(report);
    }

    static cluster::get_node_health_reply from_json(json::Value& rd) {
        cluster::get_node_health_reply obj;
        json_read(error);
        json_read(report);
        return obj;
    }

    static std::vector<compat_binary>
    to_binary(cluster::get_node_health_reply obj) {
        return {compat_binary::serde(obj)};
    }

    static void
    check(cluster::get_node_health_reply expected, compat_binary test) {
        const auto name = test.name;
        auto decoded = decode_serde_only<cluster::get_node_health_reply>(
          std::move(test));

        /*
         * adl encoding doesn't consider the error so we need to ignore that and
         * only consider the report. serde encodes both error and report.
         */
        vassert(name == "serde" || name == "adl", "unexpected name {}", name);
        const auto equal = name == "serde" ? expected == decoded
                                           : expected.report == decoded.report;
        if (!equal) {
            throw compat_error(fmt::format(
              "Verify of {{{}}} decoding failed:\nExpected: {}\nDecoded: {}",
              name,
              expected,
              decoded));
        }
    }
};

}; // namespace compat

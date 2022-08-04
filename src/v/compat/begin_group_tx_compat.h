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
#include "compat/begin_group_tx_generator.h"
#include "compat/check.h"
#include "compat/json.h"

namespace compat {

/*
 * cluster::begin_group_tx_request
 */
template<>
struct compat_check<cluster::begin_group_tx_request> {
    static constexpr std::string_view name = "cluster::begin_group_tx_request";

    static std::vector<cluster::begin_group_tx_request> create_test_cases() {
        return generate_instances<cluster::begin_group_tx_request>();
    }

    static void to_json(
      cluster::begin_group_tx_request obj,
      json::Writer<json::StringBuffer>& wr) {
        json_write(ntp);
        json_write(group_id);
        json_write(pid);
        json_write(tx_seq);
        json_write(timeout);
    }

    static cluster::begin_group_tx_request from_json(json::Value& rd) {
        cluster::begin_group_tx_request obj;
        json_read(ntp);
        json_read(group_id);
        json_read(pid);
        json_read(tx_seq);
        json_read(timeout);
        return obj;
    }

    static std::vector<compat_binary>
    to_binary(cluster::begin_group_tx_request obj) {
        return compat_binary::serde_and_adl(obj);
    }

    static bool check(cluster::begin_group_tx_request obj, compat_binary test) {
        return verify_adl_or_serde(obj, std::move(test));
    }
};

/*
 * cluster::begin_group_tx_reply
 */
template<>
struct compat_check<cluster::begin_group_tx_reply> {
    static constexpr std::string_view name = "cluster::begin_group_tx_reply";
    static std::vector<cluster::begin_group_tx_reply> create_test_cases() {
        return generate_instances<cluster::begin_group_tx_reply>();
    }
    static void to_json(
      cluster::begin_group_tx_reply obj, json::Writer<json::StringBuffer>& wr) {
        json_write(etag);
        json_write(ec);
    }
    static cluster::begin_group_tx_reply from_json(json::Value& rd) {
        cluster::begin_group_tx_reply obj;
        json_read(etag);
        json_read(ec);
        return obj;
    }
    static std::vector<compat_binary>
    to_binary(cluster::begin_group_tx_reply obj) {
        return compat_binary::serde_and_adl(obj);
    }
    static bool check(cluster::begin_group_tx_reply obj, compat_binary test) {
        return verify_adl_or_serde(obj, std::move(test));
    }
};

} // namespace compat

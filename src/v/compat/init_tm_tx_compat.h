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
#include "compat/cluster_json.h"
#include "compat/init_tm_tx_generator.h"
#include "compat/model_json.h"
#include "test_utils/randoms.h"

#include <vector>

namespace compat {

/*
 * cluster::init_tm_tx_request
 */
template<>
struct compat_check<cluster::init_tm_tx_request> {
    static constexpr std::string_view name = "cluster::init_tm_tx_request";
    static std::vector<cluster::init_tm_tx_request> create_test_cases() {
        return generate_instances<cluster::init_tm_tx_request>();
    }
    static void to_json(
      cluster::init_tm_tx_request obj, json::Writer<json::StringBuffer>& wr) {
        json_write(tx_id);
        json_write(transaction_timeout_ms);
        json_write(timeout);
    }
    static cluster::init_tm_tx_request from_json(json::Value& rd) {
        cluster::init_tm_tx_request obj;
        json_read(tx_id);
        json_read(transaction_timeout_ms);
        json_read(timeout);
        return obj;
    }
    static std::vector<compat_binary>
    to_binary(cluster::init_tm_tx_request obj) {
        return {compat_binary::serde(obj)};
    }
    static void check(cluster::init_tm_tx_request obj, compat_binary test) {
        verify_serde_only(obj, std::move(test));
    }
};

/*
 * cluster::init_tm_tx_reply
 */
template<>
struct compat_check<cluster::init_tm_tx_reply> {
    static constexpr std::string_view name = "cluster::init_tm_tx_reply";
    static std::vector<cluster::init_tm_tx_reply> create_test_cases() {
        return generate_instances<cluster::init_tm_tx_reply>();
    }
    static void to_json(
      cluster::init_tm_tx_reply obj, json::Writer<json::StringBuffer>& wr) {
        json_write(pid);
        json_write(ec);
    }
    static cluster::init_tm_tx_reply from_json(json::Value& rd) {
        cluster::init_tm_tx_reply obj;
        json_read(pid);
        json_read(ec);
        return obj;
    }
    static std::vector<compat_binary> to_binary(cluster::init_tm_tx_reply obj) {
        return {compat_binary::serde(obj)};
    }
    static void check(cluster::init_tm_tx_reply obj, compat_binary test) {
        verify_serde_only(obj, std::move(test));
    }
};

} // namespace compat

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
#include "compat/model_json.h"
#include "compat/try_abort_generator.h"
#include "test_utils/randoms.h"

#include <vector>

namespace compat {

/*
 * cluster::try_abort_request
 */
template<>
struct compat_check<cluster::try_abort_request> {
    static constexpr std::string_view name = "cluster::try_abort_request";
    static std::vector<cluster::try_abort_request> create_test_cases() {
        return generate_instances<cluster::try_abort_request>();
    }
    static void to_json(
      cluster::try_abort_request obj, json::Writer<json::StringBuffer>& wr) {
        json_write(tm);
        json_write(pid);
        json_write(tx_seq);
        json_write(timeout);
    }
    static cluster::try_abort_request from_json(json::Value& rd) {
        cluster::try_abort_request obj;
        json_read(tm);
        json_read(pid);
        json_read(tx_seq);
        json_read(timeout);
        return obj;
    }
    static std::vector<compat_binary>
    to_binary(cluster::try_abort_request obj) {
        return {compat_binary::serde(obj)};
    }
    static void check(cluster::try_abort_request obj, compat_binary test) {
        verify_serde_only(obj, std::move(test));
    }
};

/*
 * cluster::try_abort_reply
 */
template<>
struct compat_check<cluster::try_abort_reply> {
    static constexpr std::string_view name = "cluster::try_abort_reply";
    static std::vector<cluster::try_abort_reply> create_test_cases() {
        return generate_instances<cluster::try_abort_reply>();
    }
    static void to_json(
      cluster::try_abort_reply obj, json::Writer<json::StringBuffer>& wr) {
        json_write(commited);
        json_write(aborted);
        json_write(ec);
    }
    static cluster::try_abort_reply from_json(json::Value& rd) {
        cluster::try_abort_reply obj;
        json_read(commited);
        json_read(aborted);
        json_read(ec);
        return obj;
    }
    static std::vector<compat_binary> to_binary(cluster::try_abort_reply obj) {
        return {compat_binary::serde(obj)};
    }
    static void check(cluster::try_abort_reply obj, compat_binary test) {
        verify_serde_only(obj, std::move(test));
    }
};

} // namespace compat

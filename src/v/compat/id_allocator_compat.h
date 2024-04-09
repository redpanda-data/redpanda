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

#include "cluster/errc.h"
#include "cluster/types.h"
#include "compat/check.h"
#include "compat/id_allocator_generator.h"
#include "compat/json.h"
#include "model/timeout_clock.h"

#include <cstdint>

namespace compat {

/*
 * cluster::allocate_id_request
 */
template<>
struct compat_check<cluster::allocate_id_request> {
    static constexpr std::string_view name = "cluster::allocate_id_request";

    static std::vector<cluster::allocate_id_request> create_test_cases() {
        return compat::generate_instances<cluster::allocate_id_request>();
    }

    static void to_json(
      cluster::allocate_id_request obj, json::Writer<json::StringBuffer>& wr) {
        json_write(timeout);
    }

    static cluster::allocate_id_request from_json(json::Value& rd) {
        cluster::allocate_id_request obj{};
        json_read(timeout);
        return obj;
    }

    static std::vector<compat_binary>
    to_binary(cluster::allocate_id_request obj) {
        return {compat_binary::serde(obj)};
    }

    static void check(cluster::allocate_id_request obj, compat_binary test) {
        verify_serde_only(obj, std::move(test));
    }
};

/*
 * cluster::allocate_id_reply
 */
template<>
struct compat_check<cluster::allocate_id_reply> {
    static constexpr std::string_view name = "cluster::allocate_id_reply";

    static std::vector<cluster::allocate_id_reply> create_test_cases() {
        return compat::generate_instances<cluster::allocate_id_reply>();
    }

    static void to_json(
      cluster::allocate_id_reply obj, json::Writer<json::StringBuffer>& wr) {
        json_write(id);
        json_write(ec);
    }

    static cluster::allocate_id_reply from_json(json::Value& rd) {
        cluster::allocate_id_reply obj;
        json_read(id);
        json_read(ec);
        return obj;
    }

    static std::vector<compat_binary>
    to_binary(cluster::allocate_id_reply obj) {
        return {compat_binary::serde(obj)};
    }

    static void check(cluster::allocate_id_reply obj, compat_binary test) {
        verify_serde_only(obj, std::move(test));
    }
};

}; // namespace compat

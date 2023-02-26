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

#include "cluster/metadata_dissemination_types.h"
#include "compat/check.h"
#include "compat/metadata_dissemination_generator.h"
#include "compat/metadata_dissemination_json.h"

namespace compat {

/*
 * cluster::update_leadership_request
 */
template<>
struct compat_check<cluster::update_leadership_request> {
    static constexpr std::string_view name
      = "cluster::update_leadership_request";

    static std::vector<cluster::update_leadership_request> create_test_cases() {
        return generate_instances<cluster::update_leadership_request>();
    }

    static void to_json(
      cluster::update_leadership_request obj,
      json::Writer<json::StringBuffer>& wr) {
        json_write(leaders);
    }

    static cluster::update_leadership_request from_json(json::Value& rd) {
        cluster::update_leadership_request obj;
        json_read(leaders);
        return obj;
    }

    static std::vector<compat_binary>
    to_binary(cluster::update_leadership_request obj) {
        return {compat_binary::serde(obj)};
    }

    static void
    check(cluster::update_leadership_request obj, compat_binary test) {
        verify_serde_only(obj, std::move(test));
    }
};

/*
 * cluster::update_leadership_request_v2
 */
template<>
struct compat_check<cluster::update_leadership_request_v2> {
    static constexpr std::string_view name
      = "cluster::update_leadership_request_v2";

    static std::vector<cluster::update_leadership_request_v2>
    create_test_cases() {
        return generate_instances<cluster::update_leadership_request_v2>();
    }

    static void to_json(
      cluster::update_leadership_request_v2 obj,
      json::Writer<json::StringBuffer>& wr) {
        json_write(leaders);
    }

    static cluster::update_leadership_request_v2 from_json(json::Value& rd) {
        cluster::update_leadership_request_v2 obj;
        json_read(leaders);
        return obj;
    }

    static std::vector<compat_binary>
    to_binary(cluster::update_leadership_request_v2 obj) {
        return {compat_binary::serde(obj)};
    }

    static void
    check(cluster::update_leadership_request_v2 obj, compat_binary test) {
        verify_serde_only(obj, std::move(test));
    }
};

/*
 * cluster::update_leadership_reply
 */
EMPTY_COMPAT_CHECK_SERDE_ONLY(cluster::update_leadership_reply);

/*
 * cluster::get_leadership_request
 */
EMPTY_COMPAT_CHECK_SERDE_ONLY(cluster::get_leadership_request);

/*
 * cluster::get_leadership_reply
 */
template<>
struct compat_check<cluster::get_leadership_reply> {
    static constexpr std::string_view name = "cluster::get_leadership_reply";
    static std::vector<cluster::get_leadership_reply> create_test_cases() {
        return generate_instances<cluster::get_leadership_reply>();
    }
    static void to_json(
      cluster::get_leadership_reply obj, json::Writer<json::StringBuffer>& wr) {
        json_write(leaders);
    }
    static cluster::get_leadership_reply from_json(json::Value& rd) {
        cluster::get_leadership_reply obj;
        json_read(leaders);
        return obj;
    }
    static std::vector<compat_binary>
    to_binary(cluster::get_leadership_reply obj) {
        return {compat_binary::serde(obj)};
    }
    static void check(cluster::get_leadership_reply obj, compat_binary test) {
        verify_serde_only(obj, std::move(test));
    }
};

} // namespace compat

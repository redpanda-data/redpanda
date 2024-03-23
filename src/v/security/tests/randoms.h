/*
 * Copyright 2020 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */
#pragma once

#include "bytes/random.h"
#include "random/generators.h"
#include "security/acl.h"
#include "security/scram_credential.h"
#include "test_utils/randoms.h"

namespace tests {

inline security::scram_credential random_credential() {
    return security::scram_credential(
      random_generators::get_bytes(256),
      random_generators::get_bytes(256),
      random_generators::get_bytes(256),
      random_generators::get_int(1, 10));
}

inline security::resource_type random_resource_type() {
    return random_generators::random_choice<security::resource_type>(
      {security::resource_type::cluster,
       security::resource_type::group,
       security::resource_type::topic,
       security::resource_type::transactional_id});
}

inline security::pattern_type random_pattern_type() {
    return random_generators::random_choice<security::pattern_type>(
      {security::pattern_type::literal, security::pattern_type::prefixed});
}

inline security::resource_pattern random_resource_pattern() {
    return {
      random_resource_type(),
      random_generators::gen_alphanum_string(10),
      random_pattern_type()};
}

inline security::acl_principal random_acl_principal() {
    return {
      security::principal_type::user,
      random_generators::gen_alphanum_string(12)};
}

inline security::acl_host create_acl_host() {
    return security::acl_host(ss::net::inet_address("127.0.0.1"));
}

inline security::acl_operation random_acl_operation() {
    return random_generators::random_choice<security::acl_operation>(
      {security::acl_operation::all,
       security::acl_operation::alter,
       security::acl_operation::alter_configs,
       security::acl_operation::describe_configs,
       security::acl_operation::cluster_action,
       security::acl_operation::create,
       security::acl_operation::remove,
       security::acl_operation::read,
       security::acl_operation::idempotent_write,
       security::acl_operation::describe});
}

inline security::acl_permission random_acl_permission() {
    return random_generators::random_choice<security::acl_permission>(
      {security::acl_permission::allow, security::acl_permission::deny});
}

inline security::acl_entry random_acl_entry() {
    return {
      random_acl_principal(),
      create_acl_host(),
      random_acl_operation(),
      random_acl_permission()};
}

inline security::acl_binding random_acl_binding() {
    return {random_resource_pattern(), random_acl_entry()};
}

inline security::resource_pattern_filter random_resource_pattern_filter() {
    auto resource = tests::random_optional(
      [] { return random_resource_type(); });

    auto name = tests::random_optional(
      [] { return random_generators::gen_alphanum_string(14); });

    auto pattern = tests::random_optional([] {
        using ret_t = std::variant<
          security::pattern_type,
          security::resource_pattern_filter::pattern_match>;
        if (tests::random_bool()) {
            return ret_t(random_pattern_type());
        } else {
            return ret_t(security::resource_pattern_filter::pattern_match{});
        }
    });

    return {resource, std::move(name), pattern};
}

inline security::acl_entry_filter random_acl_entry_filter() {
    auto principal = tests::random_optional(
      [] { return random_acl_principal(); });

    auto host = tests::random_optional([] { return create_acl_host(); });

    auto operation = tests::random_optional(
      [] { return random_acl_operation(); });

    auto permission = tests::random_optional(
      [] { return random_acl_permission(); });

    return {std::move(principal), host, operation, permission};
}

inline security::acl_binding_filter random_acl_binding_filter() {
    return {random_resource_pattern_filter(), random_acl_entry_filter()};
}

} // namespace tests

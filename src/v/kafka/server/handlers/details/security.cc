/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "kafka/server/handlers/details/security.h"

#include "security/scram_algorithm.h"

namespace kafka::details {

std::optional<security::scram_algorithm_t>
kafka_to_security_mechanism(kafka::scram_mechanism mechanism) {
    switch (mechanism) {
    case kafka::scram_mechanism::unknown:
        return std::nullopt;
    case kafka::scram_mechanism::scram_sha_256:
        return security::scram_algorithm_t::sha256;
    case kafka::scram_mechanism::scram_sha_512:
        return security::scram_algorithm_t::sha512;
    }
    return std::nullopt;
}

scram_mechanism key_size_to_mechanism(size_t key_size) {
    switch (key_size) {
    case security::scram_sha256::key_size:
        return scram_mechanism::scram_sha_256;
    case security::scram_sha512::key_size:
        return scram_mechanism::scram_sha_512;
    default:
        return scram_mechanism::unknown;
    }
}

describe_acls_resource acl_entry_to_resource(
  security::resource_pattern pattern,
  chunked_vector<security::acl_entry> acl_entries,
  bool describing_registry_resource) {
    describe_acls_resource resource;

    resource.type = describing_registry_resource
                      ? details::to_kafka_registry_resource_type(
                          pattern.resource())
                      : details::to_kafka_resource_type(pattern.resource());
    resource.name = pattern.name();
    resource.pattern_type = details::to_kafka_pattern_type(pattern.pattern());
    resource.registry_resource = describing_registry_resource;

    // acl entries
    for (auto& acl : acl_entries) {
        // ignore ephemeral_users
        auto ephemeral_user = security::principal_type::ephemeral_user;
        if (acl.principal().type() == ephemeral_user) {
            continue;
        }
        acl_description desc{
          .principal = details::to_kafka_principal(acl.principal()),
          .host = details::to_kafka_host(acl.host()),
          .operation = details::to_kafka_operation(acl.operation()),
          .permission_type = details::to_kafka_permission(acl.permission()),
        };
        resource.acls.push_back(std::move(desc));
    }

    return resource;
}
} // namespace kafka::details

/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "cluster/client_quota_types.h"

namespace cluster::client_quota {

std::ostream& operator<<(std::ostream& os, rule r) {
    switch (r) {
    case rule::not_applicable:
        return os << "not_applicable";
    case rule::kafka_client_default:
        return os << "kafka_client_default";
    case rule::kafka_client_prefix:
        return os << "kafka_client_prefix";
    case rule::kafka_client_id:
        return os << "kafka_client_id";
    case rule::kafka_user_default:
        return os << "kafka_user_default";
    case rule::kafka_user_default_client_default:
        return os << "kafka_user_default_client_default";
    case rule::kafka_user_default_client_prefix:
        return os << "kafka_user_default_client_prefix";
    case rule::kafka_user_default_client_id:
        return os << "kafka_user_default_client_id";
    case rule::kafka_user:
        return os << "kafka_user";
    case rule::kafka_user_client_default:
        return os << "kafka_user_client_default";
    case rule::kafka_user_client_prefix:
        return os << "kafka_user_client_prefix";
    case rule::kafka_user_client_id:
        return os << "kafka_user_client_id";
    }
}

} // namespace cluster::client_quota

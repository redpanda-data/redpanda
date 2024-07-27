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
#include "kafka/protocol/types.h"

#include "utils/base64.h"

namespace kafka {

uuid uuid::from_string(std::string_view encoded) {
    if (encoded.size() > 24) {
        details::throw_out_of_range(
          "Input size of {} too long to be decoded as b64-UUID, expected "
          "{} bytes or less",
          encoded.size(),
          24);
    }
    auto decoded = base64_to_bytes(encoded);
    if (decoded.size() != length) {
        details::throw_out_of_range(
          "Expected {} byte value post b64decoding the input: {} bytes",
          length,
          decoded.size());
    }
    underlying_t ul;
    std::copy_n(decoded.begin(), length, ul.begin());
    return uuid(ul);
}

ss::sstring uuid::to_string() const { return bytes_to_base64(view()); }

std::ostream& operator<<(std::ostream& os, const uuid& u) {
    return os << u.to_string();
}

std::ostream& operator<<(std::ostream& os, describe_configs_type t) {
    switch (t) {
    case describe_configs_type::unknown:
        return os << "{unknown}";
    case describe_configs_type::boolean:
        return os << "{boolean}";
    case describe_configs_type::string:
        return os << "{string}";
    case describe_configs_type::int_type:
        return os << "{int}";
    case describe_configs_type::short_type:
        return os << "{short}";
    case describe_configs_type::long_type:
        return os << "{long}";
    case describe_configs_type::double_type:
        return os << "{double}";
    case describe_configs_type::list:
        return os << "{list}";
    case describe_configs_type::class_type:
        return os << "{class}";
    case describe_configs_type::password:
        return os << "{password}";
    }
    return os << "{unsupported type}";
}

std::ostream&
operator<<(std::ostream& os, describe_client_quotas_match_type t) {
    switch (t) {
    case describe_client_quotas_match_type::exact_name:
        return os << "{exact_name}";
    case describe_client_quotas_match_type::default_name:
        return os << "{default_name}";
    case describe_client_quotas_match_type::any_specified_name:
        return os << "{any_specified_name}";
    }
    return os << "{unsupported type}";
}

std::ostream& operator<<(std::ostream& os, coordinator_type t) {
    switch (t) {
    case coordinator_type::group:
        return os << "{group}";
    case coordinator_type::transaction:
        return os << "{transaction}";
    };
    return os << "{unknown type}";
}

std::ostream& operator<<(std::ostream& os, config_resource_type t) {
    switch (t) {
    case config_resource_type::topic:
        return os << "{topic}";
    case config_resource_type::broker:
        [[fallthrough]];
    case config_resource_type::broker_logger:
        break;
    }
    return os << "{unknown type}";
}

std::ostream& operator<<(std::ostream& os, describe_configs_source s) {
    switch (s) {
    case describe_configs_source::topic:
        return os << "{topic}";
    case describe_configs_source::static_broker_config:
        return os << "{static_broker_config}";
    case describe_configs_source::default_config:
        return os << "{default_config}";
    }
    return os << "{unknown type}";
}

std::ostream& operator<<(std::ostream& os, config_resource_operation t) {
    switch (t) {
    case config_resource_operation::set:
        return os << "set";
    case config_resource_operation::append:
        return os << "append";
    case config_resource_operation::remove:
        return os << "remove";
    case config_resource_operation::subtract:
        return os << "subtract";
    }
    return os << "unknown type";
}

} // namespace kafka

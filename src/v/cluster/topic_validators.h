/*
 * Copyright 2023 Redpanda Data, Inc.
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

namespace cluster {

struct schema_id_validation_validator {
    static constexpr const char* error_message
      = "Mismatch between redpanda.* and confluent.* schema id validation "
        "properties ";
    static constexpr errc ec = errc::topic_invalid_config;

    template<typename T>
    static bool
    compatible(const std::optional<T>& lhs, const std::optional<T>& rhs) {
        // If both are specified, they must match
        return !lhs || !rhs || lhs == rhs;
    }

    static bool is_valid(cluster::topic_properties& p) {
        return compatible(
                 p.record_key_schema_id_validation,
                 p.record_key_schema_id_validation_compat)
               && compatible(
                 p.record_key_subject_name_strategy,
                 p.record_key_subject_name_strategy_compat)
               && compatible(
                 p.record_value_schema_id_validation,
                 p.record_value_schema_id_validation_compat)
               && compatible(
                 p.record_value_subject_name_strategy,
                 p.record_value_subject_name_strategy_compat);
    }
};

} // namespace cluster

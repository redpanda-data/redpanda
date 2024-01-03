// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "model/validation.h"

#include "base/seastarx.h"
#include "base/vlog.h"
#include "model/errc.h"

#include <seastar/util/log.hh>

namespace model {

std::error_code validate_kafka_topic_name(const model::topic& tpc) {
    static constexpr size_t kafka_max_topic_name_length = 249;
    const auto& name = tpc();
    if (name.empty()) {
        return make_error_code(errc::topic_name_empty);
    }
    if (name == "." || name == "..") {
        return make_error_code(errc::forbidden_topic_name);
    }
    if (name.length() > kafka_max_topic_name_length) {
        return make_error_code(errc::topic_name_len_exceeded);
    }
    for (char c : name) {
        if (!std::isalnum(c) && !(c == '.' || c == '-' || c == '_')) {
            return make_error_code(errc::invalid_topic_name);
        }
    }
    return make_error_code(errc::success);
}

} // namespace model

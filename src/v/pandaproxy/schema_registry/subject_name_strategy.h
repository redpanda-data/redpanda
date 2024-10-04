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

#include "base/seastarx.h"
#include "strings/string_switch.h"

#include <seastar/core/sstring.hh>

namespace pandaproxy::schema_registry {

enum class subject_name_strategy : uint8_t {
    topic_name,
    record_name,
    topic_record_name,
};

inline constexpr std::string_view to_string_view(subject_name_strategy e) {
    switch (e) {
    case subject_name_strategy::topic_name:
        return "TopicNameStrategy";
    case subject_name_strategy::record_name:
        return "RecordNameStrategy";
    case subject_name_strategy::topic_record_name:
        return "TopicRecordNameStrategy";
    }
    return "{invalid}";
}

inline constexpr std::string_view
to_string_view_compat(subject_name_strategy e) {
    switch (e) {
    case subject_name_strategy::topic_name:
        return "io.confluent.kafka.serializers.subject.TopicNameStrategy";
    case subject_name_strategy::record_name:
        return "io.confluent.kafka.serializers.subject.RecordNameStrategy";
    case subject_name_strategy::topic_record_name:
        return "io.confluent.kafka.serializers.subject.TopicRecordNameStrategy";
    }
    return "{invalid}";
}

inline constexpr std::ostream&
operator<<(std::ostream& os, subject_name_strategy e) {
    return os << to_string_view(e);
}

inline std::istream& operator>>(std::istream& i, subject_name_strategy& e) {
    ss::sstring s;
    i >> s;
    e = string_switch<subject_name_strategy>(s)
          .match_all(
            to_string_view(subject_name_strategy::topic_name),
            to_string_view_compat(subject_name_strategy::topic_name),
            subject_name_strategy::topic_name)
          .match_all(
            to_string_view(subject_name_strategy::record_name),
            to_string_view_compat(subject_name_strategy::record_name),
            subject_name_strategy::record_name)
          .match_all(
            to_string_view(subject_name_strategy::topic_record_name),
            to_string_view_compat(subject_name_strategy::topic_record_name),
            subject_name_strategy::topic_record_name);
    return i;
}

} // namespace pandaproxy::schema_registry

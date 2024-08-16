/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "json/document.h"
#include "transform/logging/event.h"
#include "transform/logging/tests/utils.h"

#include <gtest/gtest.h>

namespace transform::logging {

TEST(TransformLogEventTest, ValidateLogEventJson) {
    using namespace std::chrono_literals;

    ss::sstring message = "Hello";
    auto now = transform::logging::event::clock_type::now();
    model::node_id node_id{0};
    ss::log_level level{ss::log_level::info};
    model::transform_name name{"dummy"};

    transform::logging::event ev{node_id, now, level, message};
    iobuf b;
    ev.to_json(model::transform_name_view{name()}, b);

    auto doc = testing::parse_json(std::move(b));

    EXPECT_TRUE(doc.IsObject());

    EXPECT_TRUE(
      doc.HasMember("body") && doc["body"].IsObject()
      && doc["body"].HasMember("stringValue")
      && doc["body"]["stringValue"].IsString()
      && doc["body"]["stringValue"].GetString() == message);

    EXPECT_TRUE(
      doc.HasMember("timeUnixNano") && doc["timeUnixNano"].IsUint64()
      && doc["timeUnixNano"].GetUint64()
           == static_cast<uint64_t>(now.time_since_epoch() / 1ns));

    EXPECT_TRUE(
      doc.HasMember("severityNumber") && doc["severityNumber"].IsUint()
      && doc["severityNumber"].GetUint()
           == transform::logging::log_level_to_severity(level));

    EXPECT_TRUE(
      doc.HasMember("attributes") && doc["attributes"].IsArray()
      && doc["attributes"].GetArray().Size() == 2);

    const auto& attrs = doc["attributes"].GetArray();

    for (const auto& attr : attrs) {
        EXPECT_TRUE(
          attr.IsObject() && attr.HasMember("key") && attr["key"].IsString()
          && attr.HasMember("value") && attr["value"].IsObject());
    }

    EXPECT_TRUE(
      attrs[0]["key"].GetString() == ss::sstring("transform_name")
      && attrs[0]["value"]["stringValue"].IsString()
      && attrs[0]["value"]["stringValue"].GetString() == name);

    EXPECT_TRUE(
      attrs[1]["key"].GetString() == ss::sstring("node")
      && attrs[1]["value"]["intValue"].IsInt()
      && attrs[1]["value"]["intValue"].GetInt() == node_id);
}

TEST(TransformLogEventTest, LogLevelToSeverity) {
    for (auto lvl :
         {ss::log_level::trace,
          ss::log_level::debug,
          ss::log_level::info,
          ss::log_level::warn,
          ss::log_level::error}) {
        transform::logging::event ev{
          model::node_id{0},
          transform::logging::event::clock_type::now(),
          lvl,
          "bar"};
        iobuf b;
        ev.to_json(model::transform_name_view{"foo"}, b);
        auto doc = testing::parse_json(std::move(b));
        EXPECT_TRUE(
          doc.HasMember("severityNumber") && doc["severityNumber"].IsUint()
          && doc["severityNumber"].GetUint()
               == transform::logging::log_level_to_severity(ev.level));
    }
}
} // namespace transform::logging

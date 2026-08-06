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

#include "proto/redpanda/core/admin/v2/shadow_link.pb.h"

#include <gmock/gmock.h>
#include <google/api/field_behavior.pb.h>
#include <google/protobuf/descriptor.h>
#include <gtest/gtest.h>

#include <string>
#include <vector>

namespace {

bool is_input_only(const google::protobuf::FieldDescriptor& field) {
    const auto& behaviors = field.options().GetRepeatedExtension(
      google::api::field_behavior);
    return std::ranges::any_of(behaviors, [](auto behavior) {
        return behavior == google::api::INPUT_ONLY;
    });
}

// Enumerates every path (dotted chain of field names) from `message` to a
// field annotated (google.api.field_behavior) = INPUT_ONLY. Paths are
// enumerated per reference, not per field descriptor, so reusing a message
// containing INPUT_ONLY fields in a new place (e.g. another TLSSettings)
// yields a new path.
void collect_input_only_paths(
  const google::protobuf::Descriptor& message,
  const std::string& prefix,
  std::vector<const google::protobuf::Descriptor*>& parents,
  std::vector<std::string>& paths) {
    if (std::ranges::contains(parents, &message)) {
        return;
    }
    parents.push_back(&message);
    for (int i = 0; i < message.field_count(); ++i) {
        const auto* field = message.field(i);
        auto path = prefix.empty() ? std::string{field->name()}
                                   : prefix + "." + std::string{field->name()};
        if (is_input_only(*field)) {
            paths.push_back(path);
        }
        if (field->message_type() != nullptr) {
            collect_input_only_paths(
              *field->message_type(), path, parents, paths);
        }
    }
    parents.pop_back();
}

} // namespace

TEST(input_only_fields, merge_input_only_fields_covers_all) {
    std::vector<std::string> paths;
    std::vector<const google::protobuf::Descriptor*> parents;
    collect_input_only_paths(
      *redpanda::core::admin::v2::ShadowLink::descriptor(), "", parents, paths);

    // Clients cannot read INPUT_ONLY values back, so an update request that
    // omits one must keep the stored value instead of clearing it. Every
    // INPUT_ONLY path reachable from ShadowLink therefore needs a matching
    // branch in merge_input_only_fields() in converter.cc. If this
    // expectation fails because a path appeared, add that branch first, then
    // extend this list.
    // clang-format off
    EXPECT_THAT(
      paths,
      testing::UnorderedElementsAre(
        "configurations.client_options.authentication_configuration.scram_configuration.password",
        "configurations.client_options.authentication_configuration.plain_configuration.password",
        "configurations.client_options.tls_settings.tls_pem_settings.key"));
    // clang-format on
}

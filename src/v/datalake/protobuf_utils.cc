// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "datalake/protobuf_utils.h"

namespace datalake {

bool is_recursive_type(
  const google::protobuf::Descriptor& msg,
  const proto_descriptors_stack& stack) {
    return std::any_of(
      stack.begin(), stack.end(), [&](const google::protobuf::Descriptor* d) {
          return d->full_name() == msg.full_name();
      });
}

} // namespace datalake

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

#pragma once

#include "bytes/iobuf.h"
#include "serde/protobuf/parser.h"

namespace google::protobuf {
class Descriptor;
} // namespace google::protobuf

namespace serde::pb {

/// Encode a parsed Protobuf message back to wire format.
/// This is the inverse of serde::pb::parse().
ss::future<iobuf>
encode(const parsed::message& msg, const google::protobuf::Descriptor& desc);

} // namespace serde::pb

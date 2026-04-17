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

#pragma once

#include "bytes/iobuf.h"
#include "serde/avro/parser.h"

#include <avro/Schema.hh>

namespace serde::avro {

/// Encode a parsed Avro message back to Avro binary format.
/// Follows the schema field order. Uses zigzag varint encoding
/// for lengths and long/int values.
ss::future<iobuf>
encode(const parsed::message& msg, const ::avro::ValidSchema& schema);

} // namespace serde::avro

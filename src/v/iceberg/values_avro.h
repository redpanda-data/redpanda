// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0
#pragma once

#include "iceberg/datatypes.h"
#include "iceberg/values.h"

#include <avro/Node.hh>

namespace iceberg {

// Serializes the given struct value with the given struct type as an Avro
// iobuf. The given struct name will be included in the Avro schema, and the
// given metadata will be included in the Avro header.
//
// XXX: only use this for Iceberg manifest metadata! Not all Avro types are
// implemented yet.
avro::GenericDatum
struct_to_avro(const struct_value& v, const avro::NodePtr& avro_schema);

} // namespace iceberg

// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0
#pragma once

#include "datalake/conversion_outcome.h"
#include "google/protobuf/descriptor.h"
#include "iceberg/datatypes.h"

namespace datalake {

// convert a protobuf message schema to iceberg schema
conversion_outcome<iceberg::struct_type>
type_to_iceberg(const google::protobuf::Descriptor& pool);

} // namespace datalake

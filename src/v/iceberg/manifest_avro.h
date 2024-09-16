// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0
#pragma once

#include "bytes/iobuf.h"
#include "iceberg/manifest.h"
#include "iceberg/partition_key_type.h"

namespace iceberg {

iobuf serialize_avro(const manifest&);
manifest parse_manifest(const partition_key_type&, iobuf);

} // namespace iceberg

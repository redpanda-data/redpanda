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
#include "iceberg/manifest_list.h"

namespace iceberg {

iobuf serialize_avro(const manifest_list&);
manifest_list parse_manifest_list(iobuf);

} // namespace iceberg

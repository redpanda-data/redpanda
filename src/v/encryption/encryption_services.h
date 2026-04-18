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

#include "encryption/fwd.h"

namespace encryption {

/// Bundle of encryption services passed to make_partition_proxy.
struct encryption_services {
    schema_resolver& resolver;
    dek_manager& dek_mgr;
    field_transformer& transformer;
};

} // namespace encryption

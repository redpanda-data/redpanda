/*
 * Copyright 2021 Vectorized, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "pandaproxy/schema_registry/errors.h"
#include "pandaproxy/schema_registry/types.h"

namespace pandaproxy::schema_registry {

result<void> validate(std::string_view def, schema_type type);

result<schema_definition> sanitize(schema_definition def, schema_type type);

} // namespace pandaproxy::schema_registry

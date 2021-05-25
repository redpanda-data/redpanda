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

#include "pandaproxy/schema_registry/error.h"
#include "pandaproxy/schema_registry/types.h"

#include <rapidjson/stringbuffer.h>
#include <rapidjson/writer.h>

namespace pandaproxy::schema_registry {

using topic_key_magic = named_type<int32_t, struct topic_key_magic_tag>;
using topic_key_type = named_type<ss::sstring, struct topic_key_type_tag>;

} // namespace pandaproxy::schema_registry

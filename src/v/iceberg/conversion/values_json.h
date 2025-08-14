/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#pragma once

#include "iceberg/conversion/conversion_outcome.h"
#include "iceberg/conversion/ir_json.h"

namespace iceberg {

ss::future<value_outcome> deserialize_json(iobuf, const json_conversion_ir&);

}; // namespace iceberg

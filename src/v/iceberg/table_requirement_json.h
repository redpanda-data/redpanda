/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#pragma once

#include "iceberg/json_writer.h"
#include "iceberg/table_requirement.h"

namespace json {

void rjson_serialize(
  iceberg::json_writer&, const iceberg::table_requirement::requirement&);

} // namespace json

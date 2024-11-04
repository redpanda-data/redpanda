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

#include "datalake/conversion_outcome.h"

#include <avro/ValidSchema.hh>

namespace datalake {

/**
 * Deserialize an Avro serialized message into an iceberg value.
 */
ss::future<value_outcome>
deserialize_avro(iobuf buffer, avro::ValidSchema schema);

} // namespace datalake

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
#include "iceberg/datatypes.h"

#include <avro/Node.hh>

namespace datalake {

/**
 * Converts given AVRO schema to iceberg struct type. If a top level avro type
 * is different than avro record the method will return a struct type with
 * single filed being a top level type.
 *
 * Most important limitations/assumptions:
 *
 *  - do not support recursive types
 *
 *  - avro unions are 'flattened' as a structure with optional fields out of
 *    which only one or none are present (i case ther is a field with null type)
 *
 *  - all fields are required by default (avro always sets a default in binary
 *    representation)
 *
 *  - duration logical type is ignored
 *
 *  - avro null type is ignored and not represented in iceberg schema
 *
 *  - different flavours of timestamp and time types are all represented by the
 *    same iceberg type, conversion will be done when parsing avro value
 */
conversion_outcome<iceberg::struct_type> type_to_iceberg(const avro::NodePtr&);
} // namespace datalake

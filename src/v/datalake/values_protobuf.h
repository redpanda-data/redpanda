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
#include "google/protobuf/descriptor.h"
#include "serde/protobuf/parser.h"

namespace datalake {

/**
 * Deserializes a protobuf message using serde::pb::parser and converts it to
 * iceberg::value
 */
ss::future<optional_value_outcome> deserialize_protobuf(
  iobuf buffer, const google::protobuf::Descriptor& type_descriptor);
/**
 * Converts protocol serde::pb::parsed_message to an iceberg::value
 */
ss::future<optional_value_outcome> proto_parsed_message_to_value(
  std::unique_ptr<serde::pb::parsed::message> message,
  const google::protobuf::Descriptor& descriptor);

} // namespace datalake

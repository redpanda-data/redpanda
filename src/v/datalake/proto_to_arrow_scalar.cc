/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#include "datalake/proto_to_arrow_scalar.h"

#include "datalake/logger.h"

#include <arrow/array/array_primitive.h>
#include <arrow/type.h>
#include <google/protobuf/message.h>

#include <memory>
#include <stdexcept>

namespace datalake::detail {
template<>
void proto_to_arrow_scalar<arrow::Int32Type>::do_add(
  const google::protobuf::Message* msg, int field_idx) {
    auto* desc = msg->GetDescriptor()->field(field_idx);
    if (!desc) {
        datalake_log.error("Invalid protobuf field index");
        _arrow_status = arrow::Status::Invalid("Invalid protobuf field index");
        return;
    }
    _arrow_status = _builder->Append(
      msg->GetReflection()->GetInt32(*msg, desc));
}

template<>
void proto_to_arrow_scalar<arrow::Int64Type>::do_add(
  const google::protobuf::Message* msg, int field_idx) {
    auto* desc = msg->GetDescriptor()->field(field_idx);
    if (!desc) {
        datalake_log.error("Invalid protobuf field index");
        _arrow_status = arrow::Status::Invalid("Invalid protobuf field index");
        return;
    }
    _arrow_status = _builder->Append(
      msg->GetReflection()->GetInt64(*msg, desc));
}

template<>
void proto_to_arrow_scalar<arrow::BooleanType>::do_add(
  const google::protobuf::Message* msg, int field_idx) {
    auto* desc = msg->GetDescriptor()->field(field_idx);
    if (!desc) {
        datalake_log.error("Invalid protobuf field index");
        _arrow_status = arrow::Status::Invalid("Invalid protobuf field index");
        return;
    }
    _arrow_status = _builder->Append(msg->GetReflection()->GetBool(*msg, desc));
}

template<>
void proto_to_arrow_scalar<arrow::FloatType>::do_add(
  const google::protobuf::Message* msg, int field_idx) {
    auto* desc = msg->GetDescriptor()->field(field_idx);
    if (!desc) {
        datalake_log.error("Invalid protobuf field index");
        _arrow_status = arrow::Status::Invalid("Invalid protobuf field index");
        return;
    }
    _arrow_status = _builder->Append(
      msg->GetReflection()->GetFloat(*msg, desc));
}

template<>
void proto_to_arrow_scalar<arrow::DoubleType>::do_add(
  const google::protobuf::Message* msg, int field_idx) {
    auto* desc = msg->GetDescriptor()->field(field_idx);
    if (!desc) {
        datalake_log.error("Invalid protobuf field index");
        _arrow_status = arrow::Status::Invalid("Invalid protobuf field index");
        return;
    }
    _arrow_status = _builder->Append(
      msg->GetReflection()->GetDouble(*msg, desc));
}

template<>
void proto_to_arrow_scalar<arrow::StringType>::do_add(
  const google::protobuf::Message* msg, int field_idx) {
    auto* desc = msg->GetDescriptor()->field(field_idx);
    if (!desc) {
        datalake_log.error("Invalid protobuf field index");
        _arrow_status = arrow::Status::Invalid("Invalid protobuf field index");
        return;
    }
    _arrow_status = _builder->Append(
      msg->GetReflection()->GetString(*msg, desc));
}

} // namespace datalake::detail

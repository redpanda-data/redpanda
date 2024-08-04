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

#include "datalake/errors.h"
#include "datalake/proto_to_arrow_interface.h"
#include "datalake/proto_to_arrow_scalar.h"

#include <memory>
#include <stdexcept>

namespace google::protobuf {
class Descriptor;
}

namespace datalake::detail {

class proto_to_arrow_struct : public proto_to_arrow_interface {
public:
    explicit proto_to_arrow_struct(
      const google::protobuf::Descriptor* message_descriptor);

    // Because this is a struct builder, we expect the child referred to by
    // field_idx to be a struct itself.
    arrow::Status add_child_value(
      const google::protobuf::Message* msg, int field_idx) override;

    // Adds the message pointed to by msg. Unlike add_child_value, this adds
    // the message itself, and not a child of the message.
    arrow::Status add_struct_message(const google::protobuf::Message* msg);

    arrow::Status finish_chunk() override;

    std::shared_ptr<arrow::Field> field(const std::string& name) override;
    std::shared_ptr<arrow::ArrayBuilder> builder() override;

    arrow::FieldVector get_field_vector();

private:
    std::vector<std::unique_ptr<proto_to_arrow_interface>> _child_converters;
    std::shared_ptr<arrow::DataType> _arrow_data_type;
    std::shared_ptr<arrow::StructBuilder> _builder;
    arrow::FieldVector _fields;
};

} // namespace datalake::detail

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

#include "datalake/proto_to_arrow_interface.h"

#include <arrow/api.h>

namespace datalake::detail {

template<typename ArrowType>
class proto_to_arrow_scalar : public proto_to_arrow_interface {
    using BuilderType = arrow::TypeTraits<ArrowType>::BuilderType;

public:
    proto_to_arrow_scalar()
      : _builder(std::make_shared<BuilderType>()) {}

    arrow::Status add_child_value(
      const google::protobuf::Message* msg, int field_idx) override {
        if (!_arrow_status.ok()) {
            return _arrow_status;
        }
        do_add(msg, field_idx);
        return _arrow_status;
    }

    arrow::Status finish_chunk() override {
        if (!_arrow_status.ok()) {
            return _arrow_status;
        }
        auto builder_result = _builder->Finish();
        _arrow_status = builder_result.status();
        std::shared_ptr<arrow::Array> array;
        if (!_arrow_status.ok()) {
            return _arrow_status;
        }

        // Safe because we validated the status after calling `Finish`
        array = std::move(builder_result).ValueUnsafe();
        _values.push_back(array);
        return _arrow_status;
    }

    std::shared_ptr<arrow::Field> field(const std::string& name) override {
        return arrow::field(
          name, arrow::TypeTraits<ArrowType>::type_singleton());
    }

    std::shared_ptr<arrow::ArrayBuilder> builder() override { return _builder; }

private:
    void do_add(const google::protobuf::Message* msg, int field_idx);

    std::shared_ptr<BuilderType> _builder;
};

} // namespace datalake::detail

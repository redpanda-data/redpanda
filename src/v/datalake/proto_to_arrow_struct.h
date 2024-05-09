#pragma once

#include "datalake/proto_to_arrow_interface.h"
#include "datalake/proto_to_arrow_scalar.h"

#include <arrow/api.h>
#include <arrow/type.h>
#include <google/protobuf/message.h>

#include <memory>
#include <stdexcept>

namespace datalake::detail {

class proto_to_arrow_struct : public proto_to_arrow_interface {
public:
    explicit proto_to_arrow_struct(
      const google::protobuf::Descriptor* message_descriptor) {
        using namespace detail;
        namespace pb = google::protobuf;

        // Set up child arrays
        for (int field_idx = 0; field_idx < message_descriptor->field_count();
             field_idx++) {
            auto desc = message_descriptor->field(field_idx);
            if (desc->cpp_type() == pb::FieldDescriptor::CPPTYPE_INT32) {
                _child_arrays.push_back(
                  std::make_unique<proto_to_arrow_scalar<arrow::Int32Type>>());
            } else if (desc->cpp_type() == pb::FieldDescriptor::CPPTYPE_INT64) {
                _child_arrays.push_back(
                  std::make_unique<proto_to_arrow_scalar<arrow::Int64Type>>());
            } else if (desc->cpp_type() == pb::FieldDescriptor::CPPTYPE_BOOL) {
                _child_arrays.push_back(
                  std::make_unique<
                    proto_to_arrow_scalar<arrow::BooleanType>>());
            } else if (desc->cpp_type() == pb::FieldDescriptor::CPPTYPE_FLOAT) {
                _child_arrays.push_back(
                  std::make_unique<proto_to_arrow_scalar<arrow::FloatType>>());
            } else if (
              desc->cpp_type() == pb::FieldDescriptor::CPPTYPE_DOUBLE) {
                _child_arrays.push_back(
                  std::make_unique<proto_to_arrow_scalar<arrow::DoubleType>>());
            } else if (
              desc->cpp_type() == pb::FieldDescriptor::CPPTYPE_STRING) {
                _child_arrays.push_back(
                  std::make_unique<proto_to_arrow_scalar<arrow::StringType>>());
            } else if (
              desc->cpp_type() == pb::FieldDescriptor::CPPTYPE_MESSAGE) {
                auto field_message_descriptor = desc->message_type();
                if (field_message_descriptor == nullptr) {
                    throw std::runtime_error(
                      std::string("Can't find schema for nested type : ")
                      + desc->cpp_type_name());
                }
                _child_arrays.push_back(std::make_unique<proto_to_arrow_struct>(
                  field_message_descriptor));
            } else {
                throw std::runtime_error(
                  std::string("Unknown type: ") + desc->cpp_type_name());
            }
        }
        // Make Arrow data types

        // This could be combined into a single loop with the one above, but
        // this seems more readable to me
        arrow::FieldVector fields;
        for (int field_idx = 0; field_idx < message_descriptor->field_count();
             field_idx++) {
            auto field_desc = message_descriptor->field(field_idx);
            auto field_name = field_desc->name();
            fields.push_back(_child_arrays[field_idx]->field(field_name));
        }
        _arrow_data_type = arrow::struct_(fields);

        // Make builder
        std::vector<std::shared_ptr<arrow::ArrayBuilder>> child_builders;
        // This could also be collapsed into the above loop
        for (auto& child : _child_arrays) {
            child_builders.push_back(child->builder());
        }
        _builder = std::make_shared<arrow::StructBuilder>(
          _arrow_data_type, arrow::default_memory_pool(), child_builders);
    }

    arrow::Status
    add_value(const google::protobuf::Message* msg, int field_idx) override {
        if (!_arrow_status.ok()) {
            return _arrow_status;
        }
        auto desc = msg->GetDescriptor()->field(field_idx);
        auto child_message = &msg->GetReflection()->GetMessage(*msg, desc);
        for (size_t child_idx = 0; child_idx < _child_arrays.size();
             child_idx++) {
            _arrow_status = _child_arrays[child_idx]->add_value(
              child_message, int(child_idx));
            if (!_arrow_status.ok()) {
                return _arrow_status;
            }
        }
        _arrow_status = _builder->Append();

        return _arrow_status;
    }

    arrow::Status finish_batch() override {
        if (!_arrow_status.ok()) {
            return _arrow_status;
        }

        auto&& builder_result = _builder->Finish();
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
        return arrow::field(name, _arrow_data_type);
    }
    std::shared_ptr<arrow::ArrayBuilder> builder() override { return _builder; }

private:
    std::vector<std::unique_ptr<proto_to_arrow_interface>> _child_arrays;
    std::shared_ptr<arrow::DataType> _arrow_data_type;
    std::shared_ptr<arrow::StructBuilder> _builder;
};

} // namespace datalake::detail

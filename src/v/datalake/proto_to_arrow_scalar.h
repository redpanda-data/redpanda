#pragma once

#include "datalake/proto_to_arrow_interface.h"

#include <arrow/api.h>
#include <arrow/array/array_primitive.h>
#include <arrow/type.h>
#include <google/protobuf/message.h>

#include <memory>
#include <stdexcept>

namespace datalake::detail {

template<typename ArrowType>
class proto_to_arrow_scalar : public proto_to_arrow_interface {
    using BuilderType = arrow::TypeTraits<ArrowType>::BuilderType;

public:
    proto_to_arrow_scalar()
      : _builder(std::make_shared<BuilderType>()) {}

    arrow::Status
    add_value(const google::protobuf::Message* msg, int field_idx) override {
        if (!_arrow_status.ok()) {
            return _arrow_status;
        }
        do_add<ArrowType>(msg, field_idx);
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
        return arrow::field(
          name, arrow::TypeTraits<ArrowType>::type_singleton());
    }

    std::shared_ptr<arrow::ArrayBuilder> builder() override { return _builder; }

private:
    template<typename T>
    void
    do_add(const google::protobuf::Message* /* msg */, int /* field_idx */) {
        throw std::runtime_error("Not implemented!");
    }

    // Signed integer types
    template<>
    void do_add<arrow::Int32Type>(
      const google::protobuf::Message* msg, int field_idx) {
        auto desc = msg->GetDescriptor()->field(field_idx);
        _arrow_status = _builder->Append(
          msg->GetReflection()->GetInt32(*msg, desc));
    }

    template<>
    void do_add<arrow::Int64Type>(
      const google::protobuf::Message* msg, int field_idx) {
        auto desc = msg->GetDescriptor()->field(field_idx);
        _arrow_status = _builder->Append(
          msg->GetReflection()->GetInt64(*msg, desc));
    }

    // Bool
    template<>
    void do_add<arrow::BooleanType>(
      const google::protobuf::Message* msg, int field_idx) {
        auto desc = msg->GetDescriptor()->field(field_idx);
        _arrow_status = _builder->Append(
          msg->GetReflection()->GetBool(*msg, desc));
    }

    // Floating Point Types
    template<>
    void do_add<arrow::FloatType>(
      const google::protobuf::Message* msg, int field_idx) {
        auto desc = msg->GetDescriptor()->field(field_idx);
        _arrow_status = _builder->Append(
          msg->GetReflection()->GetFloat(*msg, desc));
    }

    template<>
    void do_add<arrow::DoubleType>(
      const google::protobuf::Message* msg, int field_idx) {
        auto desc = msg->GetDescriptor()->field(field_idx);
        _arrow_status = _builder->Append(
          msg->GetReflection()->GetDouble(*msg, desc));
    }

    // Unsigned Integer Types
    // FIXME: Iceberg doesn't support unsigned integer types. I'm including
    // these to use for Tweet Ids for a demo, but we should not actually include
    // them.
    template<>
    void do_add<arrow::UInt32Type>(
      const google::protobuf::Message* msg, int field_idx) {
        auto desc = msg->GetDescriptor()->field(field_idx);
        _arrow_status = _builder->Append(
          msg->GetReflection()->GetUInt32(*msg, desc));
    }

    template<>
    void do_add<arrow::UInt64Type>(
      const google::protobuf::Message* msg, int field_idx) {
        auto desc = msg->GetDescriptor()->field(field_idx);
        _arrow_status = _builder->Append(
          msg->GetReflection()->GetUInt64(*msg, desc));
    }

    // String
    template<>
    void do_add<arrow::StringType>(
      const google::protobuf::Message* msg, int field_idx) {
        auto desc = msg->GetDescriptor()->field(field_idx);
        _arrow_status = _builder->Append(
          msg->GetReflection()->GetString(*msg, desc));
    }

    std::shared_ptr<BuilderType> _builder;
};

} // namespace datalake::detail

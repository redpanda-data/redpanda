#include "datalake/protobuf_to_arrow_converter.h"

#include <stdexcept>

datalake::proto_to_arrow_converter::proto_to_arrow_converter(
  std::string schema) {
    initialize_protobuf_schema(schema);
    if (!initialize_arrow_arrays()) {
        // FIXME: exception type
        throw std::runtime_error("Could not initialize arrow arrays");
    }
}
void datalake::proto_to_arrow_converter::add_message(
  const std::string& serialized_message) {
    std::unique_ptr<google::protobuf::Message> message = parse_message(
      serialized_message);
    if (message == nullptr) {
        // FIXME: Silently ignoring unparseable messages seems bad.
        return;
    }
    add_message_parsed(std::move(message));
}
void datalake::proto_to_arrow_converter::finish_batch() {
    for (auto& [field_idx, array] : _arrays) {
        assert(
          field_idx >= 0); // Dummy assert to avoid unused variable warning.
        if (!array->finish_batch().ok()) {
            // FIXME
            throw std::runtime_error("Could not finish batch");
        }
    }
}
std::shared_ptr<arrow::Table>
datalake::proto_to_arrow_converter::build_table() {
    // TODO: if there is still data in the builders, call finish_batch and
    // log an error that the caller should have called it.
    std::vector<std::shared_ptr<arrow::ChunkedArray>> data_arrays;
    for (auto& [field_idx, array] : _arrays) {
        assert(
          field_idx >= 0); // Dummy assert to avoid unused variable warning.
        data_arrays.push_back(array->finish());
    }
    // FIXME: This will fail if we don't have any columns!
    auto table = arrow::Table::Make(
      build_schema(), data_arrays, data_arrays[0]->length());
    return table;
}
std::vector<std::shared_ptr<arrow::Field>>
datalake::proto_to_arrow_converter::build_field_vec() {
    const google::protobuf::Descriptor* message_desc = message_descriptor();
    assert(message_desc != nullptr);

    std::vector<std::shared_ptr<arrow::Field>> field_vec;
    for (auto& [field_idx, array] : _arrays) {
        auto field = message_desc->field(field_idx);
        field_vec.push_back(array->field(field->name()));
    }
    return field_vec;
}
std::shared_ptr<arrow::Schema>
datalake::proto_to_arrow_converter::build_schema() {
    return arrow::schema(build_field_vec());
}
void datalake::proto_to_arrow_converter::add_message_parsed(
  std::unique_ptr<google::protobuf::Message> message) {
    // TODO(jcipar): Allocating and deallocating the field descriptor array
    // for every message is probably a bad idea.
    auto reflection = message->GetReflection();
    std::vector<const google::protobuf::FieldDescriptor*> field_descriptors;
    reflection->ListFields(*message, &field_descriptors);
    for (auto& [field_idx, array] : _arrays) {
        assert(
          field_idx >= 0); // Dummy assert to avoid unused variable warning.
        // TODO: handle this error
        if (!array->add_value(message.get(), field_idx).ok()) {
            // FIXME
            throw new std::runtime_error("Could not add value");
        }
    }
}
void datalake::proto_to_arrow_converter::initialize_protobuf_schema(
  const std::string& schema) {
    google::protobuf::io::ArrayInputStream proto_input_stream(
      schema.c_str(), schema.size());
    google::protobuf::io::Tokenizer tokenizer(&proto_input_stream, nullptr);

    google::protobuf::compiler::Parser parser;
    if (!parser.Parse(&tokenizer, &_file_descriptor_proto)) {
        // TODO: custom exception type, or does something exist in wasm or
        // schema registry already?
        throw std::runtime_error("Could not parse protobuf schema");
    }

    if (!_file_descriptor_proto.has_name()) {
        _file_descriptor_proto.set_name("test_message");
    }

    _file_desc = _protobuf_descriptor_pool.BuildFile(_file_descriptor_proto);
    if (_file_desc == nullptr) {
        throw std::runtime_error("Could not build descriptor pool");
    }
}
bool datalake::proto_to_arrow_converter::initialize_arrow_arrays() {
    using namespace detail;
    namespace pb = google::protobuf;

    const pb::Descriptor* message_desc = message_descriptor();
    if (message_desc == nullptr) {
        return false;
    }

    if (message_desc == nullptr) {
        return false;
    }

    for (int field_idx = 0; field_idx < message_desc->field_count();
         field_idx++) {
        auto desc = message_desc->field(field_idx);

        if (desc->cpp_type() == pb::FieldDescriptor::CPPTYPE_INT32) {
            _arrays[field_idx]
              = std::make_unique<proto_to_arrow_scalar<arrow::Int32Type>>();
        } else if (desc->cpp_type() == pb::FieldDescriptor::CPPTYPE_INT64) {
            _arrays[field_idx]
              = std::make_unique<proto_to_arrow_scalar<arrow::Int64Type>>();
        } else if (desc->cpp_type() == pb::FieldDescriptor::CPPTYPE_UINT32) {
            _arrays[field_idx]
              = std::make_unique<proto_to_arrow_scalar<arrow::UInt32Type>>();
        } else if (desc->cpp_type() == pb::FieldDescriptor::CPPTYPE_UINT64) {
            _arrays[field_idx]
              = std::make_unique<proto_to_arrow_scalar<arrow::UInt64Type>>();
        } else if (desc->cpp_type() == pb::FieldDescriptor::CPPTYPE_STRING) {
            _arrays[field_idx]
              = std::make_unique<proto_to_arrow_scalar<arrow::StringType>>();
        } else if (desc->cpp_type() == pb::FieldDescriptor::CPPTYPE_MESSAGE) {
            auto field_message_descriptor = desc->message_type();
            if (field_message_descriptor == nullptr) {
                return false;
            }
            _arrays[field_idx] = std::make_unique<proto_to_arrow_struct>(
              field_message_descriptor);
        } else {
            throw std::runtime_error(
              std::string("Unknown type: ") + desc->cpp_type_name());
        }
    }
    return true;
}
std::unique_ptr<google::protobuf::Message>
datalake::proto_to_arrow_converter::parse_message(const std::string& message) {
    // TODO: How much of this can be moved to initialization code to avoid
    // reallocating objects?

    // Get the message descriptor
    const google::protobuf::Descriptor* message_desc = message_descriptor();
    assert(message_desc != nullptr);

    const google::protobuf::Message* prototype_msg = _factory.GetPrototype(
      message_desc);
    assert(prototype_msg != nullptr);

    google::protobuf::Message* mutable_msg = prototype_msg->New();
    assert(mutable_msg != nullptr);

    if (!mutable_msg->ParseFromString(message)) {
        return nullptr;
    }
    return std::unique_ptr<google::protobuf::Message>(mutable_msg);
}

const google::protobuf::Descriptor*
datalake::proto_to_arrow_converter::message_descriptor() {
    int message_type_count = _file_desc->message_type_count();
    if (message_type_count == 0) {
        return nullptr;
    }
    return _file_desc->message_type(message_type_count - 1);
}

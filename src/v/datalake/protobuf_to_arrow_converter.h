#pragma once
#include "datalake/proto_to_arrow_interface.h"
#include "datalake/proto_to_arrow_scalar.h"
#include "datalake/proto_to_arrow_struct.h"

#include <arrow/api.h>
#include <arrow/array/builder_base.h>
#include <arrow/array/builder_nested.h>
#include <arrow/chunked_array.h>
#include <arrow/io/api.h>
#include <arrow/result.h>
#include <arrow/status.h>
#include <arrow/type.h>
#include <arrow/type_fwd.h>
#include <arrow/type_traits.h>
#include <google/protobuf/arena.h>
#include <google/protobuf/compiler/parser.h>
#include <google/protobuf/descriptor.h>
#include <google/protobuf/dynamic_message.h>
#include <google/protobuf/io/tokenizer.h>
#include <google/protobuf/io/zero_copy_stream_impl_lite.h>
#include <google/protobuf/message.h>
#include <google/protobuf/stubs/common.h>
#include <google/protobuf/unknown_field_set.h>

#include <memory>
#include <stdexcept>

namespace datalake {

class proto_to_arrow_converter {
public:
    proto_to_arrow_converter(std::string schema);

    void add_message(const std::string& serialized_message);

    void finish_batch();

    std::shared_ptr<arrow::Table> build_table();
    void clear();

    std::vector<std::shared_ptr<arrow::Field>> build_field_vec();

    std::shared_ptr<arrow::Schema> build_schema();

    /// Parse the message to a protobuf message.
    /// Return nullptr on error.
    std::unique_ptr<google::protobuf::Message>
    parse_message(const std::string& message);

private:
    FRIEND_TEST(ArrowWriter, EmptyMessageTest);
    FRIEND_TEST(ArrowWriter, SimpleMessageTest);
    FRIEND_TEST(ArrowWriter, NestedMessageTest);
    FRIEND_TEST(ArrowWriter, NestedMessageBench);

    void add_message_parsed(std::unique_ptr<google::protobuf::Message> message);

    void initialize_protobuf_schema(const std::string& schema);

    bool initialize_arrow_arrays();

    const google::protobuf::Descriptor* message_descriptor();

private:
    // Proto to array converters. Map represents field_id->proto_to_array
    std::map<int, std::unique_ptr<detail::proto_to_arrow_interface>> _arrays;

    // Protobuf parsing
    google::protobuf::DescriptorPool _protobuf_descriptor_pool;
    google::protobuf::FileDescriptorProto _file_descriptor_proto;
    google::protobuf::DynamicMessageFactory _factory;
    const google::protobuf::FileDescriptor* _file_desc;
};

} // namespace datalake

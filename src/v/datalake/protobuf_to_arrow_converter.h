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

    std::vector<std::shared_ptr<arrow::Field>> build_field_vec();

    std::shared_ptr<arrow::Schema> build_schema();

private:
    FRIEND_TEST(ArrowWriter, EmptyMessageTest);
    FRIEND_TEST(ArrowWriter, SimpleMessageTest);
    FRIEND_TEST(ArrowWriter, NestedMessageTest);

    void add_message_parsed(std::unique_ptr<google::protobuf::Message> message);

    void initialize_protobuf_schema(const std::string& schema);

    bool initialize_arrow_arrays();

    /// Parse the message to a protobuf message.
    /// Return nullptr on error.
    std::unique_ptr<google::protobuf::Message>
    parse_message(const std::string& message);
    const google::protobuf::Descriptor* message_descriptor();

private:
    // Proto to array converters. Map represents field_id->proto_to_array
    std::map<int, std::unique_ptr<detail::proto_to_arrow_interface>> _arrays;

    // Protobuf parsing
    // TODO: Figure out which of these need to remain live after the constructor
    // builds them
    google::protobuf::DescriptorPool _protobuf_descriptor_pool;
    google::protobuf::FileDescriptorProto _file_descriptor_proto;
    google::protobuf::DynamicMessageFactory _factory;
    const google::protobuf::FileDescriptor* _file_desc;
};

inline std::shared_ptr<arrow::ChunkedArray>
table_to_chunked_struct_array(const std::shared_ptr<arrow::Table> table) {
    if (table->columns().size() == 0) {
        return nullptr;
    }
    int chunk_count = table->columns()[0]->num_chunks();

    // Build data type & child builders
    arrow::FieldVector fields;
    auto column_names = table->ColumnNames();
    std::vector<std::shared_ptr<arrow::ArrayBuilder>> child_builders;
    for (const auto& name : column_names) {
        auto column = table->GetColumnByName(name);
        auto type = column->type();
        fields.push_back(arrow::field(name, type));

        // make builder
        auto unique_builder_result = arrow::MakeBuilder(type);
        if (!unique_builder_result.ok()) {
            return nullptr;
        }
        std::shared_ptr<arrow::ArrayBuilder> builder
          = std::shared_ptr<arrow::ArrayBuilder>(
            std::move(unique_builder_result.ValueUnsafe()));
        child_builders.push_back(builder);
    }
    std::shared_ptr<arrow::DataType> struct_type = arrow::struct_(fields);

    // Make builder
    auto struct_builder = arrow::StructBuilder(
      struct_type, arrow::default_memory_pool(), child_builders);

    // Iterate over chunks, rows, and columns
    arrow::ArrayVector result_vector;
    for (int chunk_num = 0; chunk_num < chunk_count; chunk_num++) {
        int64_t row_count = table->columns()[0]->chunk(chunk_num)->length();
        for (int64_t row_num = 0; row_num < row_count; row_num++) {
            for (int column_num = 0; column_num < table->num_columns();
                 column_num++) {
                auto column = table->column(column_num);
                auto chunk = column->chunk(chunk_num);
                auto scalar_result = chunk->GetScalar(row_num);
                if (!scalar_result.ok()) {
                    return nullptr;
                }
                if (!child_builders[column_num]
                       ->AppendScalar(*scalar_result.ValueUnsafe())
                       .ok()) {
                    return nullptr;
                }
            }
            if (!struct_builder.Append().ok()) {
                return nullptr;
            }
        }

        // Finish the chunk
        auto struct_result = struct_builder.Finish();
        if (!struct_result.ok()) {
            return nullptr;
        }
        result_vector.push_back(struct_result.ValueUnsafe());
    }

    // Make the chunked array
    auto result_array = std::make_shared<arrow::ChunkedArray>(
      result_vector, struct_type);

    return result_array;
}

} // namespace datalake

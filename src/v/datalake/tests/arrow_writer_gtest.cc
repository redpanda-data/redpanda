#include "datalake/protobuf_to_arrow_converter.h"
#include "datalake/tests/arrow_writer_test_utils.h"
#include "test_utils/test.h"

#include <google/protobuf/descriptor.h>
#include <google/protobuf/message.h>
#include <gtest/gtest.h>

#include <stdexcept>
#include <string>

namespace datalake {
TEST(ArrowWriter, EmptyMessageTest) {
    using namespace datalake;
    test_data test_data;
    std::string schema = test_data.empty_schema;

    proto_to_arrow_converter converter(schema);

    std::string serialized_message = generate_empty_message();

    auto parsed_message = converter.parse_message(serialized_message);
    EXPECT_NE(parsed_message, nullptr);
    EXPECT_EQ(parsed_message->GetTypeName(), "datalake.proto.empty_message");
}

TEST(ArrowWriter, SimpleMessageTest) {
    using namespace datalake;
    std::string serialized_message = generate_simple_message(
      "Hello world", 12345);
    std::cerr << "Serialized message is " << serialized_message.size()
              << " bytes\n";
    EXPECT_TRUE(false);

    proto_to_arrow_converter converter(test_data().simple_schema);
    EXPECT_EQ(converter._arrays.size(), 6);
    auto parsed_message = converter.parse_message(serialized_message);
    EXPECT_NE(parsed_message, nullptr);
    EXPECT_EQ(parsed_message->GetTypeName(), "datalake.proto.simple_message");

    converter.add_message(serialized_message);
    {
        converter.add_message(generate_simple_message("I", 1));
        converter.add_message(generate_simple_message("II", 2));
        converter.add_message(generate_simple_message("III", 3));
        converter.add_message(generate_simple_message("IV", 4));
        converter.add_message(generate_simple_message("V", 5));
    }
    converter.finish_batch();

    auto schema = converter.build_schema();
    auto table = converter.build_table();

    EXPECT_EQ(
      schema->field_names(),
      std::vector<std::string>(
        {"label",
         "number",
         "big_number",
         "float_number",
         "double_number",
         "true_or_false"}));
    std::vector<std::string> table_field_names;
    for (const auto& field : table->fields()) {
        table_field_names.push_back(field->name());
    }
    EXPECT_EQ(table_field_names, schema->field_names());
}

TEST(ArrowWriter, NestedMessageTest) {
    using namespace datalake;
    std::string serialized_message = generate_nested_message(
      "Hello world", 12345);

    proto_to_arrow_converter converter(test_data().nested_schema);
    EXPECT_EQ(converter._arrays.size(), 3);
    auto parsed_message = converter.parse_message(serialized_message);
    EXPECT_NE(parsed_message, nullptr);
    EXPECT_EQ(parsed_message->GetTypeName(), "datalake.proto.nested_message");

    converter.add_message(serialized_message);
    {
        converter.add_message(generate_nested_message("I", 1));
        converter.add_message(generate_nested_message("II", 2));
        converter.add_message(generate_nested_message("III", 3));
        converter.add_message(generate_nested_message("IV", 4));
        converter.add_message(generate_nested_message("V", 5));
    }
    converter.finish_batch();

    auto schema = converter.build_schema();
    auto table = converter.build_table();

    EXPECT_EQ(
      schema->field_names(),
      std::vector<std::string>({"label", "number", "inner_message"}));
    std::vector<std::string> table_field_names;
    for (const auto& field : table->fields()) {
        table_field_names.push_back(field->name());
    }
    EXPECT_EQ(table_field_names, schema->field_names());
}
} // namespace datalake

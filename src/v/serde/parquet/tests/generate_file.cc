/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "base/seastarx.h"
#include "bytes/iostream.h"
#include "container/zip.h"
#include "json/chunked_buffer.h"
#include "json/iobuf_writer.h"
#include "serde/parquet/schema.h"
#include "serde/parquet/value.h"
#include "serde/parquet/writer.h"
#include "utils/base64.h"

#include <seastar/core/app-template.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/core/file.hh>
#include <seastar/core/fstream.hh>
#include <seastar/core/reactor.hh>
#include <seastar/util/variant_utils.hh>

using json_writer = json::iobuf_writer<json::chunked_buffer>;

namespace serde::parquet {
namespace {

void json(json_writer& w, const schema_element& schema, const value& v) {
    ss::visit(
      v,
      [&w](const null_value&) { w.Null(); },
      [&w](const boolean_value& v) { w.Bool(v.val); },
      [&w](const int32_value& v) { w.Int(v.val); },
      [&w](const int64_value& v) { w.Int64(v.val); },
      [&w](const float32_value& v) { w.Double(v.val); },
      [&w](const float64_value& v) { w.Double(v.val); },
      [&w](const byte_array_value& v) { w.String(iobuf_to_base64(v.val)); },
      [&w](const fixed_byte_array_value& v) {
          w.String(iobuf_to_base64(v.val));
      },
      [&w, &schema](const group_value& v) {
          w.StartObject();
          using container::zip;
          for (const auto& [child, member] : zip(schema.children, v)) {
              w.Key(child.name);
              json(w, child, member.field);
          }
          w.EndObject();
      },
      [&w, &schema](const repeated_value& v) {
          w.StartArray();
          for (const auto& member : v) {
              json(w, schema, member.element);
          }
          w.EndArray();
      });
}

struct testcase {
    schema_element schema;
    std::vector<value> rows;
    iobuf parquet_file;
};

iobuf json(const testcase& tc) {
    json::chunked_buffer buf;
    json_writer w(buf);
    w.StartObject();
    w.Key("file");
    w.String(iobuf_to_base64(tc.parquet_file));
    w.Key("rows");
    w.StartArray();
    for (const auto& row : tc.rows) {
        json(w, tc.schema, row);
    }
    w.EndArray();
    w.EndObject();
    return std::move(buf).as_iobuf();
}

template<typename... Args>
schema_element
group_node(ss::sstring name, field_repetition_type rep_type, Args... args) {
    chunked_vector<schema_element> children;
    (children.push_back(std::move(args)), ...);
    return {
      .type = std::monostate{},
      .repetition_type = rep_type,
      .name = std::move(name),
      .children = std::move(children),
    };
}

schema_element leaf_node(
  ss::sstring name,
  field_repetition_type rep_type,
  physical_type ptype,
  logical_type ltype = logical_type{}) {
    return {
      .type = ptype,
      .repetition_type = rep_type,
      .name = std::move(name),
      .logical_type = ltype,
    };
}

template<typename... Args>
group_value record(Args... field) {
    chunked_vector<group_member> fields;
    (fields.push_back(group_member{std::move(field)}), ...);
    return fields;
}

template<typename... Args>
repeated_value list(Args... field) {
    chunked_vector<repeated_element> fields;
    (fields.push_back(repeated_element{std::move(field)}), ...);
    return fields;
}

// NOLINTBEGIN(*magic-number*)

schema_element dremel_paper_schema() {
    schema_element document_schema = group_node(
      "Document",
      field_repetition_type::required,
      leaf_node("DocId", field_repetition_type::required, i64_type{}),
      group_node(
        "Links",
        field_repetition_type::optional,
        leaf_node("Forward", field_repetition_type::repeated, i64_type{}),
        leaf_node("Backward", field_repetition_type::repeated, i64_type{})),
      group_node(
        "Name",
        field_repetition_type::repeated,
        group_node(
          "Language",
          field_repetition_type::repeated,
          leaf_node(
            "Code",
            field_repetition_type::required,
            byte_array_type{},
            string_type{}),
          leaf_node(
            "Country",
            field_repetition_type::optional,
            byte_array_type{},
            string_type{})),
        leaf_node(
          "Url",
          field_repetition_type::optional,
          byte_array_type{},
          string_type{})));
    index_schema(document_schema);
    return document_schema;
}

std::vector<value> dremel_paper_values() {
    std::vector<value> values;
    values.emplace_back(record(
      /*DocId*/ int64_value(10),
      /*Links*/
      record(
        /*Forward*/ list(int64_value(20), int64_value(40), int64_value(60)),
        /*Backward*/ list()),
      list(
        /*Name*/
        record(
          list(
            /*Language*/
            record(
              /*Code*/
              byte_array_value(iobuf::from("en-us")),
              /*Country*/
              byte_array_value(iobuf::from("us"))),
            /*Language*/
            record(byte_array_value(iobuf::from("en")), null_value())),
          /*Url*/
          byte_array_value(iobuf::from("http://A"))),
        /*Name*/
        record(
          list(),
          /*Url*/
          byte_array_value(iobuf::from("http://B"))),
        /*Name*/
        record(
          list(
            /*Language*/
            record(
              /*Code*/
              byte_array_value(iobuf::from("en-gb")),
              /*Country*/
              byte_array_value(iobuf::from("gb")))),
          /*Url*/
          null_value()))));
    values.emplace_back(record(
      /*DocId*/ int64_value(20),
      /*Links*/
      record(
        /*Forward*/ list(int64_value(80)),
        /*Backward*/ list(int64_value(10), int64_value(30))),
      /*Name*/
      list(record(
        /*Language*/
        repeated_value(),
        /*Url*/
        byte_array_value(iobuf::from("http://C"))))));
    return values;
}

[[maybe_unused]]
schema_element all_types_schema() {
    schema_element document_schema = group_node(
      "Root",
      field_repetition_type::required,
      leaf_node("A", field_repetition_type::required, bool_type{}),
      leaf_node("B", field_repetition_type::required, i32_type{}),
      leaf_node("C", field_repetition_type::required, i64_type{}),
      leaf_node("D", field_repetition_type::required, f32_type{}),
      leaf_node("E", field_repetition_type::required, f64_type{}),
      leaf_node("F", field_repetition_type::required, byte_array_type{}),
      leaf_node(
        "G",
        field_repetition_type::required,
        byte_array_type{.fixed_length = 16}),
      group_node(
        "Nested",
        field_repetition_type::repeated,
        leaf_node("A", field_repetition_type::required, bool_type{}),
        leaf_node("B", field_repetition_type::required, i32_type{}),
        leaf_node("C", field_repetition_type::required, i64_type{}),
        leaf_node("D", field_repetition_type::required, f32_type{}),
        leaf_node("E", field_repetition_type::required, f64_type{}),
        leaf_node("F", field_repetition_type::required, byte_array_type{})),
      group_node("Logical", field_repetition_type::optional));
    index_schema(document_schema);
    return document_schema;
}

ss::future<iobuf> serialize_testcase(size_t test_case) {
    switch (test_case) {
    case 0: {
        iobuf file;
        writer w(
          {
            .schema = dremel_paper_schema(),
          },
          make_iobuf_ref_output_stream(file));
        co_await w.init();
        for (auto& value : dremel_paper_values()) {
            co_await w.write_row(std::get<group_value>(std::move(value)));
        }
        co_await w.close();
        co_return json(testcase{
          .schema = dremel_paper_schema(),
          .rows = dremel_paper_values(),
          .parquet_file = std::move(file),
        });
    }
    case 1:
        // TODO
        co_return iobuf();
    default:
        throw std::runtime_error(
          fmt::format("unsupported test case: {}", test_case));
    }
}
// NOLINTEND(*magic-number*)

ss::future<> run_main(std::string output_file, size_t test_case) {
    auto handle = co_await ss::open_file_dma(
      output_file, ss::open_flags::rw | ss::open_flags::create);
    auto stream = co_await ss::make_file_output_stream(handle);
    auto buf = co_await serialize_testcase(test_case);
    co_await write_iobuf_to_output_stream(std::move(buf), stream);
    co_await stream.flush();
    co_await stream.close();
}
} // namespace
} // namespace serde::parquet

namespace bpo = boost::program_options;

int main(int argc, char** argv) {
    ss::app_template::seastar_options seastar_config;
    // Use a small footprint to generate this single file.
    seastar_config.smp_opts.smp.set_value(1);
    seastar_config.smp_opts.memory_allocator = ss::memory_allocator::standard;
    seastar_config.log_opts.default_log_level.set_value(ss::log_level::warn);
    ss::app_template app(std::move(seastar_config));
    app.add_options()(
      "output", bpo::value<std::string>(), "e.g. --output /tmp/foo/bar");
    app.add_options()("test-case", bpo::value<size_t>(), "e.g. --test-case 99");
    app.run(argc, argv, [&app]() -> ss::future<> {
        const auto& config = app.configuration();
        return serde::parquet::run_main(
          config["output"].as<std::string>(), config["test-case"].as<size_t>());
    });
}

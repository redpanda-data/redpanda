#include "manifest_entry.hh"
#include "manifest_file.hh"
#include <avro/Compiler.hh>
#include <avro/DataFile.hh>
#include <avro/Decoder.hh>
#include <avro/Encoder.hh>
#include <avro/Types.hh>
#include <avro/ValidSchema.hh>
#include <filesystem>
#include <fstream>
#include <iostream>

bool write_manifest_list(std::filesystem::path manifest_list_file_name,
                         std::vector<std::filesystem::path> data_files,
                         std::string table_schema);
bool write_manifest_file(std::filesystem::path manifest_file_name,
                         std::vector<std::filesystem::path> data_files,
                         std::string table_schema);

avro::ValidSchema load_schema(std::filesystem::path filename) {
  std::ifstream ifs(filename);
  avro::ValidSchema result;
  avro::compileJsonSchema(ifs, result);
  return result;
}

std::string get_schema();

std::vector<std::filesystem::path> data_files = {
    "/home/jim/Software/duckdb_iceberg/data/iceberg/lineitem_iceberg/data/"
    "00000-411-0792dcfe-4e25-4ca3-8ada-175286069a47-00001.parquet",

    "/home/jim/Software/duckdb_iceberg/data/iceberg/lineitem_iceberg/data/"
    "00041-414-f3c73457-bbd6-4b92-9c15-17b241171b16-00001.parquet"};

int main(void) {
  std::cerr << "Writing manfifest files\n";
  write_manifest_file("generated/cpp_manifest_file.avro", data_files,
                      get_schema());

  std::cerr << "Writing manifest list\n";
  write_manifest_list(
      "generated/cpp_manifest_list.avro",
      {"/home/jim/Programming/avro-python/generated/cpp_manifest_file.avro"},
      get_schema());
  return 0;
}

bool write_manifest_list(std::filesystem::path manifest_list_file_name,
                         std::vector<std::filesystem::path> manifest_paths,
                         std::string table_schema) {

  avro::ValidSchema schema = load_schema("manifest_file_string_schema.json");
  std::cout << schema.toJson(true);

  avro::DataFileWriter<manifest_file> dfw(manifest_list_file_name.c_str(),
                                          schema);
  for (const auto &path : manifest_paths) {
    size_t size = std::filesystem::file_size(path);
    manifest_file entry;
    entry.manifest_path = path;
    entry.manifest_length = size;
    entry.partition_spec_id = 0;
    entry.content = 0;
    entry.sequence_number = 2;
    entry.min_sequence_number = 0;
    entry.added_snapshot_id = 1;
    entry.added_data_files_count = manifest_paths.size();
    entry.existing_data_files_count = 0;
    entry.deleted_data_files_count = 0;
    entry.added_rows_count = 0;
    entry.existing_rows_count = 0;
    entry.deleted_rows_count = 0;
    entry.partitions = {};
    dfw.write(entry);
  }

  dfw.close();

  return true;
}

bool write_manifest_file(std::filesystem::path manifest_file_name,
                         std::vector<std::filesystem::path> data_files,
                         std::string table_schema) {
  std::map<std::string, std::string> custom_metadata = {
    {"schema", table_schema},
    {"partition-spec", "[]"},
    {"partition-spec-id", "0"},
    {"format-version", "2"},
    {"content", "data"}
  };

  avro::ValidSchema schema = load_schema("manifest_entry_string_schema.json");
  size_t avro_default_sync_interval = 16 * 1024;
  avro::DataFileWriter<manifest_entry> dfw(manifest_file_name.c_str(), schema,
                                           avro_default_sync_interval,
                                           avro::NULL_CODEC, custom_metadata);

  for (const auto &path : data_files) {
    size_t size = std::filesystem::file_size(path);
    manifest_entry entry;
    entry.status = 1;
    entry.data_file.content = 0;
    entry.data_file.file_path = path;
    entry.data_file.file_format = "PARQUET";
    entry.data_file.partition = {};
    entry.data_file.record_count = 1; // FIXME
    entry.data_file.file_size_in_bytes = size;
    dfw.write(entry);
  }
  dfw.close();

  return true;
}

std::string get_schema() {
  return R"SCHEMA(
{
    "type" : "struct",
    "schema-id" : 0,
    "fields" : [ {
      "id" : 1,
      "name" : "l_orderkey",
      "required" : false,
      "type" : "int"
    }, {
      "id" : 2,
      "name" : "l_partkey",
      "required" : false,
      "type" : "int"
    }, {
      "id" : 3,
      "name" : "l_suppkey",
      "required" : false,
      "type" : "int"
    }, {
      "id" : 4,
      "name" : "l_linenumber",
      "required" : false,
      "type" : "int"
    }, {
      "id" : 5,
      "name" : "l_quantity",
      "required" : false,
      "type" : "int"
    }, {
      "id" : 6,
      "name" : "l_extendedprice",
      "required" : false,
      "type" : "decimal(15, 2)"
    }, {
      "id" : 7,
      "name" : "l_discount",
      "required" : false,
      "type" : "decimal(15, 2)"
    }, {
      "id" : 8,
      "name" : "l_tax",
      "required" : false,
      "type" : "decimal(15, 2)"
    }, {
      "id" : 9,
      "name" : "l_returnflag",
      "required" : false,
      "type" : "string"
    }, {
      "id" : 10,
      "name" : "l_linestatus",
      "required" : false,
      "type" : "string"
    }, {
      "id" : 11,
      "name" : "l_shipdate",
      "required" : false,
      "type" : "date"
    }, {
      "id" : 12,
      "name" : "l_commitdate",
      "required" : false,
      "type" : "date"
    }, {
      "id" : 13,
      "name" : "l_receiptdate",
      "required" : false,
      "type" : "date"
    }, {
      "id" : 14,
      "name" : "l_shipinstruct",
      "required" : false,
      "type" : "string"
    }, {
      "id" : 15,
      "name" : "l_shipmode",
      "required" : false,
      "type" : "string"
    }, {
      "id" : 16,
      "name" : "l_comment",
      "required" : false,
      "type" : "string"
    } ]
  }
)SCHEMA";
}
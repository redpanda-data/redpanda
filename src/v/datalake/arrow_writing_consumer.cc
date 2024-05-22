#include "datalake/arrow_writing_consumer.h"

#include "datalake/protobuf_to_arrow_converter.h"

#include <seastar/core/future.hh>
#include <seastar/core/loop.hh>

#include <arrow/array/builder_binary.h>
#include <arrow/io/file.h>
#include <arrow/table.h>
#include <arrow/type_fwd.h>
#include <parquet/arrow/writer.h>

#include <exception>
#include <memory>

datalake::arrow_writing_consumer::arrow_writing_consumer(
  std::filesystem::path local_file_path, std::string protobuf_schema)
  : _local_file_path(std::move(local_file_path)) {
    protobuf_schema = (R"schema(
          syntax = "proto2";
        package datalake.proto;

        message message_type {

        optional int32 number_1 = 1;
optional int32 number_2 = 2;
optional int32 number_3 = 3;
optional int32 number_4 = 4;
optional int32 number_5 = 5;
optional int32 number_6 = 6;
optional int32 number_7 = 7;
optional int32 number_8 = 8;
optional int32 number_9 = 9;
optional int32 number_10 = 10;
optional int32 number_11 = 11;
optional int32 number_12 = 12;
optional int32 number_13 = 13;
optional int32 number_14 = 14;
optional int32 number_15 = 15;
optional int32 number_16 = 16;
optional int32 number_17 = 17;
optional int32 number_18 = 18;
optional int32 number_19 = 19;
optional int32 number_20 = 20;
optional int32 number_21 = 21;
optional int32 number_22 = 22;
optional int32 number_23 = 23;
optional int32 number_24 = 24;
optional int32 number_25 = 25;
optional int32 number_26 = 26;
optional int32 number_27 = 27;
optional int32 number_28 = 28;
optional int32 number_29 = 29;
optional int32 number_30 = 30;
optional int32 number_31 = 31;
optional int32 number_32 = 32;
optional int32 number_33 = 33;
optional int32 number_34 = 34;
optional int32 number_35 = 35;
optional int32 number_36 = 36;
optional int32 number_37 = 37;
optional int32 number_38 = 38;
optional int32 number_39 = 39;
optional int32 number_40 = 40;
optional int32 number_41 = 41;
optional int32 number_42 = 42;
optional int32 number_43 = 43;
optional int32 number_44 = 44;
optional int32 number_45 = 45;
optional int32 number_46 = 46;
optional int32 number_47 = 47;
optional int32 number_48 = 48;
optional int32 number_49 = 49;
optional int32 number_50 = 50;
optional int32 number_51 = 51;
optional int32 number_52 = 52;
optional int32 number_53 = 53;
optional int32 number_54 = 54;
optional int32 number_55 = 55;
optional int32 number_56 = 56;
optional int32 number_57 = 57;
optional int32 number_58 = 58;
optional int32 number_59 = 59;
optional int32 number_60 = 60;
optional int32 number_61 = 61;
optional int32 number_62 = 62;
optional int32 number_63 = 63;
optional int32 number_64 = 64;
optional int32 number_65 = 65;
optional int32 number_66 = 66;
optional int32 number_67 = 67;
optional int32 number_68 = 68;
optional int32 number_69 = 69;
optional int32 number_70 = 70;
optional int32 number_71 = 71;
optional int32 number_72 = 72;
optional int32 number_73 = 73;
optional int32 number_74 = 74;
optional int32 number_75 = 75;
optional int32 number_76 = 76;
optional int32 number_77 = 77;
optional int32 number_78 = 78;
optional int32 number_79 = 79;
optional int32 number_80 = 80;
optional int32 number_81 = 81;
optional int32 number_82 = 82;
optional int32 number_83 = 83;
optional int32 number_84 = 84;
optional int32 number_85 = 85;
optional int32 number_86 = 86;
optional int32 number_87 = 87;
optional int32 number_88 = 88;
optional int32 number_89 = 89;
optional int32 number_90 = 90;
optional int32 number_91 = 91;
optional int32 number_92 = 92;
optional int32 number_93 = 93;
optional int32 number_94 = 94;
optional int32 number_95 = 95;
optional int32 number_96 = 96;
optional int32 number_97 = 97;
optional int32 number_98 = 98;
optional int32 number_99 = 99;
optional int32 number_100 = 100;
optional int32 number_101 = 101;
optional int32 number_102 = 102;
optional int32 number_103 = 103;
optional int32 number_104 = 104;
optional int32 number_105 = 105;
optional int32 number_106 = 106;
optional int32 number_107 = 107;
optional int32 number_108 = 108;
optional int32 number_109 = 109;
optional int32 number_110 = 110;
optional string str_1 = 111;
optional string str_2 = 112;
optional string str_3 = 113;
optional string str_4 = 114;
optional string str_5 = 115;
optional string str_6 = 116;
optional string str_7 = 117;
optional string str_8 = 118;
optional string str_9 = 119;
optional string str_10 = 120;
optional string str_11 = 121;
optional string str_12 = 122;
optional string str_13 = 123;
optional string str_14 = 124;
optional string str_15 = 125;
optional string str_16 = 126;
optional string str_17 = 127;
optional string str_18 = 128;
optional string str_19 = 129;
optional string str_20 = 130;
optional string str_21 = 131;
optional string str_22 = 132;
optional string str_23 = 133;
optional string str_24 = 134;
optional string str_25 = 135;
optional string str_26 = 136;
optional string str_27 = 137;
optional string str_28 = 138;
optional string str_29 = 139;
optional string str_30 = 140;
optional string str_31 = 141;
optional string str_32 = 142;
optional string str_33 = 143;
optional string str_34 = 144;
optional string str_35 = 145;
optional string str_36 = 146;
optional string str_37 = 147;
optional string str_38 = 148;
optional string str_39 = 149;
optional string str_40 = 150;
optional string str_41 = 151;
optional string str_42 = 152;
optional string str_43 = 153;
optional string str_44 = 154;
optional string str_45 = 155;
optional string str_46 = 156;
optional string str_47 = 157;
optional string str_48 = 158;
optional string str_49 = 159;
optional string str_50 = 160;
optional string str_51 = 161;
optional string str_52 = 162;
optional string str_53 = 163;
optional string str_54 = 164;
optional string str_55 = 165;
optional string str_56 = 166;
optional string str_57 = 167;
optional string str_58 = 168;
optional string str_59 = 169;
optional string str_60 = 170;
optional string str_61 = 171;
optional string str_62 = 172;
optional string str_63 = 173;
optional string str_64 = 174;
optional string str_65 = 175;
optional string str_66 = 176;
optional string str_67 = 177;
optional string str_68 = 178;
optional string str_69 = 179;
optional string str_70 = 180;
optional string str_71 = 181;
optional string str_72 = 182;
optional string str_73 = 183;
optional string str_74 = 184;
optional string str_75 = 185;
optional string str_76 = 186;
optional string str_77 = 187;
optional string str_78 = 188;
optional string str_79 = 189;
optional string str_80 = 190;
optional string str_81 = 191;
optional string str_82 = 192;
optional string str_83 = 193;
optional string str_84 = 194;
optional string str_85 = 195;
optional string str_86 = 196;
optional string str_87 = 197;
optional string str_88 = 198;
optional string str_89 = 199;
optional string str_90 = 200;
optional string str_91 = 201;
optional string str_92 = 202;
optional string str_93 = 203;
optional string str_94 = 204;
optional string str_95 = 205;
optional string str_96 = 206;
optional string str_97 = 207;
optional string str_98 = 208;
optional string str_99 = 209;
optional string str_100 = 210;
optional string str_101 = 211;
optional string str_102 = 212;
optional string str_103 = 213;
optional string str_104 = 214;
optional string str_105 = 215;
optional string str_106 = 216;
optional string str_107 = 217;
optional string str_108 = 218;
optional string str_109 = 219;
optional string str_110 = 220;
}
)schema");

    try {
        _table_builder = std::make_unique<proto_to_arrow_converter>(
          protobuf_schema);
    } catch (const std::exception& e) {
        // Couldn't build a table builder, fall back to schemaless
        // TODO: Log this
    }

    _field_key = arrow::field("Key", arrow::binary());
    _field_value = arrow::field("Value", arrow::binary());

    // TODO: use the timestamp type? Iceberg does not support unsigned integers.
    _field_timestamp = arrow::field("Timestamp", arrow::uint64());

    if (_table_builder) {
        _schema = _table_builder->build_schema();
    } else {
        _schema = arrow::schema({_field_key, _field_value, _field_timestamp});
    }

    // Initialize output file
    std::filesystem::create_directories(_local_file_path.parent_path());

    // TODO: use compression. Originally I set it to SNAPPY because that's what
    // the example code in the docs uses, but that was throwing a NotImplemented
    // error at runtime. Pick a compresseion algorithm and ensure our Arrow
    // library is compiled with it.
    std::shared_ptr<parquet::WriterProperties> props
      = parquet::WriterProperties::Builder()
          //   .compression(arrow::Compression::UNCOMPRESSED)
          .build();

    // Opt to store Arrow schema for easier reads back into Arrow
    std::shared_ptr<parquet::ArrowWriterProperties> arrow_props
      = parquet::ArrowWriterProperties::Builder().store_schema()->build();

    auto outfile_result = arrow::io::FileOutputStream::Open(_local_file_path);

    if (!outfile_result.ok()) {
        _ok = outfile_result.status();
        return;
    }
    std::shared_ptr<arrow::io::FileOutputStream> outfile
      = outfile_result.ValueUnsafe();

    // // std::shared_ptr<arrow::io::BufferOutputStream> outfile;
    // std::shared_ptr<arrow::io::MockOutputStream> outfile;

    auto file_writer_result = parquet::arrow::FileWriter::Open(
      *_schema.get(),
      arrow::default_memory_pool(),
      outfile,
      props,
      arrow_props);

    if (!file_writer_result.ok()) {
        _ok = file_writer_result.status();
        return;
    }
    _file_writer = std::move(file_writer_result.ValueUnsafe());
}

ss::future<ss::stop_iteration>
datalake::arrow_writing_consumer::operator()(model::record_batch batch) {
    arrow::BinaryBuilder key_builder;
    arrow::BinaryBuilder value_builder;
    arrow::UInt64Builder timestamp_builder;
    if (batch.compressed()) {
        _compressed_batches++;

        // FIXME: calling internal method of storage module seems like a red
        // flag.
        batch = storage::internal::maybe_decompress_batch_sync(batch);
    } else {
        _uncompressed_batches++;
    }
    batch.for_each_record(
      [this, &batch, &key_builder, &value_builder, &timestamp_builder](
        model::record&& record) {
          std::string key;
          std::string value;
          key = iobuf_to_string(record.key());

          // TODO: Factor out the schemaless code into a schemaless
          // table builder.
          if (record.has_value()) {
              value = iobuf_to_string(record.value());
          }

          _ok = key_builder.Append(key);
          if (!_ok.ok()) {
              return ss::stop_iteration::yes;
          }

          _ok = value_builder.Append(value);
          if (!_ok.ok()) {
              return ss::stop_iteration::yes;
          }

          _ok = timestamp_builder.Append(
            batch.header().first_timestamp.value() + record.timestamp_delta());
          if (!_ok.ok()) {
              return ss::stop_iteration::yes;
          }

          if (_table_builder != nullptr) {
              _table_builder->add_message(value);
          }

          _total_row_count++;
          _unwritten_row_count++;
          return ss::stop_iteration::no;
      });
    if (!_ok.ok()) {
        return ss::make_ready_future<ss::stop_iteration>(
          ss::stop_iteration::yes);
    }

    std::shared_ptr<arrow::Array> key_array;
    std::shared_ptr<arrow::Array> value_array;
    std::shared_ptr<arrow::Array> timestamp_array;

    auto&& key_builder_result = key_builder.Finish();
    // Arrow ASSIGN_OR_RAISE macro doesn't actually raise, it returns a
    // not-ok value. Expanding the macro and editing ensures we get the
    // correct return type.
    _ok = key_builder_result.status();
    if (!_ok.ok()) {
        return ss::make_ready_future<ss::stop_iteration>(
          ss::stop_iteration::yes);
    } else {
        key_array = std::move(key_builder_result).ValueUnsafe();
    }

    auto&& value_builder_result = value_builder.Finish();
    _ok = value_builder_result.status();
    if (!_ok.ok()) {
        return ss::make_ready_future<ss::stop_iteration>(
          ss::stop_iteration::yes);
    } else {
        value_array = std::move(value_builder_result).ValueUnsafe();
    }

    auto&& timestamp_builder_result = timestamp_builder.Finish();
    _ok = timestamp_builder_result.status();
    if (!_ok.ok()) {
        return ss::make_ready_future<ss::stop_iteration>(
          ss::stop_iteration::yes);
    } else {
        timestamp_array = std::move(timestamp_builder_result).ValueUnsafe();
    }

    _key_vector.push_back(key_array);
    _value_vector.push_back(value_array);
    _timestamp_vector.push_back(timestamp_array);

    if (_unwritten_row_count > _row_group_size) {
        if (!write_row_group()) {
            return ss::make_ready_future<ss::stop_iteration>(
              ss::stop_iteration::yes);
        }
    }

    return ss::make_ready_future<ss::stop_iteration>(ss::stop_iteration::no);
}

ss::future<arrow::Status> datalake::arrow_writing_consumer::end_of_stream() {
    if (!_ok.ok()) {
        co_return _ok;
    }
    write_row_group();

    auto close_result = _file_writer->Close();
    if (!close_result.ok()) {
        _ok = close_result;
    }

    co_return _ok;
}

bool datalake::arrow_writing_consumer::write_row_group() {
    if (_unwritten_row_count == 0) {
        return true;
    }

    std::shared_ptr<arrow::Table> table;
    if (_table_builder == nullptr) {
        // TODO: debug log: using schemaless table builder
        table = get_schemaless_table();
    } else {
        // TODO: debug log: using schemaful table builder
        _table_builder->finish_batch();
        table = _table_builder->build_table();
    }
    if (table == nullptr) {
        return false;
    }

    auto write_result = _file_writer->WriteTable(*table.get());
    if (!write_result.ok()) {
        _ok = write_result;
        return false;
    }

    _key_vector.clear();
    _value_vector.clear();
    _timestamp_vector.clear();
    _unwritten_row_count = 0;

    return true;
}

std::shared_ptr<arrow::Table>
datalake::arrow_writing_consumer::get_schemaless_table() {
    if (!_ok.ok()) {
        return nullptr;
    }
    // Create a ChunkedArray
    std::shared_ptr<arrow::ChunkedArray> key_chunks
      = std::make_shared<arrow::ChunkedArray>(_key_vector);
    std::shared_ptr<arrow::ChunkedArray> value_chunks
      = std::make_shared<arrow::ChunkedArray>(_value_vector);
    std::shared_ptr<arrow::ChunkedArray> timestamp_chunks
      = std::make_shared<arrow::ChunkedArray>(_timestamp_vector);

    // Create a table
    return arrow::Table::Make(
      _schema,
      {key_chunks, value_chunks, timestamp_chunks},
      key_chunks->length());
}

uint32_t datalake::arrow_writing_consumer::iobuf_to_uint32(const iobuf& buf) {
    auto kbegin = iobuf::byte_iterator(buf.cbegin(), buf.cend());
    auto kend = iobuf::byte_iterator(buf.cend(), buf.cend());
    std::vector<uint8_t> key_bytes;
    while (kbegin != kend) {
        key_bytes.push_back(*kbegin);
        ++kbegin;
    }
    return *reinterpret_cast<const uint32_t*>(key_bytes.data());
}

std::string
datalake::arrow_writing_consumer::iobuf_to_string(const iobuf& buf) {
    auto vbegin = iobuf::byte_iterator(buf.cbegin(), buf.cend());
    auto vend = iobuf::byte_iterator(buf.cend(), buf.cend());
    std::string value;
    // Byte iterators don't work with the string constructor.
    while (vbegin != vend) {
        value += *vbegin;
        ++vbegin;
    }
    return value;
}

// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0
#include "iceberg/manifest_list_avro.h"

#include "base/units.h"
#include "bytes/iobuf.h"
#include "iceberg/avro_utils.h"
#include "iceberg/manifest_file.avrogen.h"
#include "strings/string_switch.h"

#include <avro/DataFile.hh>

#include <stdexcept>
#include <vector>

namespace iceberg {

namespace {

manifest_file_content content_type_from_int(int t) {
    if (t < static_cast<int>(manifest_file_content::min_supported)) {
        throw std::invalid_argument(
          fmt::format("Unexpected content type: {}", t));
    }
    if (t > static_cast<int>(manifest_file_content::max_supported)) {
        throw std::invalid_argument(
          fmt::format("Unexpected content type: {}", t));
    }
    return manifest_file_content{t};
}

avrogen::r508 summary_to_avro(const field_summary& s) {
    avrogen::r508 ret;
    ret.contains_null = s.contains_null;
    if (s.contains_nan.has_value()) {
        ret.contains_nan.set_bool(s.contains_nan.value());
    } else {
        ret.contains_nan.set_null();
    }

    if (s.lower_bound.has_value()) {
        const auto& bound = s.lower_bound.value();
        std::vector<uint8_t> b;
        b.reserve(bound.size());
        for (const auto byte : bound) {
            b.emplace_back(byte);
        }
        ret.lower_bound.set_bytes(b);
    }
    if (s.upper_bound.has_value()) {
        const auto& bound = s.upper_bound.value();
        std::vector<uint8_t> b;
        b.reserve(bound.size());
        for (const auto byte : bound) {
            b.emplace_back(byte);
        }
        ret.upper_bound.set_bytes(b);
    }
    return ret;
}

field_summary summary_from_avro(const avrogen::r508& s) {
    field_summary ret;
    ret.contains_null = s.contains_null;
    if (!s.contains_nan.is_null()) {
        ret.contains_nan = s.contains_nan.get_bool();
    }
    if (!s.lower_bound.is_null()) {
        const auto& bytes = s.lower_bound.get_bytes();
        iobuf buf;
        buf.append(bytes.data(), bytes.size());
        ret.lower_bound = iobuf_to_bytes(buf);
    }
    if (!s.upper_bound.is_null()) {
        const auto& bytes = s.upper_bound.get_bytes();
        iobuf buf;
        buf.append(bytes.data(), bytes.size());
        ret.upper_bound = iobuf_to_bytes(buf);
    }
    return ret;
}

avrogen::manifest_file file_to_avro(const manifest_file& f) {
    avrogen::manifest_file ret;
    ret.manifest_path = f.manifest_path;
    ret.manifest_length = static_cast<int64_t>(f.manifest_length);
    ret.partition_spec_id = f.partition_spec_id();
    ret.content = static_cast<int32_t>(f.content);
    ret.sequence_number = f.seq_number();
    ret.min_sequence_number = f.min_seq_number();
    ret.added_snapshot_id = f.added_snapshot_id();

    ret.added_data_files_count = static_cast<int32_t>(f.added_files_count);
    ret.existing_data_files_count = static_cast<int32_t>(
      f.existing_files_count);
    ret.deleted_data_files_count = static_cast<int32_t>(f.deleted_files_count);

    ret.added_rows_count = static_cast<int32_t>(f.added_rows_count);
    ret.existing_rows_count = static_cast<int32_t>(f.existing_rows_count);
    ret.deleted_rows_count = static_cast<int32_t>(f.deleted_rows_count);

    if (f.partitions.empty()) {
        ret.partitions.set_null();
    } else {
        std::vector<avrogen::r508> partitions;
        partitions.reserve(f.partitions.size());
        for (const auto& s : f.partitions) {
            partitions.emplace_back(summary_to_avro(s));
        }
        ret.partitions.set_array(partitions);
    }
    return ret;
}

manifest_file file_from_avro(const avrogen::manifest_file& f) {
    manifest_file ret;
    ret.manifest_path = f.manifest_path;
    ret.manifest_length = f.manifest_length;
    ret.partition_spec_id = partition_spec::id_t{f.partition_spec_id};
    ret.content = content_type_from_int(f.content);
    ret.seq_number = sequence_number{f.sequence_number};
    ret.min_seq_number = sequence_number{f.min_sequence_number};
    ret.added_snapshot_id = snapshot_id{f.added_snapshot_id};

    ret.added_files_count = f.added_data_files_count;
    ret.existing_files_count = f.existing_data_files_count;
    ret.deleted_files_count = f.deleted_data_files_count;

    ret.added_rows_count = f.added_rows_count;
    ret.existing_rows_count = f.existing_rows_count;
    ret.deleted_rows_count = f.deleted_rows_count;
    if (!f.partitions.is_null()) {
        for (const auto& s : f.partitions.get_array()) {
            ret.partitions.emplace_back(summary_from_avro(s));
        }
    }
    return ret;
}

} // namespace

iobuf serialize_avro(const manifest_list& m) {
    size_t bytes_streamed = 0;
    avro_iobuf_ostream::buf_container_t bufs;
    static constexpr size_t avro_default_sync_bytes = 16_KiB;
    {
        auto out = std::make_unique<avro_iobuf_ostream>(
          4_KiB, &bufs, &bytes_streamed);
        avro::DataFileWriter<avrogen::manifest_file> writer(
          std::move(out),
          avrogen::manifest_file::valid_schema(),
          avro_default_sync_bytes,
          avro::NULL_CODEC);

        for (const auto& f : m.files) {
            writer.write(file_to_avro(f));
        }
        writer.flush();
        writer.close();

        // NOTE: ~DataFileWriter does a final sync which may write to the
        // chunks. Destruct the writer before moving ownership of the chunks.
    }
    iobuf buf;
    for (auto& b : bufs) {
        buf.append(std::move(b));
    }
    buf.trim_back(buf.size_bytes() - bytes_streamed);
    return buf;
}

manifest_list parse_manifest_list(iobuf buf) {
    auto in = std::make_unique<avro_iobuf_istream>(std::move(buf));
    avro::DataFileReader<avrogen::manifest_file> reader(
      std::move(in), avrogen::manifest_file::valid_schema());
    chunked_vector<manifest_file> files;
    while (true) {
        avrogen::manifest_file f;
        auto did_read = reader.read(f);
        if (!did_read) {
            break;
        }
        files.emplace_back(file_from_avro(f));
    }
    manifest_list m;
    m.files = std::move(files);
    return m;
}

} // namespace iceberg

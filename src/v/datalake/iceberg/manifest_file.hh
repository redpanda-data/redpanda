/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


#ifndef MANIFEST_FILE_HH_648135512_H
#define MANIFEST_FILE_HH_648135512_H


#include <sstream>
#include "boost/any.hpp"
#include "avro/Specific.hh"
#include "avro/Encoder.hh"
#include "avro/Decoder.hh"

struct manifest_file_string_schema_json_Union__0__ {
private:
    size_t idx_;
    boost::any value_;
public:
    size_t idx() const { return idx_; }
    bool is_null() const {
        return (idx_ == 0);
    }
    void set_null() {
        idx_ = 0;
        value_ = boost::any();
    }
    bool get_bool() const;
    void set_bool(const bool& v);
    manifest_file_string_schema_json_Union__0__();
};

struct manifest_file_string_schema_json_Union__1__ {
private:
    size_t idx_;
    boost::any value_;
public:
    size_t idx() const { return idx_; }
    bool is_null() const {
        return (idx_ == 0);
    }
    void set_null() {
        idx_ = 0;
        value_ = boost::any();
    }
    std::vector<uint8_t> get_bytes() const;
    void set_bytes(const std::vector<uint8_t>& v);
    manifest_file_string_schema_json_Union__1__();
};

struct manifest_file_string_schema_json_Union__2__ {
private:
    size_t idx_;
    boost::any value_;
public:
    size_t idx() const { return idx_; }
    bool is_null() const {
        return (idx_ == 0);
    }
    void set_null() {
        idx_ = 0;
        value_ = boost::any();
    }
    std::vector<uint8_t> get_bytes() const;
    void set_bytes(const std::vector<uint8_t>& v);
    manifest_file_string_schema_json_Union__2__();
};

struct r508 {
    typedef manifest_file_string_schema_json_Union__0__ contains_nan_t;
    typedef manifest_file_string_schema_json_Union__1__ lower_bound_t;
    typedef manifest_file_string_schema_json_Union__2__ upper_bound_t;
    contains_nan_t contains_nan;
    bool contains_null;
    lower_bound_t lower_bound;
    upper_bound_t upper_bound;
    r508() :
        contains_nan(contains_nan_t()),
        contains_null(bool()),
        lower_bound(lower_bound_t()),
        upper_bound(upper_bound_t())
        { }
};

struct manifest_file_string_schema_json_Union__3__ {
private:
    size_t idx_;
    boost::any value_;
public:
    size_t idx() const { return idx_; }
    bool is_null() const {
        return (idx_ == 0);
    }
    void set_null() {
        idx_ = 0;
        value_ = boost::any();
    }
    std::vector<r508 > get_array() const;
    void set_array(const std::vector<r508 >& v);
    manifest_file_string_schema_json_Union__3__();
};

struct manifest_file {
    typedef manifest_file_string_schema_json_Union__3__ partitions_t;
    std::string manifest_path;
    int64_t manifest_length;
    int32_t partition_spec_id;
    int32_t content;
    int64_t sequence_number;
    int64_t min_sequence_number;
    int64_t added_snapshot_id;
    int32_t added_data_files_count;
    int32_t existing_data_files_count;
    int32_t deleted_data_files_count;
    int64_t added_rows_count;
    int64_t existing_rows_count;
    int64_t deleted_rows_count;
    partitions_t partitions;
    manifest_file() :
        manifest_path(std::string()),
        manifest_length(int64_t()),
        partition_spec_id(int32_t()),
        content(int32_t()),
        sequence_number(int64_t()),
        min_sequence_number(int64_t()),
        added_snapshot_id(int64_t()),
        added_data_files_count(int32_t()),
        existing_data_files_count(int32_t()),
        deleted_data_files_count(int32_t()),
        added_rows_count(int64_t()),
        existing_rows_count(int64_t()),
        deleted_rows_count(int64_t()),
        partitions(partitions_t())
        { }
};

inline
bool manifest_file_string_schema_json_Union__0__::get_bool() const {
    if (idx_ != 1) {
        throw avro::Exception("Invalid type for union");
    }
    return boost::any_cast<bool >(value_);
}

inline
void manifest_file_string_schema_json_Union__0__::set_bool(const bool& v) {
    idx_ = 1;
    value_ = v;
}

inline
std::vector<uint8_t> manifest_file_string_schema_json_Union__1__::get_bytes() const {
    if (idx_ != 1) {
        throw avro::Exception("Invalid type for union");
    }
    return boost::any_cast<std::vector<uint8_t> >(value_);
}

inline
void manifest_file_string_schema_json_Union__1__::set_bytes(const std::vector<uint8_t>& v) {
    idx_ = 1;
    value_ = v;
}

inline
std::vector<uint8_t> manifest_file_string_schema_json_Union__2__::get_bytes() const {
    if (idx_ != 1) {
        throw avro::Exception("Invalid type for union");
    }
    return boost::any_cast<std::vector<uint8_t> >(value_);
}

inline
void manifest_file_string_schema_json_Union__2__::set_bytes(const std::vector<uint8_t>& v) {
    idx_ = 1;
    value_ = v;
}

inline
std::vector<r508 > manifest_file_string_schema_json_Union__3__::get_array() const {
    if (idx_ != 1) {
        throw avro::Exception("Invalid type for union");
    }
    return boost::any_cast<std::vector<r508 > >(value_);
}

inline
void manifest_file_string_schema_json_Union__3__::set_array(const std::vector<r508 >& v) {
    idx_ = 1;
    value_ = v;
}

inline manifest_file_string_schema_json_Union__0__::manifest_file_string_schema_json_Union__0__() : idx_(0) { }
inline manifest_file_string_schema_json_Union__1__::manifest_file_string_schema_json_Union__1__() : idx_(0) { }
inline manifest_file_string_schema_json_Union__2__::manifest_file_string_schema_json_Union__2__() : idx_(0) { }
inline manifest_file_string_schema_json_Union__3__::manifest_file_string_schema_json_Union__3__() : idx_(0) { }
namespace avro {
template<> struct codec_traits<manifest_file_string_schema_json_Union__0__> {
    static void encode(Encoder& e, manifest_file_string_schema_json_Union__0__ v) {
        e.encodeUnionIndex(v.idx());
        switch (v.idx()) {
        case 0:
            e.encodeNull();
            break;
        case 1:
            avro::encode(e, v.get_bool());
            break;
        }
    }
    static void decode(Decoder& d, manifest_file_string_schema_json_Union__0__& v) {
        size_t n = d.decodeUnionIndex();
        if (n >= 2) { throw avro::Exception("Union index too big"); }
        switch (n) {
        case 0:
            d.decodeNull();
            v.set_null();
            break;
        case 1:
            {
                bool vv;
                avro::decode(d, vv);
                v.set_bool(vv);
            }
            break;
        }
    }
};

template<> struct codec_traits<manifest_file_string_schema_json_Union__1__> {
    static void encode(Encoder& e, manifest_file_string_schema_json_Union__1__ v) {
        e.encodeUnionIndex(v.idx());
        switch (v.idx()) {
        case 0:
            e.encodeNull();
            break;
        case 1:
            avro::encode(e, v.get_bytes());
            break;
        }
    }
    static void decode(Decoder& d, manifest_file_string_schema_json_Union__1__& v) {
        size_t n = d.decodeUnionIndex();
        if (n >= 2) { throw avro::Exception("Union index too big"); }
        switch (n) {
        case 0:
            d.decodeNull();
            v.set_null();
            break;
        case 1:
            {
                std::vector<uint8_t> vv;
                avro::decode(d, vv);
                v.set_bytes(vv);
            }
            break;
        }
    }
};

template<> struct codec_traits<manifest_file_string_schema_json_Union__2__> {
    static void encode(Encoder& e, manifest_file_string_schema_json_Union__2__ v) {
        e.encodeUnionIndex(v.idx());
        switch (v.idx()) {
        case 0:
            e.encodeNull();
            break;
        case 1:
            avro::encode(e, v.get_bytes());
            break;
        }
    }
    static void decode(Decoder& d, manifest_file_string_schema_json_Union__2__& v) {
        size_t n = d.decodeUnionIndex();
        if (n >= 2) { throw avro::Exception("Union index too big"); }
        switch (n) {
        case 0:
            d.decodeNull();
            v.set_null();
            break;
        case 1:
            {
                std::vector<uint8_t> vv;
                avro::decode(d, vv);
                v.set_bytes(vv);
            }
            break;
        }
    }
};

template<> struct codec_traits<r508> {
    static void encode(Encoder& e, const r508& v) {
        avro::encode(e, v.contains_nan);
        avro::encode(e, v.contains_null);
        avro::encode(e, v.lower_bound);
        avro::encode(e, v.upper_bound);
    }
    static void decode(Decoder& d, r508& v) {
        if (avro::ResolvingDecoder *rd =
            dynamic_cast<avro::ResolvingDecoder *>(&d)) {
            const std::vector<size_t> fo = rd->fieldOrder();
            for (std::vector<size_t>::const_iterator it = fo.begin();
                it != fo.end(); ++it) {
                switch (*it) {
                case 0:
                    avro::decode(d, v.contains_nan);
                    break;
                case 1:
                    avro::decode(d, v.contains_null);
                    break;
                case 2:
                    avro::decode(d, v.lower_bound);
                    break;
                case 3:
                    avro::decode(d, v.upper_bound);
                    break;
                default:
                    break;
                }
            }
        } else {
            avro::decode(d, v.contains_nan);
            avro::decode(d, v.contains_null);
            avro::decode(d, v.lower_bound);
            avro::decode(d, v.upper_bound);
        }
    }
};

template<> struct codec_traits<manifest_file_string_schema_json_Union__3__> {
    static void encode(Encoder& e, manifest_file_string_schema_json_Union__3__ v) {
        e.encodeUnionIndex(v.idx());
        switch (v.idx()) {
        case 0:
            e.encodeNull();
            break;
        case 1:
            avro::encode(e, v.get_array());
            break;
        }
    }
    static void decode(Decoder& d, manifest_file_string_schema_json_Union__3__& v) {
        size_t n = d.decodeUnionIndex();
        if (n >= 2) { throw avro::Exception("Union index too big"); }
        switch (n) {
        case 0:
            d.decodeNull();
            v.set_null();
            break;
        case 1:
            {
                std::vector<r508 > vv;
                avro::decode(d, vv);
                v.set_array(vv);
            }
            break;
        }
    }
};

template<> struct codec_traits<manifest_file> {
    static void encode(Encoder& e, const manifest_file& v) {
        avro::encode(e, v.manifest_path);
        avro::encode(e, v.manifest_length);
        avro::encode(e, v.partition_spec_id);
        avro::encode(e, v.content);
        avro::encode(e, v.sequence_number);
        avro::encode(e, v.min_sequence_number);
        avro::encode(e, v.added_snapshot_id);
        avro::encode(e, v.added_data_files_count);
        avro::encode(e, v.existing_data_files_count);
        avro::encode(e, v.deleted_data_files_count);
        avro::encode(e, v.added_rows_count);
        avro::encode(e, v.existing_rows_count);
        avro::encode(e, v.deleted_rows_count);
        avro::encode(e, v.partitions);
    }
    static void decode(Decoder& d, manifest_file& v) {
        if (avro::ResolvingDecoder *rd =
            dynamic_cast<avro::ResolvingDecoder *>(&d)) {
            const std::vector<size_t> fo = rd->fieldOrder();
            for (std::vector<size_t>::const_iterator it = fo.begin();
                it != fo.end(); ++it) {
                switch (*it) {
                case 0:
                    avro::decode(d, v.manifest_path);
                    break;
                case 1:
                    avro::decode(d, v.manifest_length);
                    break;
                case 2:
                    avro::decode(d, v.partition_spec_id);
                    break;
                case 3:
                    avro::decode(d, v.content);
                    break;
                case 4:
                    avro::decode(d, v.sequence_number);
                    break;
                case 5:
                    avro::decode(d, v.min_sequence_number);
                    break;
                case 6:
                    avro::decode(d, v.added_snapshot_id);
                    break;
                case 7:
                    avro::decode(d, v.added_data_files_count);
                    break;
                case 8:
                    avro::decode(d, v.existing_data_files_count);
                    break;
                case 9:
                    avro::decode(d, v.deleted_data_files_count);
                    break;
                case 10:
                    avro::decode(d, v.added_rows_count);
                    break;
                case 11:
                    avro::decode(d, v.existing_rows_count);
                    break;
                case 12:
                    avro::decode(d, v.deleted_rows_count);
                    break;
                case 13:
                    avro::decode(d, v.partitions);
                    break;
                default:
                    break;
                }
            }
        } else {
            avro::decode(d, v.manifest_path);
            avro::decode(d, v.manifest_length);
            avro::decode(d, v.partition_spec_id);
            avro::decode(d, v.content);
            avro::decode(d, v.sequence_number);
            avro::decode(d, v.min_sequence_number);
            avro::decode(d, v.added_snapshot_id);
            avro::decode(d, v.added_data_files_count);
            avro::decode(d, v.existing_data_files_count);
            avro::decode(d, v.deleted_data_files_count);
            avro::decode(d, v.added_rows_count);
            avro::decode(d, v.existing_rows_count);
            avro::decode(d, v.deleted_rows_count);
            avro::decode(d, v.partitions);
        }
    }
};

}
#endif

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


#ifndef MANIFEST_ENTRY_HH_3311231037_H
#define MANIFEST_ENTRY_HH_3311231037_H


#include <sstream>
#include "boost/any.hpp"
#include "avro/Specific.hh"
#include "avro/Encoder.hh"
#include "avro/Decoder.hh"

struct manifest_entry_string_schema_json_Union__0__ {
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
    int64_t get_long() const;
    void set_long(const int64_t& v);
    manifest_entry_string_schema_json_Union__0__();
};

struct manifest_entry_string_schema_json_Union__1__ {
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
    int64_t get_long() const;
    void set_long(const int64_t& v);
    manifest_entry_string_schema_json_Union__1__();
};

struct r102 {
    r102()
        { }
};

struct k117_v118 {
    int32_t key;
    int64_t value;
    k117_v118() :
        key(int32_t()),
        value(int64_t())
        { }
};

struct manifest_entry_string_schema_json_Union__2__ {
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
    std::vector<k117_v118 > get_array() const;
    void set_array(const std::vector<k117_v118 >& v);
    manifest_entry_string_schema_json_Union__2__();
};

struct k119_v120 {
    int32_t key;
    int64_t value;
    k119_v120() :
        key(int32_t()),
        value(int64_t())
        { }
};

struct manifest_entry_string_schema_json_Union__3__ {
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
    std::vector<k119_v120 > get_array() const;
    void set_array(const std::vector<k119_v120 >& v);
    manifest_entry_string_schema_json_Union__3__();
};

struct k121_v122 {
    int32_t key;
    int64_t value;
    k121_v122() :
        key(int32_t()),
        value(int64_t())
        { }
};

struct manifest_entry_string_schema_json_Union__4__ {
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
    std::vector<k121_v122 > get_array() const;
    void set_array(const std::vector<k121_v122 >& v);
    manifest_entry_string_schema_json_Union__4__();
};

struct k138_v139 {
    int32_t key;
    int64_t value;
    k138_v139() :
        key(int32_t()),
        value(int64_t())
        { }
};

struct manifest_entry_string_schema_json_Union__5__ {
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
    std::vector<k138_v139 > get_array() const;
    void set_array(const std::vector<k138_v139 >& v);
    manifest_entry_string_schema_json_Union__5__();
};

struct k126_v127 {
    int32_t key;
    std::vector<uint8_t> value;
    k126_v127() :
        key(int32_t()),
        value(std::vector<uint8_t>())
        { }
};

struct manifest_entry_string_schema_json_Union__6__ {
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
    std::vector<k126_v127 > get_array() const;
    void set_array(const std::vector<k126_v127 >& v);
    manifest_entry_string_schema_json_Union__6__();
};

struct k129_v130 {
    int32_t key;
    std::vector<uint8_t> value;
    k129_v130() :
        key(int32_t()),
        value(std::vector<uint8_t>())
        { }
};

struct manifest_entry_string_schema_json_Union__7__ {
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
    std::vector<k129_v130 > get_array() const;
    void set_array(const std::vector<k129_v130 >& v);
    manifest_entry_string_schema_json_Union__7__();
};

struct manifest_entry_string_schema_json_Union__8__ {
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
    manifest_entry_string_schema_json_Union__8__();
};

struct manifest_entry_string_schema_json_Union__9__ {
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
    std::vector<int64_t > get_array() const;
    void set_array(const std::vector<int64_t >& v);
    manifest_entry_string_schema_json_Union__9__();
};

struct manifest_entry_string_schema_json_Union__10__ {
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
    std::vector<int32_t > get_array() const;
    void set_array(const std::vector<int32_t >& v);
    manifest_entry_string_schema_json_Union__10__();
};

struct manifest_entry_string_schema_json_Union__11__ {
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
    int32_t get_int() const;
    void set_int(const int32_t& v);
    manifest_entry_string_schema_json_Union__11__();
};

struct r2 {
    typedef manifest_entry_string_schema_json_Union__2__ column_sizes_t;
    typedef manifest_entry_string_schema_json_Union__3__ value_counts_t;
    typedef manifest_entry_string_schema_json_Union__4__ null_value_counts_t;
    typedef manifest_entry_string_schema_json_Union__5__ nan_value_counts_t;
    typedef manifest_entry_string_schema_json_Union__6__ lower_bounds_t;
    typedef manifest_entry_string_schema_json_Union__7__ upper_bounds_t;
    typedef manifest_entry_string_schema_json_Union__8__ key_metadata_t;
    typedef manifest_entry_string_schema_json_Union__9__ split_offsets_t;
    typedef manifest_entry_string_schema_json_Union__10__ equality_ids_t;
    typedef manifest_entry_string_schema_json_Union__11__ sort_order_id_t;
    int32_t content;
    std::string file_path;
    std::string file_format;
    r102 partition;
    int64_t record_count;
    int64_t file_size_in_bytes;
    column_sizes_t column_sizes;
    value_counts_t value_counts;
    null_value_counts_t null_value_counts;
    nan_value_counts_t nan_value_counts;
    lower_bounds_t lower_bounds;
    upper_bounds_t upper_bounds;
    key_metadata_t key_metadata;
    split_offsets_t split_offsets;
    equality_ids_t equality_ids;
    sort_order_id_t sort_order_id;
    r2() :
        content(int32_t()),
        file_path(std::string()),
        file_format(std::string()),
        partition(r102()),
        record_count(int64_t()),
        file_size_in_bytes(int64_t()),
        column_sizes(column_sizes_t()),
        value_counts(value_counts_t()),
        null_value_counts(null_value_counts_t()),
        nan_value_counts(nan_value_counts_t()),
        lower_bounds(lower_bounds_t()),
        upper_bounds(upper_bounds_t()),
        key_metadata(key_metadata_t()),
        split_offsets(split_offsets_t()),
        equality_ids(equality_ids_t()),
        sort_order_id(sort_order_id_t())
        { }
};

struct manifest_entry {
    typedef manifest_entry_string_schema_json_Union__0__ snapshot_id_t;
    typedef manifest_entry_string_schema_json_Union__1__ sequence_number_t;
    int32_t status;
    snapshot_id_t snapshot_id;
    sequence_number_t sequence_number;
    r2 data_file;
    manifest_entry() :
        status(int32_t()),
        snapshot_id(snapshot_id_t()),
        sequence_number(sequence_number_t()),
        data_file(r2())
        { }
};

inline
int64_t manifest_entry_string_schema_json_Union__0__::get_long() const {
    if (idx_ != 1) {
        throw avro::Exception("Invalid type for union");
    }
    return boost::any_cast<int64_t >(value_);
}

inline
void manifest_entry_string_schema_json_Union__0__::set_long(const int64_t& v) {
    idx_ = 1;
    value_ = v;
}

inline
int64_t manifest_entry_string_schema_json_Union__1__::get_long() const {
    if (idx_ != 1) {
        throw avro::Exception("Invalid type for union");
    }
    return boost::any_cast<int64_t >(value_);
}

inline
void manifest_entry_string_schema_json_Union__1__::set_long(const int64_t& v) {
    idx_ = 1;
    value_ = v;
}

inline
std::vector<k117_v118 > manifest_entry_string_schema_json_Union__2__::get_array() const {
    if (idx_ != 1) {
        throw avro::Exception("Invalid type for union");
    }
    return boost::any_cast<std::vector<k117_v118 > >(value_);
}

inline
void manifest_entry_string_schema_json_Union__2__::set_array(const std::vector<k117_v118 >& v) {
    idx_ = 1;
    value_ = v;
}

inline
std::vector<k119_v120 > manifest_entry_string_schema_json_Union__3__::get_array() const {
    if (idx_ != 1) {
        throw avro::Exception("Invalid type for union");
    }
    return boost::any_cast<std::vector<k119_v120 > >(value_);
}

inline
void manifest_entry_string_schema_json_Union__3__::set_array(const std::vector<k119_v120 >& v) {
    idx_ = 1;
    value_ = v;
}

inline
std::vector<k121_v122 > manifest_entry_string_schema_json_Union__4__::get_array() const {
    if (idx_ != 1) {
        throw avro::Exception("Invalid type for union");
    }
    return boost::any_cast<std::vector<k121_v122 > >(value_);
}

inline
void manifest_entry_string_schema_json_Union__4__::set_array(const std::vector<k121_v122 >& v) {
    idx_ = 1;
    value_ = v;
}

inline
std::vector<k138_v139 > manifest_entry_string_schema_json_Union__5__::get_array() const {
    if (idx_ != 1) {
        throw avro::Exception("Invalid type for union");
    }
    return boost::any_cast<std::vector<k138_v139 > >(value_);
}

inline
void manifest_entry_string_schema_json_Union__5__::set_array(const std::vector<k138_v139 >& v) {
    idx_ = 1;
    value_ = v;
}

inline
std::vector<k126_v127 > manifest_entry_string_schema_json_Union__6__::get_array() const {
    if (idx_ != 1) {
        throw avro::Exception("Invalid type for union");
    }
    return boost::any_cast<std::vector<k126_v127 > >(value_);
}

inline
void manifest_entry_string_schema_json_Union__6__::set_array(const std::vector<k126_v127 >& v) {
    idx_ = 1;
    value_ = v;
}

inline
std::vector<k129_v130 > manifest_entry_string_schema_json_Union__7__::get_array() const {
    if (idx_ != 1) {
        throw avro::Exception("Invalid type for union");
    }
    return boost::any_cast<std::vector<k129_v130 > >(value_);
}

inline
void manifest_entry_string_schema_json_Union__7__::set_array(const std::vector<k129_v130 >& v) {
    idx_ = 1;
    value_ = v;
}

inline
std::vector<uint8_t> manifest_entry_string_schema_json_Union__8__::get_bytes() const {
    if (idx_ != 1) {
        throw avro::Exception("Invalid type for union");
    }
    return boost::any_cast<std::vector<uint8_t> >(value_);
}

inline
void manifest_entry_string_schema_json_Union__8__::set_bytes(const std::vector<uint8_t>& v) {
    idx_ = 1;
    value_ = v;
}

inline
std::vector<int64_t > manifest_entry_string_schema_json_Union__9__::get_array() const {
    if (idx_ != 1) {
        throw avro::Exception("Invalid type for union");
    }
    return boost::any_cast<std::vector<int64_t > >(value_);
}

inline
void manifest_entry_string_schema_json_Union__9__::set_array(const std::vector<int64_t >& v) {
    idx_ = 1;
    value_ = v;
}

inline
std::vector<int32_t > manifest_entry_string_schema_json_Union__10__::get_array() const {
    if (idx_ != 1) {
        throw avro::Exception("Invalid type for union");
    }
    return boost::any_cast<std::vector<int32_t > >(value_);
}

inline
void manifest_entry_string_schema_json_Union__10__::set_array(const std::vector<int32_t >& v) {
    idx_ = 1;
    value_ = v;
}

inline
int32_t manifest_entry_string_schema_json_Union__11__::get_int() const {
    if (idx_ != 1) {
        throw avro::Exception("Invalid type for union");
    }
    return boost::any_cast<int32_t >(value_);
}

inline
void manifest_entry_string_schema_json_Union__11__::set_int(const int32_t& v) {
    idx_ = 1;
    value_ = v;
}

inline manifest_entry_string_schema_json_Union__0__::manifest_entry_string_schema_json_Union__0__() : idx_(0) { }
inline manifest_entry_string_schema_json_Union__1__::manifest_entry_string_schema_json_Union__1__() : idx_(0) { }
inline manifest_entry_string_schema_json_Union__2__::manifest_entry_string_schema_json_Union__2__() : idx_(0) { }
inline manifest_entry_string_schema_json_Union__3__::manifest_entry_string_schema_json_Union__3__() : idx_(0) { }
inline manifest_entry_string_schema_json_Union__4__::manifest_entry_string_schema_json_Union__4__() : idx_(0) { }
inline manifest_entry_string_schema_json_Union__5__::manifest_entry_string_schema_json_Union__5__() : idx_(0) { }
inline manifest_entry_string_schema_json_Union__6__::manifest_entry_string_schema_json_Union__6__() : idx_(0) { }
inline manifest_entry_string_schema_json_Union__7__::manifest_entry_string_schema_json_Union__7__() : idx_(0) { }
inline manifest_entry_string_schema_json_Union__8__::manifest_entry_string_schema_json_Union__8__() : idx_(0) { }
inline manifest_entry_string_schema_json_Union__9__::manifest_entry_string_schema_json_Union__9__() : idx_(0) { }
inline manifest_entry_string_schema_json_Union__10__::manifest_entry_string_schema_json_Union__10__() : idx_(0) { }
inline manifest_entry_string_schema_json_Union__11__::manifest_entry_string_schema_json_Union__11__() : idx_(0) { }
namespace avro {
template<> struct codec_traits<manifest_entry_string_schema_json_Union__0__> {
    static void encode(Encoder& e, manifest_entry_string_schema_json_Union__0__ v) {
        e.encodeUnionIndex(v.idx());
        switch (v.idx()) {
        case 0:
            e.encodeNull();
            break;
        case 1:
            avro::encode(e, v.get_long());
            break;
        }
    }
    static void decode(Decoder& d, manifest_entry_string_schema_json_Union__0__& v) {
        size_t n = d.decodeUnionIndex();
        if (n >= 2) { throw avro::Exception("Union index too big"); }
        switch (n) {
        case 0:
            d.decodeNull();
            v.set_null();
            break;
        case 1:
            {
                int64_t vv;
                avro::decode(d, vv);
                v.set_long(vv);
            }
            break;
        }
    }
};

template<> struct codec_traits<manifest_entry_string_schema_json_Union__1__> {
    static void encode(Encoder& e, manifest_entry_string_schema_json_Union__1__ v) {
        e.encodeUnionIndex(v.idx());
        switch (v.idx()) {
        case 0:
            e.encodeNull();
            break;
        case 1:
            avro::encode(e, v.get_long());
            break;
        }
    }
    static void decode(Decoder& d, manifest_entry_string_schema_json_Union__1__& v) {
        size_t n = d.decodeUnionIndex();
        if (n >= 2) { throw avro::Exception("Union index too big"); }
        switch (n) {
        case 0:
            d.decodeNull();
            v.set_null();
            break;
        case 1:
            {
                int64_t vv;
                avro::decode(d, vv);
                v.set_long(vv);
            }
            break;
        }
    }
};

template<> struct codec_traits<r102> {
    static void encode(Encoder& e, const r102& v) {
    }
    static void decode(Decoder& d, r102& v) {
        if (avro::ResolvingDecoder *rd =
            dynamic_cast<avro::ResolvingDecoder *>(&d)) {
            const std::vector<size_t> fo = rd->fieldOrder();
            for (std::vector<size_t>::const_iterator it = fo.begin();
                it != fo.end(); ++it) {
                switch (*it) {
                default:
                    break;
                }
            }
        } else {
        }
    }
};

template<> struct codec_traits<k117_v118> {
    static void encode(Encoder& e, const k117_v118& v) {
        avro::encode(e, v.key);
        avro::encode(e, v.value);
    }
    static void decode(Decoder& d, k117_v118& v) {
        if (avro::ResolvingDecoder *rd =
            dynamic_cast<avro::ResolvingDecoder *>(&d)) {
            const std::vector<size_t> fo = rd->fieldOrder();
            for (std::vector<size_t>::const_iterator it = fo.begin();
                it != fo.end(); ++it) {
                switch (*it) {
                case 0:
                    avro::decode(d, v.key);
                    break;
                case 1:
                    avro::decode(d, v.value);
                    break;
                default:
                    break;
                }
            }
        } else {
            avro::decode(d, v.key);
            avro::decode(d, v.value);
        }
    }
};

template<> struct codec_traits<manifest_entry_string_schema_json_Union__2__> {
    static void encode(Encoder& e, manifest_entry_string_schema_json_Union__2__ v) {
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
    static void decode(Decoder& d, manifest_entry_string_schema_json_Union__2__& v) {
        size_t n = d.decodeUnionIndex();
        if (n >= 2) { throw avro::Exception("Union index too big"); }
        switch (n) {
        case 0:
            d.decodeNull();
            v.set_null();
            break;
        case 1:
            {
                std::vector<k117_v118 > vv;
                avro::decode(d, vv);
                v.set_array(vv);
            }
            break;
        }
    }
};

template<> struct codec_traits<k119_v120> {
    static void encode(Encoder& e, const k119_v120& v) {
        avro::encode(e, v.key);
        avro::encode(e, v.value);
    }
    static void decode(Decoder& d, k119_v120& v) {
        if (avro::ResolvingDecoder *rd =
            dynamic_cast<avro::ResolvingDecoder *>(&d)) {
            const std::vector<size_t> fo = rd->fieldOrder();
            for (std::vector<size_t>::const_iterator it = fo.begin();
                it != fo.end(); ++it) {
                switch (*it) {
                case 0:
                    avro::decode(d, v.key);
                    break;
                case 1:
                    avro::decode(d, v.value);
                    break;
                default:
                    break;
                }
            }
        } else {
            avro::decode(d, v.key);
            avro::decode(d, v.value);
        }
    }
};

template<> struct codec_traits<manifest_entry_string_schema_json_Union__3__> {
    static void encode(Encoder& e, manifest_entry_string_schema_json_Union__3__ v) {
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
    static void decode(Decoder& d, manifest_entry_string_schema_json_Union__3__& v) {
        size_t n = d.decodeUnionIndex();
        if (n >= 2) { throw avro::Exception("Union index too big"); }
        switch (n) {
        case 0:
            d.decodeNull();
            v.set_null();
            break;
        case 1:
            {
                std::vector<k119_v120 > vv;
                avro::decode(d, vv);
                v.set_array(vv);
            }
            break;
        }
    }
};

template<> struct codec_traits<k121_v122> {
    static void encode(Encoder& e, const k121_v122& v) {
        avro::encode(e, v.key);
        avro::encode(e, v.value);
    }
    static void decode(Decoder& d, k121_v122& v) {
        if (avro::ResolvingDecoder *rd =
            dynamic_cast<avro::ResolvingDecoder *>(&d)) {
            const std::vector<size_t> fo = rd->fieldOrder();
            for (std::vector<size_t>::const_iterator it = fo.begin();
                it != fo.end(); ++it) {
                switch (*it) {
                case 0:
                    avro::decode(d, v.key);
                    break;
                case 1:
                    avro::decode(d, v.value);
                    break;
                default:
                    break;
                }
            }
        } else {
            avro::decode(d, v.key);
            avro::decode(d, v.value);
        }
    }
};

template<> struct codec_traits<manifest_entry_string_schema_json_Union__4__> {
    static void encode(Encoder& e, manifest_entry_string_schema_json_Union__4__ v) {
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
    static void decode(Decoder& d, manifest_entry_string_schema_json_Union__4__& v) {
        size_t n = d.decodeUnionIndex();
        if (n >= 2) { throw avro::Exception("Union index too big"); }
        switch (n) {
        case 0:
            d.decodeNull();
            v.set_null();
            break;
        case 1:
            {
                std::vector<k121_v122 > vv;
                avro::decode(d, vv);
                v.set_array(vv);
            }
            break;
        }
    }
};

template<> struct codec_traits<k138_v139> {
    static void encode(Encoder& e, const k138_v139& v) {
        avro::encode(e, v.key);
        avro::encode(e, v.value);
    }
    static void decode(Decoder& d, k138_v139& v) {
        if (avro::ResolvingDecoder *rd =
            dynamic_cast<avro::ResolvingDecoder *>(&d)) {
            const std::vector<size_t> fo = rd->fieldOrder();
            for (std::vector<size_t>::const_iterator it = fo.begin();
                it != fo.end(); ++it) {
                switch (*it) {
                case 0:
                    avro::decode(d, v.key);
                    break;
                case 1:
                    avro::decode(d, v.value);
                    break;
                default:
                    break;
                }
            }
        } else {
            avro::decode(d, v.key);
            avro::decode(d, v.value);
        }
    }
};

template<> struct codec_traits<manifest_entry_string_schema_json_Union__5__> {
    static void encode(Encoder& e, manifest_entry_string_schema_json_Union__5__ v) {
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
    static void decode(Decoder& d, manifest_entry_string_schema_json_Union__5__& v) {
        size_t n = d.decodeUnionIndex();
        if (n >= 2) { throw avro::Exception("Union index too big"); }
        switch (n) {
        case 0:
            d.decodeNull();
            v.set_null();
            break;
        case 1:
            {
                std::vector<k138_v139 > vv;
                avro::decode(d, vv);
                v.set_array(vv);
            }
            break;
        }
    }
};

template<> struct codec_traits<k126_v127> {
    static void encode(Encoder& e, const k126_v127& v) {
        avro::encode(e, v.key);
        avro::encode(e, v.value);
    }
    static void decode(Decoder& d, k126_v127& v) {
        if (avro::ResolvingDecoder *rd =
            dynamic_cast<avro::ResolvingDecoder *>(&d)) {
            const std::vector<size_t> fo = rd->fieldOrder();
            for (std::vector<size_t>::const_iterator it = fo.begin();
                it != fo.end(); ++it) {
                switch (*it) {
                case 0:
                    avro::decode(d, v.key);
                    break;
                case 1:
                    avro::decode(d, v.value);
                    break;
                default:
                    break;
                }
            }
        } else {
            avro::decode(d, v.key);
            avro::decode(d, v.value);
        }
    }
};

template<> struct codec_traits<manifest_entry_string_schema_json_Union__6__> {
    static void encode(Encoder& e, manifest_entry_string_schema_json_Union__6__ v) {
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
    static void decode(Decoder& d, manifest_entry_string_schema_json_Union__6__& v) {
        size_t n = d.decodeUnionIndex();
        if (n >= 2) { throw avro::Exception("Union index too big"); }
        switch (n) {
        case 0:
            d.decodeNull();
            v.set_null();
            break;
        case 1:
            {
                std::vector<k126_v127 > vv;
                avro::decode(d, vv);
                v.set_array(vv);
            }
            break;
        }
    }
};

template<> struct codec_traits<k129_v130> {
    static void encode(Encoder& e, const k129_v130& v) {
        avro::encode(e, v.key);
        avro::encode(e, v.value);
    }
    static void decode(Decoder& d, k129_v130& v) {
        if (avro::ResolvingDecoder *rd =
            dynamic_cast<avro::ResolvingDecoder *>(&d)) {
            const std::vector<size_t> fo = rd->fieldOrder();
            for (std::vector<size_t>::const_iterator it = fo.begin();
                it != fo.end(); ++it) {
                switch (*it) {
                case 0:
                    avro::decode(d, v.key);
                    break;
                case 1:
                    avro::decode(d, v.value);
                    break;
                default:
                    break;
                }
            }
        } else {
            avro::decode(d, v.key);
            avro::decode(d, v.value);
        }
    }
};

template<> struct codec_traits<manifest_entry_string_schema_json_Union__7__> {
    static void encode(Encoder& e, manifest_entry_string_schema_json_Union__7__ v) {
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
    static void decode(Decoder& d, manifest_entry_string_schema_json_Union__7__& v) {
        size_t n = d.decodeUnionIndex();
        if (n >= 2) { throw avro::Exception("Union index too big"); }
        switch (n) {
        case 0:
            d.decodeNull();
            v.set_null();
            break;
        case 1:
            {
                std::vector<k129_v130 > vv;
                avro::decode(d, vv);
                v.set_array(vv);
            }
            break;
        }
    }
};

template<> struct codec_traits<manifest_entry_string_schema_json_Union__8__> {
    static void encode(Encoder& e, manifest_entry_string_schema_json_Union__8__ v) {
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
    static void decode(Decoder& d, manifest_entry_string_schema_json_Union__8__& v) {
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

template<> struct codec_traits<manifest_entry_string_schema_json_Union__9__> {
    static void encode(Encoder& e, manifest_entry_string_schema_json_Union__9__ v) {
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
    static void decode(Decoder& d, manifest_entry_string_schema_json_Union__9__& v) {
        size_t n = d.decodeUnionIndex();
        if (n >= 2) { throw avro::Exception("Union index too big"); }
        switch (n) {
        case 0:
            d.decodeNull();
            v.set_null();
            break;
        case 1:
            {
                std::vector<int64_t > vv;
                avro::decode(d, vv);
                v.set_array(vv);
            }
            break;
        }
    }
};

template<> struct codec_traits<manifest_entry_string_schema_json_Union__10__> {
    static void encode(Encoder& e, manifest_entry_string_schema_json_Union__10__ v) {
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
    static void decode(Decoder& d, manifest_entry_string_schema_json_Union__10__& v) {
        size_t n = d.decodeUnionIndex();
        if (n >= 2) { throw avro::Exception("Union index too big"); }
        switch (n) {
        case 0:
            d.decodeNull();
            v.set_null();
            break;
        case 1:
            {
                std::vector<int32_t > vv;
                avro::decode(d, vv);
                v.set_array(vv);
            }
            break;
        }
    }
};

template<> struct codec_traits<manifest_entry_string_schema_json_Union__11__> {
    static void encode(Encoder& e, manifest_entry_string_schema_json_Union__11__ v) {
        e.encodeUnionIndex(v.idx());
        switch (v.idx()) {
        case 0:
            e.encodeNull();
            break;
        case 1:
            avro::encode(e, v.get_int());
            break;
        }
    }
    static void decode(Decoder& d, manifest_entry_string_schema_json_Union__11__& v) {
        size_t n = d.decodeUnionIndex();
        if (n >= 2) { throw avro::Exception("Union index too big"); }
        switch (n) {
        case 0:
            d.decodeNull();
            v.set_null();
            break;
        case 1:
            {
                int32_t vv;
                avro::decode(d, vv);
                v.set_int(vv);
            }
            break;
        }
    }
};

template<> struct codec_traits<r2> {
    static void encode(Encoder& e, const r2& v) {
        avro::encode(e, v.content);
        avro::encode(e, v.file_path);
        avro::encode(e, v.file_format);
        avro::encode(e, v.partition);
        avro::encode(e, v.record_count);
        avro::encode(e, v.file_size_in_bytes);
        avro::encode(e, v.column_sizes);
        avro::encode(e, v.value_counts);
        avro::encode(e, v.null_value_counts);
        avro::encode(e, v.nan_value_counts);
        avro::encode(e, v.lower_bounds);
        avro::encode(e, v.upper_bounds);
        avro::encode(e, v.key_metadata);
        avro::encode(e, v.split_offsets);
        avro::encode(e, v.equality_ids);
        avro::encode(e, v.sort_order_id);
    }
    static void decode(Decoder& d, r2& v) {
        if (avro::ResolvingDecoder *rd =
            dynamic_cast<avro::ResolvingDecoder *>(&d)) {
            const std::vector<size_t> fo = rd->fieldOrder();
            for (std::vector<size_t>::const_iterator it = fo.begin();
                it != fo.end(); ++it) {
                switch (*it) {
                case 0:
                    avro::decode(d, v.content);
                    break;
                case 1:
                    avro::decode(d, v.file_path);
                    break;
                case 2:
                    avro::decode(d, v.file_format);
                    break;
                case 3:
                    avro::decode(d, v.partition);
                    break;
                case 4:
                    avro::decode(d, v.record_count);
                    break;
                case 5:
                    avro::decode(d, v.file_size_in_bytes);
                    break;
                case 6:
                    avro::decode(d, v.column_sizes);
                    break;
                case 7:
                    avro::decode(d, v.value_counts);
                    break;
                case 8:
                    avro::decode(d, v.null_value_counts);
                    break;
                case 9:
                    avro::decode(d, v.nan_value_counts);
                    break;
                case 10:
                    avro::decode(d, v.lower_bounds);
                    break;
                case 11:
                    avro::decode(d, v.upper_bounds);
                    break;
                case 12:
                    avro::decode(d, v.key_metadata);
                    break;
                case 13:
                    avro::decode(d, v.split_offsets);
                    break;
                case 14:
                    avro::decode(d, v.equality_ids);
                    break;
                case 15:
                    avro::decode(d, v.sort_order_id);
                    break;
                default:
                    break;
                }
            }
        } else {
            avro::decode(d, v.content);
            avro::decode(d, v.file_path);
            avro::decode(d, v.file_format);
            avro::decode(d, v.partition);
            avro::decode(d, v.record_count);
            avro::decode(d, v.file_size_in_bytes);
            avro::decode(d, v.column_sizes);
            avro::decode(d, v.value_counts);
            avro::decode(d, v.null_value_counts);
            avro::decode(d, v.nan_value_counts);
            avro::decode(d, v.lower_bounds);
            avro::decode(d, v.upper_bounds);
            avro::decode(d, v.key_metadata);
            avro::decode(d, v.split_offsets);
            avro::decode(d, v.equality_ids);
            avro::decode(d, v.sort_order_id);
        }
    }
};

template<> struct codec_traits<manifest_entry> {
    static void encode(Encoder& e, const manifest_entry& v) {
        avro::encode(e, v.status);
        avro::encode(e, v.snapshot_id);
        avro::encode(e, v.sequence_number);
        avro::encode(e, v.data_file);
    }
    static void decode(Decoder& d, manifest_entry& v) {
        if (avro::ResolvingDecoder *rd =
            dynamic_cast<avro::ResolvingDecoder *>(&d)) {
            const std::vector<size_t> fo = rd->fieldOrder();
            for (std::vector<size_t>::const_iterator it = fo.begin();
                it != fo.end(); ++it) {
                switch (*it) {
                case 0:
                    avro::decode(d, v.status);
                    break;
                case 1:
                    avro::decode(d, v.snapshot_id);
                    break;
                case 2:
                    avro::decode(d, v.sequence_number);
                    break;
                case 3:
                    avro::decode(d, v.data_file);
                    break;
                default:
                    break;
                }
            }
        } else {
            avro::decode(d, v.status);
            avro::decode(d, v.snapshot_id);
            avro::decode(d, v.sequence_number);
            avro::decode(d, v.data_file);
        }
    }
};

}
#endif

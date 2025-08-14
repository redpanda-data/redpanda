/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "bytes/iobuf.h"

namespace serde::json {

// The writer class is responsible for serializing data into JSON format.
//
// NOTE: This class is easily misused, it's recommended to use a DOM version
// (not yet written) or have good tests.
class writer {
public:
    void begin_object() {
        append_delimiter();
        _buf.append_str("{");
        _next_delimiter = '\0';
    }
    void end_object() {
        _buf.append_str("}");
        _next_delimiter = ',';
    }
    void key(std::string_view k) {
        append_delimiter();
        append_string(k);
        _next_delimiter = ':';
    }
    void key(const iobuf& k) {
        append_delimiter();
        append_string(k);
        _next_delimiter = ':';
    }
    void begin_array() {
        append_delimiter();
        _buf.append_str("[");
        _next_delimiter = '\0';
    }
    void end_array() {
        _buf.append_str("]");
        _next_delimiter = ',';
    }
    void null() {
        append_delimiter();
        _buf.append_str("null");
        _next_delimiter = ',';
    }
    void boolean(bool b) {
        append_delimiter();
        _buf.append_str(b ? "true" : "false");
        _next_delimiter = ',';
    }
    void string(const iobuf& b) {
        append_delimiter();
        append_string(b);
        _next_delimiter = ',';
    }
    void string(std::string_view b) {
        append_delimiter();
        append_string(b);
        _next_delimiter = ',';
    }
    void base64_string(const iobuf& b);
    void number(double d);
    void integer(int32_t i);
    void integer(uint32_t i);
    // Because JSON doesn't have a great story around numbers larger than 2^53
    // we encode larger numbers as strings, similar to proto3 json.
    void integer_string(int64_t i);
    void integer_string(uint64_t i);

    // Append raw JSON data to the buffer, the delimiters for lists and object
    // values are still added. Do not use this for object keys, but values only.
    //
    // This method is dangerous, as you need to ensure that the JSON is valid.
    void append_raw_json(iobuf&& b) {
        append_delimiter();
        _buf.append(std::move(b));
        _next_delimiter = ',';
    }

    iobuf&& finish() && { return std::move(_buf); }

private:
    void append_string(std::string_view);
    void append_string(const iobuf&);

    void append_delimiter() {
        if (_next_delimiter) {
            _buf.append_str({&_next_delimiter, 1});
        }
    }

    char _next_delimiter = '\0';
    iobuf _buf;
};

} // namespace serde::json

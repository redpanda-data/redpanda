// Copyright 2026 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "raft/fundamental.h"

#include "bytes/iobuf_parser.h"
#include "serde/rw/rw.h"

#include <array>
#include <cstring>

namespace {

constexpr size_t vnode_body_size = sizeof(int32_t) + sizeof(int64_t);

template<typename T>
void write_little_endian(char*& cursor, T value) {
    const auto little_endian = ss::cpu_to_le(value);
    std::memcpy(cursor, &little_endian, sizeof(little_endian));
    cursor += sizeof(little_endian);
}

template<typename T>
T read_little_endian(const char*& cursor) {
    T value;
    std::memcpy(&value, cursor, sizeof(value));
    cursor += sizeof(value);
    return ss::le_to_cpu(value);
}

} // namespace

namespace raft {

void vnode::serde_write(iobuf& out) const {
    std::array<char, vnode_body_size> encoded;
    char* cursor = encoded.data();
    write_little_endian(cursor, _node_id());
    write_little_endian(cursor, _revision());
    out.append(encoded.data(), encoded.size());
}

void vnode::serde_read(iobuf_parser& in, const serde::header& envelope) {
    const auto available = in.bytes_left() - envelope._bytes_left_limit;
    if (available >= vnode_body_size) {
        std::array<char, vnode_body_size> encoded;
        in.consume_to(encoded.size(), encoded.begin());
        const char* cursor = encoded.data();
        _node_id = model::node_id(read_little_endian<int32_t>(cursor));
        _revision = model::revision_id(read_little_endian<int64_t>(cursor));
        return;
    }

    if (in.bytes_left() != envelope._bytes_left_limit) {
        serde::read_nested(in, _node_id, envelope._bytes_left_limit);
    }
    if (in.bytes_left() != envelope._bytes_left_limit) {
        serde::read_nested(in, _revision, envelope._bytes_left_limit);
    }
}

} // namespace raft

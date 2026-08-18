// Copyright 2023 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#pragma once

#include "bytes/iobuf_parser.h"

#include <algorithm>
#include <cstddef>

namespace serde {

template<typename T>
concept Reservable = requires(T t) { t.reserve(0U); };

// Reserves room for an element count that came off the wire. Encoding an
// element always costs at least one byte, so a count larger than the bytes left
// in the parser cannot be honest, and reserving it would turn a corrupt size
// prefix into an allocation of any size the prefix asks for. The caller still
// reads exactly count elements and so still rejects the input when the bytes
// run out; this only bounds the capacity taken up front.
template<Reservable T>
void reserve_from_wire(T& t, std::size_t count, const iobuf_parser& in) {
    t.reserve(std::min(count, in.bytes_left()));
}

} // namespace serde

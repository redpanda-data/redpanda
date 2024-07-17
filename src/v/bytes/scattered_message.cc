/*
 * Copyright 2022 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */
#include "bytes/scattered_message.h"

ss::scattered_message<char> iobuf_as_scattered(iobuf b) {
    ss::scattered_message<char> msg;
    auto in = iobuf::iterator_consumer(b.cbegin(), b.cend());
    int32_t chunk_no = 0;
    in.consume(
      b.size_bytes(), [&msg, &chunk_no, &b](const char* src, size_t sz) {
          ++chunk_no;
          vassert(
            chunk_no <= std::numeric_limits<int16_t>::max(),
            "Invalid construction of scattered_message. fragment coutn exceeds "
            "max count:{}. Usually a bug with small append() to iobuf. {}",
            chunk_no,
            b);
          msg.append_static(src, sz);
          return ss::stop_iteration::no;
      });
    msg.on_delete([b = std::move(b)] {});
    return msg;
}

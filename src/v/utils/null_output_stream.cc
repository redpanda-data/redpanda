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

#include "utils/null_output_stream.h"

#include "base/units.h"

namespace utils {

ss::future<> null_data_sink::put(ss::net::packet data) {
    return put(data.release());
}

ss::future<> null_data_sink::put(std::vector<ss::temporary_buffer<char>> all) {
    return ss::do_with(
      std::move(all), [this](std::vector<ss::temporary_buffer<char>>& all) {
          return ss::do_for_each(all, [this](ss::temporary_buffer<char>& buf) {
              return put(std::move(buf));
          });
      });
}

ss::future<> null_data_sink::put(ss::temporary_buffer<char>) {
    return ss::now();
}

ss::future<> null_data_sink::flush() { return ss::now(); }
ss::future<> null_data_sink::close() { return ss::now(); }

ss::output_stream<char> make_null_output_stream() {
    auto ds = ss::data_sink(std::make_unique<null_data_sink>());
    ss::output_stream<char> ostr(std::move(ds), 4_KiB);
    return ostr;
}
} // namespace utils

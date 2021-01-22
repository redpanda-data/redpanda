/*
 * Copyright 2020 Vectorized, Inc.
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
#include "bytes/iobuf_parser.h"
#include "hashing/crc32c.h"
#include "outcome.h"
#include "reflection/adl.h"
#include "seastarx.h"
#include "utils/file_io.h"
#include "utils/state_crc_file_errc.h"

#include <seastar/core/file-types.hh>
#include <seastar/core/future.hh>
#include <seastar/core/seastar.hh>
#include <seastar/core/sstring.hh>
#include <seastar/core/temporary_buffer.hh>

#include <bits/stdint-uintn.h>
#include <boost/range/iterator_range_core.hpp>
#include <fmt/core.h>
#include <yaml-cpp/emittermanip.h>
#include <yaml-cpp/yaml.h>

#include <cstring>

namespace utils {

inline uint32_t crc_iobuf(const iobuf& buf) {
    crc32 crc;
    for (const auto& frag : buf) {
        crc.extend(frag.get(), frag.size());
    }

    return crc.value();
}

class state_crc_file {
public:
    explicit state_crc_file(ss::sstring name)
      : _filename(std::move(name)) {}

    template<typename T>
    ss::future<result<T>> read() {
        return read_fully(_filename.string()).then([](iobuf buf) {
            if (buf.empty()) {
                return result<T>(state_crc_file_errc::file_not_found);
            }
            auto parser = iobuf_parser(std::move(buf));
            auto expected_crc32 = reflection::adl<uint32_t>{}.from(parser);
            auto data_left = parser.share(parser.bytes_left());
            auto actual_crc = crc_iobuf(data_left);

            if (unlikely(actual_crc != expected_crc32)) {
                return result<T>(state_crc_file_errc::crc_mismatch);
            }
            return result<T>(reflection::adl<T>{}.from(std::move(data_left)));
        });
    }

    /// Writes serialized state to given file together with checksum
    /// Serialized content is prepended with 4 bytes of CRC32
    ///
    /// |crc|data|
    template<typename T>
    ss::future<> persist(const T& state) {
        iobuf buf = reflection::to_iobuf(state);
        auto crc = crc_iobuf(buf);
        // prepend data with CRC
        buf.prepend(reflection::to_iobuf(crc));

        return write_fully(_filename, std::move(buf));
    }

private:
    static constexpr const size_t buf_size = 4096;
    std::filesystem::path _filename;
};
} // namespace utils

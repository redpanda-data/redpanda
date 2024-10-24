/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "cloud_storage/inventory/utils.h"

#include "bytes/iobuf.h"
#include "bytes/iostream.h"
#include "cloud_storage/logger.h"
#include "serde/rw/scalar.h"
#include "serde/rw/vector.h"

#include <seastar/core/fstream.hh>
#include <seastar/core/seastar.hh>
#include <seastar/coroutine/as_future.hh>

namespace {

ss::future<> write_hashes_to_file(
  ss::output_stream<char>& stream, fragmented_vector<uint64_t> hashes) {
    iobuf serialized;
    serde::write(serialized, std::move(hashes));

    iobuf chunk_size;
    serde::write(chunk_size, serialized.size_bytes());

    co_await write_iobuf_to_output_stream(std::move(chunk_size), stream);

    for (const auto& frag : serialized) {
        co_await stream.write(frag.get(), frag.size());
    }
}

ss::future<>
write_hashes_to_file(ss::file& f, fragmented_vector<uint64_t> hashes) {
    std::exception_ptr ep;
    auto stream = co_await ss::make_file_output_stream(f);
    auto res = co_await ss::coroutine::as_future(
      write_hashes_to_file(stream, std::move(hashes)));
    co_await stream.close();
    if (res.failed()) {
        std::rethrow_exception(res.get_exception());
    }
}

} // namespace

namespace cloud_storage::inventory {

ss::future<> flush_ntp_hashes(
  std::filesystem::path root,
  model::ntp ntp,
  fragmented_vector<uint64_t> hashes,
  uint64_t file_name) {
    auto ntp_hash_path = root / std::string_view{ntp.path()};
    const auto ntp_hash_dir = ntp_hash_path.string();

    if (!co_await ss::file_exists(ntp_hash_dir)) {
        co_await ss::recursive_touch_directory(ntp_hash_dir);
    }

    ntp_hash_path /= fmt::format("{}", file_name);
    vlog(
      cst_log.trace,
      "Writing {} hashe(s) to file {}",
      hashes.size(),
      ntp_hash_path);
    co_return co_await ss::with_file_close_on_failure(
      ss::open_file_dma(
        ntp_hash_path.string(), ss::open_flags::create | ss::open_flags::wo),
      [h = std::move(hashes)](auto& f) mutable {
          return write_hashes_to_file(f, std::move(h));
      });
}

} // namespace cloud_storage::inventory

// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "storage/snapshot.h"

#include "base/vlog.h"
#include "bytes/iobuf_parser.h"
#include "bytes/iostream.h"
#include "hashing/crc32c.h"
#include "random/generators.h"
#include "reflection/adl.h"
#include "storage/logger.h"
#include "storage/segment_utils.h"
#include "utils/directory_walker.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/future.hh>
#include <seastar/core/iostream.hh>
#include <seastar/core/seastar.hh>

#include <cstdint>
#include <filesystem>
#include <regex>

namespace storage {

ss::future<std::optional<ss::file>>
snapshot_manager::open_snapshot_file(const ss::sstring& filename) const {
    auto path = snapshot_path(filename);
    auto exists = co_await ss::file_exists(path.string());
    if (!exists) {
        co_return std::nullopt;
    }
    co_return co_await ss::open_file_dma(path.string(), ss::open_flags::ro);
}

ss::future<std::optional<snapshot_reader>>
snapshot_manager::open_snapshot(ss::sstring filename) {
    auto path = snapshot_path(filename);
    auto maybe_file = co_await open_snapshot_file(filename);
    if (!maybe_file.has_value()) {
        co_return std::nullopt;
    }
    // ss::file::~file will automatically close the file. so no
    // worries about leaking an fd if something goes wrong here.
    ss::file_input_stream_options options;
    options.io_priority_class = _io_prio;
    auto input = ss::make_file_input_stream(maybe_file.value(), options);
    co_return snapshot_reader(maybe_file.value(), std::move(input), path);
}

ss::future<uint64_t> snapshot_manager::get_snapshot_size(ss::sstring filename) {
    auto path = snapshot_path(std::move(filename));
    return ss::file_size(path.string())
      .handle_exception([](const std::exception_ptr&) {
          return ss::make_ready_future<uint64_t>(0);
      });
}

ss::future<file_snapshot_writer>
snapshot_manager::start_snapshot(ss::sstring target) {
    // the random suffix is added because the lowres clock doesn't produce
    // unique file names when tests run fast.
    auto filename = fmt::format(
      "{}.partial.{}.{}",
      _partial_prefix,
      std::chrono::duration_cast<std::chrono::milliseconds>(
        ss::lowres_system_clock::now().time_since_epoch())
        .count(),
      random_generators::gen_alphanum_string(4));

    auto path = _dir / filename;

    const auto flags = ss::open_flags::wo | ss::open_flags::create
                       | ss::open_flags::exclusive;

    return internal::make_handle(path, flags, {}, {})
      .then([this, path](ss::file file) {
          ss::file_output_stream_options options;
          options.io_priority_class = _io_prio;
          return ss::make_file_output_stream(std::move(file), options);
      })
      .then([this, target, path](ss::output_stream<char> output) {
          return file_snapshot_writer(
            std::move(output), path, snapshot_path(target));
      });
}

ss::future<> snapshot_manager::finish_snapshot(file_snapshot_writer& writer) {
    return ss::rename_file(writer.path().string(), writer.target().string())
      .then([this] { return ss::sync_directory(_dir.string()); });
}

ss::future<> snapshot_manager::remove_partial_snapshots() {
    std::regex re(fmt::format(
      R"(^{}\.partial\.(\d+)\.([a-zA-Z0-9]{{4}})$)", _partial_prefix));
    return directory_walker::walk(
      _dir.string(), [this, re = std::move(re)](ss::directory_entry ent) {
          if (!ent.type || *ent.type != ss::directory_entry_type::regular) {
              return ss::now();
          }
          std::cmatch match;
          if (std::regex_match(ent.name.c_str(), match, re)) {
              auto path = _dir / ent.name.c_str();
              return ss::remove_file(path.string());
          }
          return ss::now();
      });
}

ss::future<> snapshot_manager::remove_snapshot(ss::sstring target) {
    if (co_await ss::file_exists(snapshot_path(target).string())) {
        const auto path = snapshot_path(target).string();
        vlog(
          stlog.info,
          "removing snapshot file: {}",
          snapshot_path(target).string());
        try {
            co_await ss::remove_file(path);
        } catch (const std::filesystem::filesystem_error& e) {
            if (e.code() == std::errc::no_such_file_or_directory) {
                // This can happen if an STM tries to remove the same
                // snapshot twice in parallel, e.g. in issue
                // https://github.com/redpanda-data/redpanda/issues/9091
                vlog(
                  stlog.error,
                  "Raced removing snapshot {} (file not found)",
                  target);
            } else {
                throw;
            }
        }
    }

    co_return;
}

snapshot_reader::~snapshot_reader() noexcept {
    vassert(_closed, "snapshot reader has to be closed before destruction");
}

snapshot_reader::snapshot_reader(snapshot_reader&& o) noexcept
  : _file(std::move(o._file))
  , _path(std::move(o._path))
  , _input(std::move(o._input))
  , _closed(o._closed) {
    o._closed = true;
}

snapshot_reader& snapshot_reader::operator=(snapshot_reader&& o) noexcept {
    if (this != &o) {
        this->_closed = true;
        this->~snapshot_reader();
        new (this) snapshot_reader(std::move(o));
    }
    return *this;
}

ss::future<snapshot_header> snapshot_reader::read_header() {
    return read_iobuf_exactly(_input, snapshot_header::ondisk_size)
      .then([this](iobuf buf) {
          if (buf.size_bytes() != snapshot_header::ondisk_size) {
              return ss::make_exception_future<snapshot_header>(
                std::runtime_error(fmt::format(
                  "Snapshot file does not contain full header: {}", _path)));
          }

          iobuf_parser parser(std::move(buf));
          snapshot_header hdr;
          hdr.header_crc = reflection::adl<uint32_t>{}.from(parser);
          hdr.metadata_crc = reflection::adl<uint32_t>{}.from(parser);
          hdr.version = reflection::adl<int8_t>{}.from(parser);
          hdr.metadata_size = reflection::adl<int32_t>{}.from(parser);

          vassert(
            parser.bytes_consumed() == snapshot_header::ondisk_size,
            "Error parsing snapshot header. Consumed {} bytes expected {}",
            parser.bytes_consumed(),
            snapshot_header::ondisk_size);

          vassert(
            hdr.metadata_size >= 0,
            "Invalid metadata size {}",
            hdr.metadata_size);

          crc::crc32c crc;
          crc.extend(ss::cpu_to_le(hdr.metadata_crc));
          crc.extend(ss::cpu_to_le(hdr.version));
          crc.extend(ss::cpu_to_le(hdr.metadata_size));

          if (hdr.header_crc != crc.value()) {
              return ss::make_exception_future<snapshot_header>(
                std::runtime_error(fmt::format(
                  "Corrupt snapshot. Failed to verify header crc: {} != {}: {}",
                  crc.value(),
                  hdr.header_crc,
                  _path)));
          }

          if (hdr.version != snapshot_header::supported_version) {
              return ss::make_exception_future<snapshot_header>(
                std::runtime_error(fmt::format(
                  "Invalid snapshot version {} != {}: {}",
                  hdr.version,
                  snapshot_header::supported_version,
                  _path)));
          }

          return ss::make_ready_future<snapshot_header>(hdr);
      });
}

ss::future<iobuf> snapshot_reader::read_metadata() {
    return read_header().then([this](snapshot_header header) {
        return read_iobuf_exactly(_input, header.metadata_size)
          .then([this, header](iobuf buf) {
              if ((int32_t)buf.size_bytes() != header.metadata_size) {
                  return ss::make_exception_future<iobuf>(
                    std::runtime_error(fmt::format(
                      "Corrupt snapshot. Failed to read metadata: {}", _path)));
              }

              crc::crc32c crc;
              crc_extend_iobuf(crc, buf);

              if (header.metadata_crc != crc.value()) {
                  return ss::make_exception_future<iobuf>(
                    std::runtime_error(fmt::format(
                      "Corrupt snapshot. Failed to verify metadata crc: {} != "
                      "{}: {}",
                      crc.value(),
                      header.metadata_crc,
                      _path)));
              }

              return ss::make_ready_future<iobuf>(std::move(buf));
          });
    });
}

ss::future<size_t> snapshot_reader::get_snapshot_size() { return _file.size(); }

ss::future<> snapshot_reader::close() {
    return _input
      .close() // finishes read-ahead work
      .then([this] {
          _closed = true;
          return _file.close();
      });
}

snapshot_writer::~snapshot_writer() noexcept {
    vassert(_closed, "snapshot writer has to be closed before destruction");
}

file_snapshot_writer::file_snapshot_writer(
  ss::output_stream<char> output,
  std::filesystem::path path,
  std::filesystem::path target) noexcept
  : _writer(std::move(output))
  , _path(std::move(path))
  , _target(std::move(target)) {}

snapshot_writer::snapshot_writer(ss::output_stream<char> output) noexcept
  : _output(std::move(output)) {}

snapshot_writer::snapshot_writer(snapshot_writer&& o) noexcept
  : _output(std::move(o._output))
  , _closed(o._closed) {
    o._closed = true;
}

snapshot_writer& snapshot_writer::operator=(snapshot_writer&& o) noexcept {
    if (this != &o) {
        this->_closed = true;
        this->~snapshot_writer();
        new (this) snapshot_writer(std::move(o));
    }
    return *this;
}

ss::future<> snapshot_writer::write_metadata(iobuf buf) {
    snapshot_header header;
    header.version = snapshot_header::supported_version;
    header.metadata_size = buf.size_bytes();

    crc::crc32c meta_crc;
    crc_extend_iobuf(meta_crc, buf);
    header.metadata_crc = meta_crc.value();

    crc::crc32c header_crc;
    header_crc.extend(ss::cpu_to_le(header.metadata_crc));
    header_crc.extend(ss::cpu_to_le(header.version));
    header_crc.extend(ss::cpu_to_le(header.metadata_size));
    header.header_crc = header_crc.value();

    iobuf header_buf;
    reflection::serialize(
      header_buf,
      header.header_crc,
      header.metadata_crc,
      header.version,
      header.metadata_size);

    vassert(
      header_buf.size_bytes() == snapshot_header::ondisk_size,
      "Unexpected snapshot header serialization size {} != {}",
      header_buf.size_bytes(),
      snapshot_header::ondisk_size);

    buf.prepend(std::move(header_buf));
    return write_iobuf_to_output_stream(std::move(buf), _output);
}

ss::future<> snapshot_writer::close() {
    return _output.flush().then([this] {
        _closed = true;
        return _output.close();
    });
}

ss::future<std::optional<size_t>>
snapshot_manager::size(ss::sstring filename) const {
    const auto path = snapshot_path(std::move(filename)).string();
    try {
        co_return co_await ss::file_size(path);
    } catch (...) {
        vlog(
          stlog.debug,
          "Failed to stat snapshot file {}: {}",
          path,
          std::current_exception());
        co_return std::nullopt;
    }
}

} // namespace storage

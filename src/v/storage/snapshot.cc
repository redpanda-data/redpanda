#include "storage/snapshot.h"

#include "bytes/iobuf_parser.h"
#include "bytes/utils.h"
#include "hashing/crc32c.h"
#include "random/generators.h"
#include "reflection/adl.h"
#include "utils/directory_walker.h"

#include <seastar/core/iostream.hh>

#include <regex>

namespace storage {

ss::future<std::optional<snapshot_reader>> snapshot_manager::open_snapshot() {
    auto path = _dir / snapshot_filename;
    return ss::file_exists(path.string()).then([this, path](bool exists) {
        if (!exists) {
            return ss::make_ready_future<std::optional<snapshot_reader>>(
              std::nullopt);
        }
        return ss::open_file_dma(path.string(), ss::open_flags::ro)
          .then([this, path](ss::file file) {
              // ss::file::~file will automatically close the file. so no
              // worries about leaking an fd if something goes wrong here.
              ss::file_input_stream_options options;
              options.io_priority_class = _io_prio;
              auto input = ss::make_file_input_stream(file, options);
              return ss::make_ready_future<std::optional<snapshot_reader>>(
                snapshot_reader(file, std::move(input), path));
          });
    });
}

ss::future<snapshot_writer> snapshot_manager::start_snapshot() {
    // the random suffix is added because the lowres clock doesn't produce
    // unique file names when tests run fast.
    auto filename = fmt::format(
      "{}.partial.{}.{}",
      snapshot_filename,
      ss::lowres_system_clock::now().time_since_epoch().count(),
      random_generators::gen_alphanum_string(4));

    auto path = _dir / filename;

    const auto flags = ss::open_flags::wo | ss::open_flags::create
                       | ss::open_flags::exclusive;

    return ss::open_file_dma(path.string(), flags)
      .then([this, path](ss::file file) {
          ss::file_output_stream_options options;
          options.io_priority_class = _io_prio;
          return ss::make_file_output_stream(std::move(file), options);
      })
      .then([path](ss::output_stream<char> output) {
          return snapshot_writer(std::move(output), path);
      });
}

ss::future<> snapshot_manager::finish_snapshot(snapshot_writer& writer) {
    return ss::rename_file(writer.path().string(), snapshot_path().string())
      .then([this] { return ss::sync_directory(_dir.string()); });
}

ss::future<> snapshot_manager::remove_partial_snapshots() {
    std::regex re(fmt::format(
      "^{}\\.partial\\.(\\d+)\\.([a-zA-Z0-9]{{4}})$", snapshot_filename));
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

          crc32 crc;
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

              crc32 crc;
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

snapshot_writer::snapshot_writer(snapshot_writer&& o) noexcept
  : _path(std::move(o._path))
  , _output(std::move(o._output))
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

    crc32 meta_crc;
    crc_extend_iobuf(meta_crc, buf);
    header.metadata_crc = meta_crc.value();

    crc32 header_crc;
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

} // namespace storage

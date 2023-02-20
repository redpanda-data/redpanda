/*
 * Copyright 2020 Redpanda Data, Inc.
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
#include "model/fundamental.h"
#include "seastarx.h"

#include <seastar/core/file.hh>
#include <seastar/core/fstream.hh>
#include <seastar/core/seastar.hh>
#include <seastar/util/log.hh>

#include <filesystem>

namespace storage {

class snapshot_reader;
class snapshot_writer;

/**
 * Snapshot manager.
 *
 * One instance of the manager handles the life cycle of snapshots for a single
 * raft group. Snapshots are organized into a directory:
 *
 *    <dir>/
 *      - snapshot            <- current snapshot (optional)
 *      - prefix.partial.ts   <- partially written snapshot
 *      - ...
 *
 * Snapshots are initially created as `snapshot.partial.*` files. When a
 * snapshot is fully created and flushed to disk it is atomically renamed to
 * become the current snapshot. Partial snapshots may accumulate when crashes
 * occur, and the manager can be used to clean-up old snapshots.
 *
 * Snapshot file format is an envelope for the state machine snapshot data:
 *
 *    [ {version, crc}, metadata, <snapshot blob>, crc]
 *
 *    NOTE: the footer crc is not yet implemented.
 *
 * The version and crc are contained in snapshot_header, and are followed by
 * snapshot metadata that can be used to store information about the snapshot
 * and is accessible independent of the full snapshot data.  The crc covers
 * everything from the beginning of the snapshot file up to the snapshot blob.
 * This is all managed by the consensus and the state machine middleare.  The
 * blob is written by the state machine itself. For example, a state machine may
 * write a small serialized blob, or stream data to the snapshot file. Streaming
 * may be useful in cases where the data structure permits determistic iteration
 * such as a pre-order tree travseral. The snapshot file contains a footer that
 * records a crc of the snapshot blob itself.
 *
 * Usage:
 *
 *    Create a manager instance:
 *
 *       snapshot_manager mgr(prefix, "/path/to/ntp/", io_priority);
 *
 *    Snapshot manager saves snapshots atomicly by writing data to temp files
 *    and then using atomic `mv` to replace older snapshots. Prefix is a name
 *    prefix for the temporary files (suffix is unique, mix of time and salt).
 *    It's needed to avoid collision when several snapshot_managers points to
 *    the same directory. In this case when one manager does
 *    remove_partial_snapshots it may accidentally remove a wip snapshot of an
 *    another manager.
 *
 *    All snapshots will be stored in the provided directory, and snapshot
 *    readers and writers will be created using the given io priority.
 *
 *    Open the current snapshot.
 *
 *       snapshot_reader reader = mgr.open_snapshot(file_name);
 *
 *    Snapshots are created by creating a new snapshot writer:
 *
 *       snapshot_writer writer = mgr.start_snapshot(file_name);
 *
 *    And once a snapshot has been fully written pass the writer back to the
 *    snapshot manager which will detect if the snapshot should replace the
 *    current snapshot:
 *
 *       mgr.finish_snapshot(writer);
 *
 *    Partial snapshot can be deleted using the convenience function. This is
 *    generally useful when starting up the system or after a failure occurs
 *    while creating a snapshot.
 *
 *       mgr.remove_partial_snapshots();
 */
class snapshot_manager {
public:
    snapshot_manager(
      ss::sstring partial_prefix,
      std::filesystem::path dir,
      ss::io_priority_class io_prio) noexcept
      : _partial_prefix(partial_prefix)
      , _dir(std::move(dir))
      , _io_prio(io_prio) {}

    ss::future<std::optional<snapshot_reader>> open_snapshot(ss::sstring);
    ss::future<uint64_t> get_snapshot_size(ss::sstring);

    ss::future<snapshot_writer> start_snapshot(ss::sstring);
    ss::future<> finish_snapshot(snapshot_writer&);

    std::filesystem::path snapshot_path(ss::sstring filename) const {
        return _dir / filename.c_str();
    }

    ss::future<> remove_partial_snapshots();

    ss::future<> remove_snapshot(ss::sstring);

private:
    ss::sstring _partial_prefix;
    std::filesystem::path _dir;
    ss::io_priority_class _io_prio;
};

/**
 * Snapshot header.
 *
 * header_crc: covers header
 * metadata_crc: covers metadata
 * version: metadata version
 * size: size of metadata
 */
struct snapshot_header {
    static constexpr const int8_t supported_version = 1;

    uint32_t header_crc;
    uint32_t metadata_crc;
    int8_t version;
    int32_t metadata_size;

    static constexpr const size_t ondisk_size = sizeof(header_crc)
                                                + sizeof(metadata_crc)
                                                + sizeof(version)
                                                + sizeof(metadata_size);
};

/**
 * Snapshot reader.
 *
 * Usage:
 *
 *    Open the current snapshot with the manager:
 *
 *       snapshot_reader reader = mgr.open_snapshot();
 *
 *    The consensus layer will then use snapshot metadata:
 *
 *       snapshot_header header = reader.read_header();
 *
 *    And finally pass the input stream from reader.input() to the state machine
 *    which will hydrate its state from the stream.
 *
 *    The reader should be closed when it is no longer needed.
 */
class snapshot_reader {
public:
    explicit snapshot_reader(
      ss::file file,
      ss::input_stream<char> input,
      std::filesystem::path path) noexcept
      : _file(std::move(file))
      , _path(std::move(path))
      , _input(std::move(input)) {}

    ~snapshot_reader() noexcept;
    snapshot_reader(const snapshot_reader&) = delete;
    snapshot_reader(snapshot_reader&&) noexcept;
    snapshot_reader& operator=(const snapshot_reader&) = delete;
    snapshot_reader& operator=(snapshot_reader&&) noexcept;

    ss::future<iobuf> read_metadata();
    ss::future<size_t> get_snapshot_size();
    ss::input_stream<char>& input() { return _input; }
    ss::future<> close();

private:
    ss::future<snapshot_header> read_header();

    ss::file _file;
    std::filesystem::path _path;
    ss::input_stream<char> _input;
    bool _closed = false;
};

/**
 * Use the snapshot manager to open a new snapshot:
 *
 *    snapshot_writer writer = manager.start_snapshot().get0();
 *
 * Initialize the snapshot with metadata:
 *
 *    writer.write_metadata(...).get();
 *
 * Pass the output stream to the state machine
 *
 *    state_machine.take_snapshot(wrtier.output());
 *
 * Once the state machine has finished writing its snapshot data finalize the
 * snapshot by closing the writer and moving the writer back into the snapshot
 * manager:
 *
 *    writer.close().get();
 *    manager.finish_snapshot(std::move(writer));
 */
class snapshot_writer {
public:
    snapshot_writer(
      ss::output_stream<char>,
      std::filesystem::path,
      std::filesystem::path) noexcept;

    ~snapshot_writer() noexcept;
    snapshot_writer(const snapshot_writer&) = delete;
    snapshot_writer(snapshot_writer&&) noexcept;
    snapshot_writer& operator=(const snapshot_writer&) = delete;
    snapshot_writer& operator=(snapshot_writer&&) noexcept;

    ss::future<> write_metadata(iobuf);
    ss::output_stream<char>& output() { return _output; }
    ss::future<> close();

    const std::filesystem::path& path() const { return _path; }

    const std::filesystem::path& target() const { return _target; }

private:
    std::filesystem::path _path;
    ss::output_stream<char> _output;
    std::filesystem::path _target;
    bool _closed = false;
};

class simple_snapshot_manager {
public:
    static constexpr const char* default_snapshot_filename = "snapshot";

    simple_snapshot_manager(
      std::filesystem::path dir,
      ss::sstring filename,
      ss::io_priority_class io_prio) noexcept
      : _filename(filename)
      , _snapshot(filename, std::move(dir), io_prio) {}

    ss::future<std::optional<snapshot_reader>> open_snapshot() {
        return _snapshot.open_snapshot(_filename);
    }

    ss::future<uint64_t> get_snapshot_size() {
        return _snapshot.get_snapshot_size(_filename);
    }

    ss::future<snapshot_writer> start_snapshot() {
        return _snapshot.start_snapshot(_filename);
    }
    ss::future<> finish_snapshot(snapshot_writer& writer) {
        return _snapshot.finish_snapshot(writer);
    }

    std::filesystem::path snapshot_path() const {
        return _snapshot.snapshot_path(_filename);
    }

    ss::future<> remove_partial_snapshots() {
        return _snapshot.remove_partial_snapshots();
    }

    ss::future<> remove_snapshot() {
        return _snapshot.remove_snapshot(_filename);
    }

    const ss::sstring& name() { return _filename; }

private:
    ss::sstring _filename;
    snapshot_manager _snapshot;
};

} // namespace storage

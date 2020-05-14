#pragma once
#include "bytes/iobuf.h"
#include "model/fundamental.h"
#include "seastarx.h"

#include <seastar/core/file.hh>
#include <seastar/core/fstream.hh>
#include <seastar/core/seastar.hh>
#include <seastar/util/log.hh>

#include <filesystem>

namespace raft {

class snapshot_reader;
class snapshot_writer;

/**
 * Snapshot manager.
 *
 * One instance of the manager handles the life cycle of snapshots for a single
 * raft group. Snapshots are organized into a directory:
 *
 *    <dir>/
 *      - snapshot              <- current snapshot (optional)
 *      - snapshot.partial.ts   <- partially written snapshot
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
 * snapshot_metadata that contains information about the snapshot like the
 * index, term, and latest configuration. The crc covers everything from the
 * beginning of the snapshot file up to the snapshot blob. This is all managed
 * by the consensus and the state machine middleare.  The blob is written by the
 * state machine itself. For example, a state machine may write a small
 * serialized blob, or stream data to the snapshot file. Streaming may be useful
 * in cases where the data structure permits determistic iteration such as a
 * pre-order tree travseral. The snapshot file contains a footer that records a
 * crc of the snapshot blob itself.
 *
 * Usage:
 *
 *    Create a manager instance:
 *
 *       snapshot_manager mgr("/path/to/ntp/", io_priority);
 *
 *    All snapshots will be stored in the provided directory, and snapshot
 *    readers and writers will be created using the given io priority.
 *
 *    Open the current snapshot.
 *
 *       snapshot_reader reader = mgr.open_snapshot();
 *
 *    Snapshots are created by creating a new snapshot writer:
 *
 *       snapshot_writer writer = mgr.start_snapshot();
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
    static constexpr const char* snapshot_filename = "snapshot";

public:
    snapshot_manager(
      std::filesystem::path dir, ss::io_priority_class io_prio) noexcept
      : _dir(std::move(dir))
      , _io_prio(io_prio) {}

    ss::future<std::optional<snapshot_reader>> open_snapshot();

    ss::future<snapshot_writer> start_snapshot();
    ss::future<> finish_snapshot(snapshot_writer&);

    std::filesystem::path snapshot_path() const {
        return _dir / snapshot_filename;
    }

    ss::future<> remove_partial_snapshots();

private:
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
    int32_t size;

    static constexpr const size_t ondisk_size = sizeof(header_crc)
                                                + sizeof(metadata_crc)
                                                + sizeof(version)
                                                + sizeof(size);
};

/**
 * Snapshot metadata.
 *
 * TODO:
 *   - Store the latest configuration for each snapshot. This will be added when
 *   configuration tracking is added into consensus.
 *
 *   - Store the cluster time associated with snapshot. This is a piece of
 *   metadata that is useful when implementing sessions for linearizability.
 */
struct snapshot_metadata {
    model::offset last_included_index;
    model::term_id last_included_term;

    static constexpr const size_t ondisk_size = sizeof(last_included_index)
                                                + sizeof(last_included_term);

    friend std::ostream& operator<<(std::ostream&, const snapshot_header&);
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

    ss::future<snapshot_metadata> read_metadata();
    ss::input_stream<char>& input() { return _input; }
    ss::future<> close();

private:
    static snapshot_metadata parse_metadata(iobuf);
    ss::future<snapshot_header> read_header();

    ss::file _file;
    std::filesystem::path _path;
    ss::input_stream<char> _input;
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
      ss::output_stream<char> output, std::filesystem::path path) noexcept
      : _path(std::move(path))
      , _output(std::move(output)) {}

    ss::future<> write_metadata(const snapshot_metadata&);
    ss::output_stream<char>& output() { return _output; }
    ss::future<> close();

    const std::filesystem::path& path() const { return _path; }

private:
    std::filesystem::path _path;
    ss::output_stream<char> _output;
};

} // namespace raft

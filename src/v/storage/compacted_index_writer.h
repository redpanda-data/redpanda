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
#include "bytes/bytes.h"
#include "model/fundamental.h"
#include "model/record_batch_types.h"
#include "storage/compacted_index.h"
#include "storage/types.h"

#include <seastar/core/file.hh>
#include <seastar/core/future.hh>

#include <bits/stdint-intn.h>

#include <cstdint>

namespace storage {

class storage_resources;

/** format on file is:
    INT16 PAYLOAD
    INT16 PAYLOAD
    INT16 PAYLOAD
    ...
    FOOTER

PAYLOAD:

    ENTRY_TYPE // 1 byte
    VINT       // batch-base-offset
    VINT       // record-offset-delta
    []BYTE     // actual key (in truncate events we use 'truncation')


footer - in little endian
*/
class compacted_index_writer {
public:
    class impl {
    public:
        explicit impl(ss::sstring filename) noexcept
          : _name(std::move(filename)) {}
        virtual ~impl() noexcept = default;
        impl(impl&&) noexcept = default;
        impl& operator=(impl&&) noexcept = default;
        impl(const impl&) = delete;
        impl& operator=(const impl&) = delete;

        virtual ss::future<> index(
          const compaction_key&, // convert from bytes which is the key-type in
                                 // map
          model::offset base_offset,
          int32_t offset_delta)
          = 0;

        virtual ss::future<> index(
          model::record_batch_type,
          const iobuf& key, // default format in record batch
          model::offset base_offset,
          int32_t offset_delta)
          = 0;

        virtual ss::future<> index(
          model::record_batch_type,
          bytes&& key, // default format in record batch
          model::offset base_offset,
          int32_t offset_delta)
          = 0;

        virtual ss::future<> append(compacted_index::entry) = 0;

        virtual ss::future<> truncate(model::offset) = 0;

        virtual void set_flag(compacted_index::footer_flags) = 0;

        virtual ss::future<> close() = 0;

        virtual void print(std::ostream&) const = 0;

        const ss::sstring& filename() const { return _name; }

    private:
        ss::sstring _name;
    };

    explicit compacted_index_writer(std::unique_ptr<impl> i)
      : _impl(std::move(i)) {}

    // accepts a compaction_key which is already prefixed with batch_type
    ss::future<> index(const compaction_key& b, model::offset, int32_t);

    ss::future<>
    index(model::record_batch_type, const iobuf& key, model::offset, int32_t);
    ss::future<>
    index(model::record_batch_type, bytes&&, model::offset, int32_t);

    ss::future<> append(compacted_index::entry);

    ss::future<> truncate(model::offset);
    ss::future<> close();
    void set_flag(compacted_index::footer_flags);
    void print(std::ostream&) const;
    const ss::sstring& filename() const;
    std::unique_ptr<impl> release() &&;

private:
    friend std::ostream&
    operator<<(std::ostream& o, const compacted_index_writer& c) {
        c.print(o);
        return o;
    }
    std::unique_ptr<impl> _impl;
};
inline void compacted_index_writer::set_flag(compacted_index::footer_flags f) {
    _impl->set_flag(f);
}
inline void compacted_index_writer::print(std::ostream& o) const {
    _impl->print(o);
}
inline const ss::sstring& compacted_index_writer::filename() const {
    return _impl->filename();
}
inline std::unique_ptr<compacted_index_writer::impl>
compacted_index_writer::release() && {
    return std::move(_impl);
}
inline ss::future<> compacted_index_writer::index(
  model::record_batch_type batch_type,
  const iobuf& b,
  model::offset base_offset,
  int32_t delta) {
    return _impl->index(batch_type, b, base_offset, delta);
}
inline ss::future<> compacted_index_writer::index(
  const compaction_key& b, model::offset base_offset, int32_t delta) {
    return _impl->index(b, base_offset, delta);
}
inline ss::future<> compacted_index_writer::index(
  model::record_batch_type batch_type,
  bytes&& b,
  model::offset base_offset,
  int32_t delta) {
    return _impl->index(batch_type, std::move(b), base_offset, delta);
}
inline ss::future<> compacted_index_writer::truncate(model::offset o) {
    return _impl->truncate(o);
}
inline ss::future<> compacted_index_writer::append(compacted_index::entry e) {
    return _impl->append(std::move(e));
}
inline ss::future<> compacted_index_writer::close() { return _impl->close(); }

compacted_index_writer make_file_backed_compacted_index(
  ss::sstring filename,
  ss::io_priority_class p,
  debug_sanitize_files debug,
  bool truncate,
  storage_resources& resources);

} // namespace storage

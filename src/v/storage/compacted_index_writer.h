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
#include "compaction/key.h"
#include "model/fundamental.h"
#include "model/record_batch_types.h"
#include "storage/compacted_index.h"
#include "storage/file_sanitizer_types.h"
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
    explicit compacted_index_writer(ss::sstring name)
      : _name(std::move(name)) {}
    virtual ~compacted_index_writer() = default;

    // accepts a compaction_key which is already prefixed with batch_type
    virtual ss::future<>
    index(const compaction::compaction_key& b, model::offset, int32_t) = 0;

    virtual ss::future<> index(
      model::record_batch_type,
      bool is_control_batch,
      const iobuf& key,
      model::offset,
      int32_t) = 0;

    virtual ss::future<> index(
      model::record_batch_type,
      bool is_control_batch,
      bytes&&,
      model::offset,
      int32_t) = 0;

    virtual ss::future<> append(compacted_index::entry) = 0;

    virtual ss::future<> close() = 0;
    virtual void set_flag(compacted_index::footer_flags) = 0;
    virtual void print(std::ostream&) const = 0;
    const ss::sstring& filename() const { return _name; }
    virtual size_t size_bytes() const = 0;

private:
    friend std::ostream&
    operator<<(std::ostream& o, const compacted_index_writer& c) {
        c.print(o);
        return o;
    }

    ss::sstring _name;
};

std::unique_ptr<compacted_index_writer> make_file_backed_compacted_index(
  ss::sstring filename,
  bool truncate,
  storage_resources& resources,
  std::optional<ntp_sanitizer_config> sanitizer_config);

} // namespace storage

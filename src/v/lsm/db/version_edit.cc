// Copyright (c) 2014 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found at https://github.com/google/leveldb/blob/main/LICENSE. See
// https://github.com/google/leveldb/blob/main/AUTHORS for names of
// contributors.
//
// Modifications copyright 2025 Redpanda Data, Inc.

#include "lsm/db/version_edit.h"

#include "utils/to_string.h" // IWYU pragma: keep

namespace lsm::db {

fmt::iterator file_meta_data::format_to(fmt::iterator it) const {
    return fmt::format_to(
      it,
      "{{handle:{},file_size:{},smallest:{},largest:{},allowed_seeks:{},oldest:"
      "{},newest:{}}}",
      handle,
      file_size,
      smallest,
      largest,
      allowed_seeks,
      oldest_seqno,
      newest_seqno);
}

fmt::iterator version_edit::mutation::format_to(fmt::iterator it) const {
    return fmt::format_to(
      it,
      "{{removed_files:{},added_files:{},compact_pointer:{}}}",
      fmt::join(removed_files, ","),
      fmt::join(added_files, ","),
      compact_pointer);
}

fmt::iterator version_edit::format_to(fmt::iterator it) const {
    return fmt::format_to(
      it, "{{mutations_by_level:{}}}", fmt::join(_mutations_by_level, ","));
}

} // namespace lsm::db

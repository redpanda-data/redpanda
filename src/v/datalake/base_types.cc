/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#include "datalake/base_types.h"

#include <fmt/core.h>
namespace datalake {
std::ostream& operator<<(std::ostream& o, const local_file_metadata& f_meta) {
    fmt::print(
      o,
      "{{relative_path: {}, size_bytes: {}, row_count: {}, hour: {}}}",
      f_meta.path,
      f_meta.size_bytes,
      f_meta.row_count,
      f_meta.hour);
    return o;
}
} // namespace datalake

/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#include "datalake/coordinator/data_file.h"

namespace datalake::coordinator {

std::ostream& operator<<(std::ostream& o, const data_file& f) {
    o << fmt::format(
      "{{remote_path: {}, row_count: {}, file_size_bytes: {}, hour: {}}}",
      f.remote_path,
      f.row_count,
      f.file_size_bytes,
      f.hour);
    return o;
}

} // namespace datalake::coordinator

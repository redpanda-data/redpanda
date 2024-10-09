/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#include "datalake/coordinator/translated_offset_range.h"

namespace datalake::coordinator {

std::ostream& operator<<(std::ostream& o, const translated_offset_range& r) {
    o << fmt::format(
      "{{start_offset: {}, last_offset: {}, files: {}}}",
      r.start_offset,
      r.last_offset,
      r.files);
    return o;
}

} // namespace datalake::coordinator

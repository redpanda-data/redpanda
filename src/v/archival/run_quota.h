/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#pragma once

#include "utils/named_type.h"

namespace archival {

/// Housekeeping job quota. One share is one segment reupload
/// or one deletion operation. The value can be negative if the
/// job overcommitted.
using run_quota_t = named_type<int32_t, struct _job_quota_tag>;

} // namespace archival

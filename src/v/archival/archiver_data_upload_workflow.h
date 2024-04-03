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

#include "archival/archiver_workflow_api.h"
#include "archival/fwd.h"
#include "model/fundamental.h"
#include "model/ktp.h"

#include <seastar/core/shared_ptr.hh>

namespace ss = seastar;

namespace archival {

ss::shared_ptr<archiver_workflow_api> make_data_upload_workflow(
  model::ktp ntp,
  model::term_id id,
  ss::shared_ptr<archiver_operations_api> api [[maybe_unused]],
  ss::shared_ptr<archiver_scheduler_api> quote [[maybe_unused]]);

} // namespace archival

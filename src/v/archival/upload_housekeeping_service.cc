/*
 * Copyright 2022 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "archival/upload_housekeeping_service.h"

namespace archival {

upload_housekeeping_service::upload_housekeeping_service(
  ss::sharded<cloud_storage::remote>& api)
  : _remote(api.local()) {}

void upload_housekeeping_service::register_archiver(ntp_archiver& archiver) {
    _archivers.push_back(archiver);
}

} // namespace archival
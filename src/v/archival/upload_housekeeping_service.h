/*
 * Copyright 2022 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "archival/ntp_archiver_service.h"
#include "cloud_storage/remote.h"
#include "utils/intrusive_list_helpers.h"

#include <seastar/core/sharded.hh>

namespace archival {

class upload_housekeeping_service {
public:
    explicit upload_housekeeping_service(
      ss::sharded<cloud_storage::remote>& api);

    /// Register ntp_archiver
    void register_archiver(ntp_archiver& archiver);

private:
    intrusive_list<ntp_archiver, &ntp_archiver::_list_hook> _archivers;

    cloud_storage::remote& _remote;
};

} // namespace archival
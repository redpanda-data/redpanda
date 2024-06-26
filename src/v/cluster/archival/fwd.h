/*
 * Copyright 2021 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#pragma once

namespace archival {

class upload_controller;
class ntp_archiver;
struct configuration;
class upload_housekeeping_service;
class purger;
class scrubber;
class archiver_manager;

class archiver_operations_api;
class archiver_scheduler_api;

} // namespace archival

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

namespace datalake {
struct data_writer_result;
class record_translator;
class schema_manager;
class type_resolver;
class table_creator;
namespace coordinator {
class coordinator_manager;
class frontend;
struct translated_offset_range;
class catalog_factory;
}; // namespace coordinator
namespace translation {
class translation_stm;
class partition_translator;
}; // namespace translation
class datalake_manager;
class cloud_data_io;
namespace tests {
class datalake_cluster_test_fixture;
}

} // namespace datalake

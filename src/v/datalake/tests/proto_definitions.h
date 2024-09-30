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

#ifdef BAZEL_TEST
#include "src/v/datalake/tests/testdata/complex.pb.h"
#include "src/v/datalake/tests/testdata/iceberg_ready_test_messages_edition2023.pb.h"
#include "src/v/datalake/tests/testdata/not_supported.pb.h"
#include "src/v/datalake/tests/testdata/person.pb.h"
#include "src/v/datalake/tests/testdata/proto2.pb.h"
#else
#include "testdata/complex.pb.h"
#include "testdata/iceberg_ready_test_messages_edition2023.pb.h"
#include "testdata/not_supported.pb.h"
#include "testdata/person.pb.h"
#include "testdata/proto2.pb.h"
#endif

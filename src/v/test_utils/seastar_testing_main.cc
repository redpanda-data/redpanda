// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#define SEASTAR_TESTING_MAIN
#include <seastar/testing/seastar_test.hh>

// this file will be compiled with a main() that invokes registered tests. used
// to create v::seastar_testing_main library.

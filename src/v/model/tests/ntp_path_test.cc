// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "model/fundamental.h"

#include <boost/test/unit_test.hpp>

#include <utility>

BOOST_AUTO_TEST_CASE(ntp_path_test) {
    auto ntp = model::ntp(
      model::ns("sf"), model::topic("richmond"), model::partition_id(94118));

    BOOST_CHECK_EQUAL("sf/richmond/94118", ntp.path());
}

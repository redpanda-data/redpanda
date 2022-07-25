// Copyright 2022 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#pragma once

#include "compat/compat_check.h"
#include "storage/index_state.h"

struct index_state_compat
  : public compat::compat_check
  , public storage::index_state {
    void random_init() override;
    void json_init(rapidjson::Value& w) override;
    iobuf to_json() const override;
    iobuf to_binary() override;
    bool check_compatibility(iobuf&& binary) const override;
    std::string name() const override { return "index_state"; }
};

// Copyright 2022 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "compat/compat_check.h"

struct remote_segment_index_compat : public compat::compat_check {
    static constexpr size_t segment_num_batches = 1023;
    static constexpr model::offset segment_base_rp_offset{1234};
    static constexpr model::offset segment_base_kaf_offset{1210};

    void random_init() override;
    void json_init(rapidjson::Value& w) override;
    iobuf to_json() const override;
    iobuf to_binary() override;
    bool check_compatibility(iobuf&& binary) const override;
    std::string name() const override { return "remote_segment_index"; }

    int64_t rp{segment_base_rp_offset()};
    int64_t kaf{segment_base_kaf_offset()};
    size_t fpos{};
    model::offset last;
    model::offset klast{};
    int64_t flast{};

    std::vector<model::offset> rp_offsets;
    std::vector<model::offset> kaf_offsets;
    std::vector<int64_t> file_offsets;
};

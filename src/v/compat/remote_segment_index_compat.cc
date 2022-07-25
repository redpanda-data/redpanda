// Copyright 2022 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "compat/remote_segment_index_compat.h"

#include "bytes/iobuf_istreambuf.h"
#include "cloud_storage/remote_segment_index.h"
#include "compat/compat_check.h"
#include "compat/json_helpers.h"
#include "compat/logger.h"
#include "compat/verify.h"
#include "index_state_compat.h"
#include "json/writer.h"
#include "random/generators.h"
#include "serde/serde.h"
#include "storage/index_state.h"
#include "utils/file_io.h"

#include <seastar/core/coroutine.hh>

#include <rapidjson/istreamwrapper.h>

#include <filesystem>
#include <ostream>
#include <string_view>

void remote_segment_index_compat::random_init() {
    bool is_config = false;
    for (size_t i = 0; i < segment_num_batches; i++) {
        if (!is_config) {
            rp_offsets.push_back(model::offset(rp));
            kaf_offsets.push_back(model::offset(kaf));
            file_offsets.push_back(fpos);
        }
        // The test queries every element using the key that matches the element
        // exactly and then it queries the element using the key which is
        // smaller than the element. In order to do this we need a way to
        // guarantee that the distance between to elements in the sequence is at
        // least 2, so we can decrement the key safely.
        auto batch_size = random_generators::get_int(2, 100);
        is_config = random_generators::get_int(20) == 0;
        rp += batch_size;
        kaf += is_config ? batch_size - 1 : batch_size;
        fpos += random_generators::get_int(1000, 2000);
    }
}

void remote_segment_index_compat::json_init(rapidjson::Value& w) {
    verify(w.IsObject(), "expected JSON object", w.GetType());
    json::read_member(w, "rp_offsets", rp_offsets);
    json::read_member(w, "kaf_offsets", kaf_offsets);
    json::read_member(w, "file_offsets", file_offsets);
    json::read_member(w, "rp", rp);
    json::read_member(w, "kaf", kaf);
    json::read_member(w, "fpos", fpos);
    json::read_member(w, "last", last);
    json::read_member(w, "klast", klast);
    json::read_member(w, "flast", flast);
}

iobuf remote_segment_index_compat::to_json() const {
    auto sb = json::StringBuffer{};
    auto w = json::Writer<json::StringBuffer>{sb};

    w.StartObject();
    json::write_member(w, "rp_offsets", rp_offsets);
    json::write_member(w, "kaf_offsets", kaf_offsets);
    json::write_member(w, "file_offsets", file_offsets);
    json::write_member(w, "rp", rp);
    json::write_member(w, "kaf", kaf);
    json::write_member(w, "fpos", fpos);
    json::write_member(w, "last", last);
    json::write_member(w, "klast", klast);
    json::write_member(w, "flast", flast);
    w.EndObject();

    iobuf s;
    s.append(sb.GetString(), sb.GetLength());
    return s;
}

iobuf remote_segment_index_compat::to_binary() {
    using namespace cloud_storage;

    offset_index tmp_index(
      segment_base_rp_offset, segment_base_kaf_offset, 0U, 1000);
    for (size_t i = 0; i < rp_offsets.size(); i++) {
        tmp_index.add(rp_offsets.at(i), kaf_offsets.at(i), file_offsets.at(i));
        last = rp_offsets.at(i);
        klast = kaf_offsets.at(i);
        flast = file_offsets.at(i);
    }

    return offset_index(
             segment_base_rp_offset, segment_base_kaf_offset, 0U, 1000)
      .to_iobuf();
}

bool remote_segment_index_compat::check_compatibility(iobuf&& binary) const {
    using namespace cloud_storage;

    offset_index index(
      segment_base_rp_offset, segment_base_kaf_offset, 0U, 1000);
    index.from_iobuf(std::move(binary));

    // Query element before the first one
    auto opt_first = index.find_rp_offset(
      segment_base_rp_offset - model::offset(1));
    verify(!opt_first.has_value(), "opt_first exists");

    auto kopt_first = index.find_kaf_offset(
      segment_base_kaf_offset - model::offset(1));
    verify(!kopt_first.has_value(), "kopt_first exists");

    for (unsigned ix = 0; ix < rp_offsets.size(); ix++) {
        auto opt = index.find_rp_offset(rp_offsets[ix] + model::offset(1));
        auto [rp, kaf, fpos] = *opt;
        verify_equal(rp, rp_offsets[ix]);
        verify_equal(kaf, kaf_offsets[ix]);
        verify_equal(fpos, file_offsets[ix]);

        auto kopt = index.find_kaf_offset(kaf_offsets[ix] + model::offset(1));
        verify_equal(kopt->rp_offset, rp_offsets[ix]);
        verify_equal(kopt->kaf_offset, kaf_offsets[ix]);
        verify_equal(kopt->file_pos, file_offsets[ix]);
    }

    // Query after the last element
    auto opt_last = index.find_rp_offset(last + model::offset(1));
    auto [rp_last, kaf_last, file_last] = *opt_last;
    verify_equal(rp_last, last);
    verify_equal(kaf_last, klast);
    verify_equal(file_last, flast);

    auto kopt_last = index.find_kaf_offset(klast + model::offset(1));
    verify_equal(kopt_last->rp_offset, last);
    verify_equal(kopt_last->kaf_offset, klast);
    verify_equal(kopt_last->file_pos, flast);

    return true;
}

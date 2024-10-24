// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0
#include "iceberg/manifest_file_packer.h"

namespace iceberg {

namespace {
struct bin {
    explicit bin(size_t target)
      : target_weight(target)
      , total_weight(0) {}
    const size_t target_weight;
    size_t total_weight;
    chunked_vector<manifest_file> items;

    bool can_add(const manifest_file& f) const {
        if (total_weight == 0) {
            return true;
        }
        return total_weight + f.manifest_length <= target_weight;
    }

    void add(manifest_file f) {
        total_weight += f.manifest_length;
        items.emplace_back(std::move(f));
    }

    // Resets this bin, returning the items.
    chunked_vector<manifest_file> reset() {
        total_weight = 0;
        return std::exchange(items, {});
    }
};
} // namespace

chunked_vector<chunked_vector<manifest_file>> manifest_packer::pack(
  size_t target_size_bytes, chunked_vector<manifest_file> files) {
    // Since we want to begin our packing from the back of the list, reverse
    // the list.
    std::reverse(files.begin(), files.end());
    bin bin{target_size_bytes};
    chunked_vector<chunked_vector<manifest_file>> binned_files;
    for (auto& f : files) {
        if (!bin.can_add(f)) {
            binned_files.emplace_back(bin.reset());
        }
        bin.add(std::move(f));
    }
    if (bin.items.size()) {
        binned_files.emplace_back(bin.reset());
    }
    // Since our input has been reversed, return the resulting bins to their
    // original order by reversing files in each  bin and then reversing the
    // order of the bins.
    for (auto& bin : binned_files) {
        std::reverse(bin.begin(), bin.end());
    }
    std::reverse(binned_files.begin(), binned_files.end());
    return binned_files;
}

} // namespace iceberg

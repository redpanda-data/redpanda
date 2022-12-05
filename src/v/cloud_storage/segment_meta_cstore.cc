/*
 * Copyright 2022 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_storage/segment_meta_cstore.h"

namespace cloud_storage {

class segment_meta_cstore::impl {
public:
private:
};

segment_meta_cstore::const_iterator segment_meta_cstore::begin() {
    throw "not implemented";
}

segment_meta_cstore::const_iterator segment_meta_cstore::end() {
    throw "not implemented";
}

std::optional<segment_meta> segment_meta_cstore::last_segment() {
    throw "not implemented";
}

segment_meta_cstore::const_iterator
segment_meta_cstore::find(model::offset) const {
    throw "not implemented";
}

bool segment_meta_cstore::contains(model::offset) const {
    throw "not implemented";
}

bool segment_meta_cstore::empty() const { throw "not implemented"; }

segment_meta_cstore::const_iterator
segment_meta_cstore::upper_bound(model::offset) const {
    throw "not implemented";
}

segment_meta_cstore::const_iterator
segment_meta_cstore::lower_bound(model::offset) const {
    throw "not implemented";
}

std::pair<segment_meta_cstore::const_iterator, bool>
segment_meta_cstore::insert(std::pair<model::offset, segment_meta>) {
    throw "not implemented";
}

} // namespace cloud_storage
/*
 * Copyright 2022 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "base/seastarx.h"
#include "cloud_storage/segment_meta_cstore.h"
#include "model/fundamental.h"
#include "random/generators.h"
#include "utils/delta_for.h"

#include <seastar/testing/perf_tests.hh>
#include <seastar/util/defer.hh>

#include <absl/container/btree_map.h>

#include <ranges>
#include <vector>

using namespace cloud_storage;

std::vector<segment_meta> generate_metadata(size_t sz) {
    namespace rg = random_generators;
    std::vector<segment_meta> manifest;
    segment_meta curr{
      .is_compacted = false,
      .size_bytes = 812,
      .base_offset = model::offset(0),
      .committed_offset = model::offset(0),
      .base_timestamp = model::timestamp(1646430092103),
      .max_timestamp = model::timestamp(1646430092103),
      .delta_offset = model::offset_delta(0),
      .archiver_term = model::term_id(2),
      .segment_term = model::term_id(0),
      .delta_offset_end = model::offset_delta(0),
      .sname_format = segment_name_format::v2,
    };
    bool short_segment_run = false;
    for (size_t i = 0; i < sz; i++) {
        auto s = curr;
        manifest.push_back(s);
        if (short_segment_run) {
            curr.base_offset = model::next_offset(curr.committed_offset);
            curr.committed_offset = curr.committed_offset
                                    + model::offset(rg::get_int(1, 10));
            curr.size_bytes = rg::get_int(1, 200);
            curr.base_timestamp = curr.max_timestamp;
            curr.max_timestamp = model::timestamp(
              curr.max_timestamp.value() + rg::get_int(0, 1000));
            curr.delta_offset = curr.delta_offset_end;
            curr.delta_offset_end = curr.delta_offset_end
                                    + model::offset_delta(rg::get_int(5));
            if (rg::get_int(50) == 0) {
                curr.segment_term = curr.segment_term
                                    + model::term_id(rg::get_int(1, 20));
                curr.archiver_term = curr.archiver_term
                                     + model::term_id(rg::get_int(1, 20));
            }
        } else {
            curr.base_offset = model::next_offset(curr.committed_offset);
            curr.committed_offset = curr.committed_offset
                                    + model::offset(rg::get_int(1, 1000));
            curr.size_bytes = rg::get_int(1, 200000);
            curr.base_timestamp = curr.max_timestamp;
            curr.max_timestamp = model::timestamp(
              curr.max_timestamp.value() + rg::get_int(0, 100000));
            curr.delta_offset = curr.delta_offset_end;
            curr.delta_offset_end = curr.delta_offset_end
                                    + model::offset_delta(rg::get_int(15));
            if (rg::get_int(50) == 0) {
                curr.segment_term = curr.segment_term
                                    + model::term_id(rg::get_int(1, 20));
                curr.archiver_term = curr.archiver_term
                                     + model::term_id(rg::get_int(1, 20));
            }
        }
        if (rg::get_int(200) == 0) {
            short_segment_run = !short_segment_run;
        }
    }
    return manifest;
}

class baseline_column_store {
public:
    using const_iterator
      = absl::btree_map<model::offset, segment_meta>::const_iterator;

    /// Return iterator
    const_iterator begin() const { return _data.begin(); }

    const_iterator end() const { return _data.end(); }

    /// Return last segment's metadata (or nullopt if empty)
    std::optional<segment_meta> last_segment() const {
        if (_data.empty()) {
            return std::nullopt;
        }
        auto it = _data.end();
        it = std::prev(it);
        return it->second;
    }

    /// Find element and return its iterator
    const_iterator find(model::offset o) const { return _data.find(o); }

    /// Check if the offset is present
    bool contains(model::offset o) const { return _data.contains(o); }

    /// Return true if data structure is empty
    bool empty() const { return _data.empty(); }

    /// Return size of the collection
    size_t size() const { return _data.size(); }

    /// Upper/lower bound search operations
    const_iterator upper_bound(model::offset o) const {
        return _data.upper_bound(o);
    }
    const_iterator lower_bound(model::offset o) const {
        return _data.lower_bound(o);
    }

    void insert(const segment_meta& meta) { _data[meta.base_offset] = meta; }

    auto to_iobuf() {
        iobuf buf;
        serialize(buf, _data.size());
        for (auto& [k, v] : _data) {
            serialize(buf, k);
            serialize(buf, v);
        }
        _data.clear();
        return buf;
    }

    void from_iobuf(iobuf in) {
        auto cons = details::io_iterator_consumer{in.begin(), in.end()};
        auto map_size = deserialize<size_t>(cons);
        for (auto i = 0u; i < map_size; ++i) {
            auto k = deserialize<model::offset>(cons);
            auto v = deserialize<segment_meta>(cons);
            _data.emplace(k, v);
        }
    }

private:
    static void serialize(iobuf& buf, const auto& v) {
        auto tmp = std::bit_cast<std::array<uint8_t, sizeof(v)>>(v);
        buf.append(tmp.data(), tmp.size());
    }

    template<typename T>
    static T deserialize(details::io_iterator_consumer& buf) {
        std::array<uint8_t, sizeof(T)> tmp;
        buf.consume_to(tmp.size(), tmp.begin());
        return std::bit_cast<T>(tmp);
    }

    absl::btree_map<model::offset, segment_meta> _data;
};

template<class StoreT>
void cs_append_test(StoreT& store, size_t sz) {
    auto manifest = generate_metadata(sz);

    size_t i = 0;
    for (const auto& s : manifest) {
        store.insert(s);
        i++;
        if (i == sz) {
            // Do not add last element
            break;
        }
    }

    perf_tests::start_measuring_time();
    store.insert(manifest.back());
    perf_tests::stop_measuring_time();
}

template<class StoreT>
void cs_scan_test(StoreT& store, size_t sz) {
    auto manifest = generate_metadata(sz);

    for (const auto& s : manifest) {
        store.insert(s);
    }

    perf_tests::start_measuring_time();
    for (const auto& i : store) {
        perf_tests::do_not_optimize(i);
    }
    perf_tests::stop_measuring_time();
}

auto last_n(size_t n) {
    return std::views::reverse | std::views::take(n) | std::views::reverse;
}

template<class StoreT>
void cs_find_test(StoreT& store, size_t sz) {
    auto manifest = generate_metadata(sz);

    for (const auto& s : manifest) {
        store.insert(s);
    }

    for (auto& e : manifest | last_n(20)) {
        perf_tests::start_measuring_time();
        auto i = store.find(e.base_offset);
        perf_tests::do_not_optimize(i);
        perf_tests::stop_measuring_time();
    }
}

template<class StoreT>
void cs_lower_bound_test(StoreT& store, size_t sz) {
    auto manifest = generate_metadata(sz);

    for (const auto& s : manifest) {
        store.insert(s);
    }

    for (auto& e : manifest | last_n(20)) {
        perf_tests::start_measuring_time();
        auto i = store.lower_bound(e.base_offset + model::offset(1));
        perf_tests::do_not_optimize(i);
        perf_tests::stop_measuring_time();
    }
}

template<class StoreT>
void cs_upper_bound_test(StoreT& store, size_t sz) {
    auto manifest = generate_metadata(sz);

    for (const auto& s : manifest) {
        store.insert(s);
    }

    for (auto& e : manifest | last_n(20)) {
        perf_tests::start_measuring_time();
        auto i = store.upper_bound(e.base_offset + model::offset(1));
        perf_tests::do_not_optimize(i);
        perf_tests::stop_measuring_time();
    }
}

template<class StoreT>
void cs_last_segment_test(StoreT& store, size_t sz) {
    auto manifest = generate_metadata(sz);

    for (const auto& s : manifest) {
        store.insert(s);
    }

    perf_tests::start_measuring_time();
    auto s = store.last_segment();
    perf_tests::do_not_optimize(s);
    perf_tests::stop_measuring_time();
}

void cs_serialize_test(auto& store, size_t sz) {
    for (auto manifest = generate_metadata(sz); auto& s : manifest) {
        store.insert(s);
    }

    perf_tests::start_measuring_time();
    auto buf = store.to_iobuf();
    perf_tests::do_not_optimize(buf);
    perf_tests::stop_measuring_time();
}

void cs_deserialize_test(auto& store, size_t sz) {
    for (auto manifest = generate_metadata(sz); auto& s : manifest) {
        store.insert(s);
    }

    auto buf = store.to_iobuf();
    perf_tests::start_measuring_time();
    store.from_iobuf(std::move(buf));
    perf_tests::stop_measuring_time();
}
PERF_TEST(cstore_bench, column_store_append_baseline) {
    baseline_column_store store;
    cs_append_test(store, 10000);
}

PERF_TEST(cstore_bench, column_store_append_result) {
    segment_meta_cstore store;
    cs_append_test(store, 10000);
}

PERF_TEST(cstore_bench, column_store_scan_baseline) {
    baseline_column_store store;
    cs_scan_test(store, 10000);
}

PERF_TEST(cstore_bench, column_store_scan_result) {
    segment_meta_cstore store;
    cs_scan_test(store, 10000);
}

PERF_TEST(cstore_bench, column_store_find_baseline) {
    baseline_column_store store;
    cs_find_test(store, 10000);
}

PERF_TEST(cstore_bench, column_store_find_result) {
    segment_meta_cstore store;
    cs_find_test(store, 10000);
}

PERF_TEST(cstore_bench, column_store_find_no_hints) {
    segment_meta_cstore store;
    config::shard_local_cfg().storage_ignore_cstore_hints.set_value(true);
    auto _ = ss::defer([] {
        config::shard_local_cfg().storage_ignore_cstore_hints.set_value(false);
    });
    cs_find_test(store, 10000);
}

PERF_TEST(cstore_bench, column_store_lower_bound_baseline) {
    baseline_column_store store;
    cs_lower_bound_test(store, 10000);
}

PERF_TEST(cstore_bench, column_store_lower_bound_result) {
    segment_meta_cstore store;
    cs_lower_bound_test(store, 10000);
}

PERF_TEST(cstore_bench, column_store_upper_bound_baseline) {
    baseline_column_store store;
    cs_upper_bound_test(store, 10000);
}

PERF_TEST(cstore_bench, column_store_upper_bound_result) {
    segment_meta_cstore store;
    cs_upper_bound_test(store, 10000);
}

PERF_TEST(cstore_bench, column_store_last_segment_baseline) {
    baseline_column_store store;
    cs_last_segment_test(store, 10000);
}

PERF_TEST(cstore_bench, column_store_last_segment_result) {
    segment_meta_cstore store;
    cs_last_segment_test(store, 10000);
}

PERF_TEST(cstore_bench, column_store_serialize_baseline) {
    baseline_column_store store;
    cs_serialize_test(store, 10000);
}

PERF_TEST(cstore_bench, column_store_serialize_result) {
    segment_meta_cstore store;
    cs_serialize_test(store, 10000);
}

PERF_TEST(cstore_bench, column_store_deserialize_baseline) {
    baseline_column_store store;
    cs_deserialize_test(store, 10000);
}

PERF_TEST(cstore_bench, column_store_deserialize_result) {
    segment_meta_cstore store;
    cs_deserialize_test(store, 10000);
}

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
#include "model/fundamental.h"
#include "random/generators.h"
#include "seastarx.h"
#include "utils/delta_for.h"

#include <seastar/testing/perf_tests.hh>

#include <absl/container/btree_map.h>

#include <vector>

using namespace cloud_storage;

using delta_xor_alg = details::delta_xor;
using delta_xor_frame = segment_meta_column_frame<int64_t, delta_xor_alg{}>;
using delta_delta_alg = details::delta_delta<int64_t>;
using delta_delta_frame = segment_meta_column_frame<int64_t, delta_delta_alg{}>;
using delta_xor_column = segment_meta_column<int64_t, delta_xor_alg>;
using delta_delta_column = segment_meta_column<int64_t, delta_delta_alg>;

static const delta_xor_frame xor_frame_4K = []() {
    delta_xor_frame frame{};
    int64_t value = random_generators::get_int(1000);
    for (int64_t i = 0; i < 4096; i++) {
        value += random_generators::get_int(1, 100);
        frame.append(value);
    }
    return frame;
}();

static const delta_xor_column xor_column_4K = []() {
    delta_xor_column column{};
    int64_t value = random_generators::get_int(1000);
    for (int64_t i = 0; i < 4096; i++) {
        value += random_generators::get_int(1, 100);
        column.append(value);
    }
    return column;
}();

static const delta_xor_column xor_column_4M = []() {
    delta_xor_column column{};
    int64_t value = random_generators::get_int(1000);
    for (int64_t i = 0; i < 4096000; i++) {
        value += random_generators::get_int(1, 100);
        column.append(value);
    }
    return column;
}();

static const delta_delta_frame delta_frame_4K = []() {
    delta_delta_frame frame{};
    int64_t value = random_generators::get_int(1000);
    for (int64_t i = 0; i < 4096; i++) {
        value += random_generators::get_int(1, 100);
        frame.append(value);
    }
    return frame;
}();

static const delta_delta_column delta_column_4K = []() {
    delta_delta_column column{};
    int64_t value = random_generators::get_int(1000);
    for (int64_t i = 0; i < 4096; i++) {
        value += random_generators::get_int(1, 100);
        column.append(value);
    }
    return column;
}();

static const delta_delta_column delta_column_4M = []() {
    delta_delta_column column{};
    int64_t value = random_generators::get_int(1000);
    for (int64_t i = 0; i < 4096000; i++) {
        value += random_generators::get_int(1, 100);
        column.append(value);
    }
    return column;
}();

template<class StoreT>
void append_test(StoreT& store, int test_scale) {
    std::vector<int64_t> head;
    int64_t tail;
    int64_t value = 0;
    for (int64_t i = 0; i < test_scale - 1; i++) {
        value += random_generators::get_int(1, 100);
        head.push_back(value);
    }
    tail = value + random_generators::get_int(1, 100);
    for (auto x : head) {
        store.append(x);
    }
    perf_tests::start_measuring_time();
    store.append(tail);
    perf_tests::stop_measuring_time();
}

template<class StoreT>
void append_tx_test(StoreT& store, int test_scale) {
    std::vector<int64_t> head;
    int64_t tail;
    int64_t value = 0;
    for (int64_t i = 0; i < test_scale - 1; i++) {
        value += random_generators::get_int(1, 100);
        head.push_back(value);
    }
    tail = value + random_generators::get_int(1, 100);
    for (auto x : head) {
        store.append(x);
    }
    perf_tests::start_measuring_time();
    auto tx = store.append_tx(tail);
    if (tx) {
        std::move(*tx).commit();
    } else {
        assert(false);
    }
    perf_tests::stop_measuring_time();
}

template<class StoreT>
void find_test(StoreT& store) {
    perf_tests::start_measuring_time();
    auto it = store.find(*store.last_value());
    perf_tests::do_not_optimize(it);
    perf_tests::stop_measuring_time();
}

template<class StoreT>
void at_test(StoreT& store) {
    perf_tests::start_measuring_time();
    auto it = store.at_index(store.size() - 1);
    perf_tests::do_not_optimize(it);
    perf_tests::stop_measuring_time();
}

PERF_TEST(cstore_bench, xor_frame_append) {
    delta_xor_frame frame{};
    append_test(frame, 4096);
}

PERF_TEST(cstore_bench, xor_frame_append_tx) {
    delta_xor_frame frame{};
    append_tx_test(frame, 4096);
}

PERF_TEST(cstore_bench, xor_column_append) {
    delta_xor_column column{};
    append_test(column, 4096);
}

PERF_TEST(cstore_bench, xor_column_append_tx) {
    delta_xor_column column{};
    append_tx_test(column, 4096);
}

PERF_TEST(cstore_bench, xor_column_append_tx2) {
    // trigger code path that commits by splicing the list
    delta_xor_column column{};
    append_tx_test(column, 4097);
}

PERF_TEST(cstore_bench, xor_frame_find_4K) { find_test(xor_frame_4K); }

PERF_TEST(cstore_bench, xor_column_find_4K) { find_test(xor_column_4K); }

PERF_TEST(cstore_bench, xor_frame_at_4K) { at_test(xor_frame_4K); }

PERF_TEST(cstore_bench, xor_column_at_4K) { at_test(xor_column_4K); }

PERF_TEST(cstore_bench, xor_column_at_4M) { at_test(xor_column_4M); }

PERF_TEST(cstore_bench, delta_frame_append) {
    delta_delta_frame frame{};
    append_test(frame, 4096);
}

PERF_TEST(cstore_bench, delta_frame_append_tx) {
    delta_delta_frame frame{};
    append_tx_test(frame, 4096);
}

PERF_TEST(cstore_bench, delta_column_append) {
    delta_delta_column column{};
    append_test(column, 4096);
}

PERF_TEST(cstore_bench, delta_column_append_tx) {
    delta_delta_column column{};
    append_tx_test(column, 4096);
}

PERF_TEST(cstore_bench, delta_column_append_tx2) {
    // trigger code path that commits by splicing the list
    delta_delta_column column{};
    append_tx_test(column, 4097);
}

PERF_TEST(cstore_bench, delta_frame_find_4K) { find_test(delta_frame_4K); }

PERF_TEST(cstore_bench, delta_column_find_4K) { find_test(delta_column_4K); }

PERF_TEST(cstore_bench, delta_column_find_4M) { at_test(delta_column_4M); }

PERF_TEST(cstore_bench, delta_frame_at_4K) { at_test(delta_frame_4K); }

PERF_TEST(cstore_bench, delta_column_at_4K) { at_test(delta_column_4K); }

PERF_TEST(cstore_bench, delta_column_at_4M) { at_test(delta_column_4M); }

PERF_TEST(cstore_bench, xor_frame_at_with_index_4K) {
    std::map<int32_t, delta_xor_frame::hint_t> index;
    delta_xor_frame frame{};
    int64_t value = random_generators::get_int(1000);
    for (int64_t i = 0; i < 4096; i++) {
        value += random_generators::get_int(1, 100);
        frame.append(value);
        if (i % 16 == 0) {
            auto row = frame.get_current_stream_pos();
            if (row) {
                index.insert(std::make_pair(i, row.value()));
            }
        }
    }
    auto it = index.at(4000);
    perf_tests::start_measuring_time();
    frame.at_index(4000, it);
    perf_tests::stop_measuring_time();
}

PERF_TEST(cstore_bench, xor_column_at_with_index_4K) {
    std::map<int32_t, delta_xor_column::hint_t> index;
    delta_xor_column column{};
    int64_t value = random_generators::get_int(1000);
    for (int64_t i = 0; i < 4096; i++) {
        value += random_generators::get_int(1, 100);
        column.append(value);
        if (i % 16 == 0) {
            auto row = column.get_current_stream_pos();
            if (row) {
                index.insert(std::make_pair(i, row.value()));
            }
        }
    }
    auto it = index.at(4000);
    perf_tests::start_measuring_time();
    column.at_index(4000, it);
    perf_tests::stop_measuring_time();
}

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
    static void serialize(iobuf& buf, auto const& v) {
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

template<class StoreT>
void cs_find_test(StoreT& store, size_t sz) {
    auto manifest = generate_metadata(sz);

    for (const auto& s : manifest) {
        store.insert(s);
    }

    perf_tests::start_measuring_time();
    auto i = store.find(manifest.back().base_offset);
    perf_tests::do_not_optimize(i);
    perf_tests::stop_measuring_time();
}

template<class StoreT>
void cs_lower_bound_test(StoreT& store, size_t sz) {
    auto manifest = generate_metadata(sz);

    for (const auto& s : manifest) {
        store.insert(s);
    }

    perf_tests::start_measuring_time();
    auto i = store.lower_bound(manifest.back().base_offset);
    perf_tests::do_not_optimize(i);
    perf_tests::stop_measuring_time();
}

template<class StoreT>
void cs_upper_bound_test(StoreT& store, size_t sz) {
    auto manifest = generate_metadata(sz);

    for (const auto& s : manifest) {
        store.insert(s);
    }

    perf_tests::start_measuring_time();
    auto i = store.upper_bound(manifest.back().base_offset);
    perf_tests::do_not_optimize(i);
    perf_tests::stop_measuring_time();
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

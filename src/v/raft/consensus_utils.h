/*
 * Copyright 2020 Vectorized, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "likely.h"
#include "model/record.h"
#include "raft/configuration_bootstrap_state.h"
#include "raft/types.h"
#include "storage/log.h"
#include "storage/snapshot.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/circular_buffer.hh>
#include <seastar/core/sstring.hh>

namespace raft::details {
/// copy all record batch readers into N containers using the
/// record_batch::share()
ss::future<std::vector<model::record_batch_reader>>
share_n(model::record_batch_reader&&, std::size_t);

/// copy all readers into N containers using the record_batch::share()
/// it also wraps all the iobufs using the ss::foreign_ptr<> method
/// which wraps the deallocator w/ a ss::submit_to() call
ss::future<std::vector<model::record_batch_reader>>
foreign_share_n(model::record_batch_reader&&, std::size_t);

/// serialize group configuration as config-type batch
ss::circular_buffer<model::record_batch>
serialize_configuration_as_batches(group_configuration cfg);

/// serialize group configuration to the record_batch_reader
model::record_batch_reader serialize_configuration(group_configuration cfg);

/// returns a fully parsed config state from a given storage log, starting at
/// given offset
ss::future<raft::configuration_bootstrap_state>
read_bootstrap_state(storage::log, model::offset, ss::abort_source&);

ss::circular_buffer<model::record_batch> make_ghost_batches_in_gaps(
  model::offset, ss::circular_buffer<model::record_batch>&&);

/// writes snapshot with given data to disk
ss::future<>
persist_snapshot(storage::snapshot_manager&, snapshot_metadata, iobuf&&);

/// looks up for the broker with request id in a vector of brokers
template<typename Iterator>
Iterator find_machine(Iterator begin, Iterator end, model::node_id id) {
    return std::find_if(
      begin, end, [id](decltype(*begin) b) { return b.id() == id; });
}

inline constexpr model::offset next_offset(model::offset o) {
    if (o < model::offset{0}) {
        return model::offset{0};
    }
    return o + model::offset{1};
}

inline constexpr model::offset prev_offset(model::offset o) {
    if (o <= model::offset{0}) {
        return model::offset{};
    }
    return o - model::offset{1};
}

class term_assigning_reader : public model::record_batch_reader::impl {
public:
    using data_t = model::record_batch_reader::data_t;
    using foreign_data_t = model::record_batch_reader::foreign_data_t;
    using storage_t = model::record_batch_reader::storage_t;

    term_assigning_reader(model::record_batch_reader r, model::term_id term)
      : _source(std::move(r).release())
      , _term(term) {}

    bool is_end_of_stream() const final { return _source->is_end_of_stream(); }

    ss::future<storage_t>
    do_load_slice(model::timeout_clock::time_point tout) final {
        return _source->do_load_slice(tout).then([t = _term](storage_t ret) {
            if (likely(std::holds_alternative<data_t>(ret))) {
                auto& d = std::get<data_t>(ret);
                for (auto& r : d) {
                    r.set_term(t);
                }
            } else {
                // NOTE: Ok to modify header here. since we are not
                // touching the underlying IOBUF's
                auto& d = std::get<foreign_data_t>(ret);
                for (auto& r : *d.buffer) {
                    r.set_term(t);
                }
            }
            return ret;
        });
    }

    void print(std::ostream& os) final {
        fmt::print(os, "{term assigning reader}");
    }

private:
    std::unique_ptr<model::record_batch_reader::impl> _source;
    model::term_id _term;
};

// clang-format off
template<typename Func>
CONCEPT(
    requires requires(Func f, model::record_batch b){
        { f(std::move(b)) } 
            -> std::same_as<ss::futurize_t<std::invoke_result_t<Func, model::record_batch&&>>>;
    }
)
// clang-format on
// Consumer applying an async action to each element in the reader
class do_for_each_batch_consumer {
public:
    explicit do_for_each_batch_consumer(Func&& f)
      : _f(std::forward<Func>(f)) {}

    ss::future<ss::stop_iteration> operator()(model::record_batch b) {
        return _f(std::move(b)).then([] { return ss::stop_iteration::no; });
    }
    void end_of_stream() {}

    Func _f;
};

/**
 * Extracts all configurations from underlying reader. Configuration are stored
 * in a vector passed as a reference to reader. The reader can will
 * automatically assing offsets to following batches using provided base offset
 * as a staring point
 */
model::record_batch_reader make_config_extracting_reader(
  model::offset,
  std::vector<offset_configuration>&,
  model::record_batch_reader&&);

/**
 * Function that allow consuming batches with given consumer while lazily
 * extracting raft::group_configuration from the reader.
 *
 * returns tuple<consumer_result, std::vector<offset_configuration>>
 */
template<typename ReferenceConsumer>
auto for_each_ref_extract_configuration(
  model::offset base_offset,
  model::record_batch_reader&& rdr,
  ReferenceConsumer c,
  model::timeout_clock::time_point tm) {
    using conf_t = std::vector<offset_configuration>;

    return ss::do_with(
      conf_t{},
      [tm, c = std::move(c), base_offset, rdr = std::move(rdr)](
        conf_t& configurations) mutable {
          return make_config_extracting_reader(
                   base_offset, configurations, std::move(rdr))
            .for_each_ref(std::move(c), tm)
            .then([&configurations](auto res) {
                return std::make_tuple(
                  std::move(res), std::move(configurations));
            });
      });
}

} // namespace raft::details

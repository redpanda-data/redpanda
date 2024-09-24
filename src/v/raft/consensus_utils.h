/*
 * Copyright 2020 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "base/likely.h"
#include "model/record.h"
#include "raft/configuration_bootstrap_state.h"
#include "raft/types.h"
#include "storage/log.h"
#include "storage/snapshot.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/circular_buffer.hh>
#include <seastar/core/smp.hh>
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

iobuf serialize_configuration(group_configuration cfg);
void write_configuration(group_configuration cfg, iobuf& out);

/// returns a fully parsed config state from a given storage log, starting at
/// given offset
ss::future<raft::configuration_bootstrap_state> read_bootstrap_state(
  ss::shared_ptr<storage::log>, model::offset, ss::abort_source&);

ss::circular_buffer<model::record_batch> make_ghost_batches_in_gaps(
  model::offset, ss::circular_buffer<model::record_batch>&&);
fragmented_vector<model::record_batch> make_ghost_batches_in_gaps(
  model::offset, fragmented_vector<model::record_batch>&&);

/// writes snapshot with given data to disk
ss::future<>
persist_snapshot(storage::simple_snapshot_manager&, snapshot_metadata, iobuf&&);

group_configuration deserialize_configuration(iobuf_parser&);

group_configuration deserialize_nested_configuration(iobuf_parser&);

/// looks up for the broker with request id in a vector of brokers
template<typename Iterator>
Iterator find_machine(Iterator begin, Iterator end, model::node_id id) {
    return std::find_if(
      begin, end, [id](decltype(*begin) b) { return b.id() == id; });
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

    void print(std::ostream& os) final { os << "{term assigning reader}"; }

private:
    std::unique_ptr<model::record_batch_reader::impl> _source;
    model::term_id _term;
};

// clang-format off
template<typename Func>
    requires requires(Func f, model::record_batch b){
        { f(std::move(b)) } 
            -> std::same_as<ss::futurize_t<std::invoke_result_t<Func, model::record_batch&&>>>;
    }
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
    struct extracting_consumer {
        ss::future<ss::stop_iteration> operator()(model::record_batch& batch) {
            if (
              batch.header().type
              == model::record_batch_type::raft_configuration) {
                iobuf_parser parser(
                  batch.copy_records().begin()->release_value());
                configurations.emplace_back(
                  next_offset, deserialize_configuration(parser));
            }

            // we have to calculate offsets manually because the batch may not
            // yet have the base offset assigned.
            next_offset += model::offset(batch.header().last_offset_delta)
                           + model::offset(1);

            return wrapped(batch);
        }

        auto end_of_stream() {
            return ss::futurize_invoke(
                     [this] { return wrapped.end_of_stream(); })
              .then([confs = std::move(configurations)](auto ret) mutable {
                  return std::make_tuple(std::move(ret), std::move(confs));
              });
        }

        ReferenceConsumer wrapped;
        model::offset next_offset;
        std::vector<offset_configuration> configurations;
    };

    return std::move(rdr).for_each_ref(
      extracting_consumer{
        .wrapped = std::move(c),
        .next_offset = model::next_offset(base_offset)},
      tm);
}

bytes serialize_group_key(raft::group_id, metadata_key);
/**
 * copies raft persistent state from KV store on source shard to the one on
 * target shard.
 */
ss::future<> copy_persistent_state(
  raft::group_id,
  storage::kvstore& source_kvs,
  ss::shard_id target_shard,
  ss::sharded<storage::api>&);

/**
 * removes raft persistent state from a kvstore.
 */
ss::future<> remove_persistent_state(raft::group_id, storage::kvstore&);

/// Creates persitent state for pre-existing partition (stored in S3 bucket).
///
/// The function is supposed to be called before creating a raft group with the
/// same group_id. The created group will have 'start_offset' equal to
/// 'min_rp_offset' and 'highest_known_offset' equal to 'max_rp_offset'.
/// The function will create the offset translator state in kv-store. The offset
/// translator state will have the right starting point and delta. It will also
/// create raft snapshot and raft state in kv-store.
ss::future<> bootstrap_pre_existing_partition(
  storage::api& api,
  const storage::ntp_config& ntp_cfg,
  raft::group_id group,
  model::offset min_rp_offset,
  model::offset max_rp_offset,
  model::term_id last_included_term,
  std::vector<model::broker> initial_nodes,
  ss::lw_shared_ptr<storage::offset_translator_state> ot_state);
} // namespace raft::details

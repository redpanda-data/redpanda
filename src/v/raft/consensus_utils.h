#pragma once

#include "likely.h"
#include "model/record.h"
#include "raft/configuration_bootstrap_state.h"
#include "raft/consensus.h"

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

/// parses the configuration out of the record_batch_reader
// if there are some configuration batch types
ss::future<std::optional<raft::group_configuration>>
extract_configuration(model::record_batch_reader&&);

/// serialize group configuration as config-type batch
ss::circular_buffer<model::record_batch>
serialize_configuration_as_batches(group_configuration cfg);

/// serialize group configuration to the record_batch_reader
model::record_batch_reader serialize_configuration(group_configuration cfg);

/// returns a fully parsed config state from a given storage log
ss::future<raft::configuration_bootstrap_state>
read_bootstrap_state(storage::log, ss::abort_source&);

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
            -> ss::futurize_t<std::result_of_t<Func(model::record_batch&&)>>;
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

} // namespace raft::details

#pragma once

#include "detail/functor_traits.h"
#include "model/record.h"
#include "rpc/deserialize.h"
#include "storage/parser.h"

#include <seastar/core/do_with.hh>
#include <seastar/core/file.hh>
#include <seastar/core/fstream.hh>
#include <seastar/core/future-util.hh>
#include <seastar/core/future.hh>
#include <seastar/core/iostream.hh>
#include <seastar/core/seastar.hh>

#include <type_traits>

namespace storage {

namespace { // internal types

template<typename OutType, typename InType, typename OpType>
class reducing_consumer final : public storage::batch_consumer {
public:
    using out_type = OutType;
    using in_type = InType;
    using op_type = OpType;

public:
    reducing_consumer(out_type init, op_type op)
      : _work(make_ready_future<out_type>(std::move(init)))
      , _op(std::move(op)) {}

    skip consume_batch_start(model::record_batch_header header, size_t) {
        return skip::no;
    }

    skip consume_record_key(
      size_t sz,
      model::record_attributes attribs,
      int32_t timestamp_delta,
      int32_t offset_delta,
      iobuf&& key) {
        return skip::no;
    }

    void consume_compressed_records(iobuf&&) {}
    void consume_record_value(iobuf&& value_and_headers) {
        _work = _work.then(
          [this, buf = std::move(value_and_headers)](out_type acc) mutable {
              return rpc::deserialize<in_type>(std::move(buf))
                .then([this, acc = std::move(acc)](in_type deserialized) {
                    return make_ready_future<out_type>(
                      _op(std::move(acc), std::move(deserialized)));
                });
          });
    }

    stop_iteration consume_batch_end() { return stop_iteration::no; }

    future<out_type> release() && { return std::move(_work); }

private:
    op_type _op;
    future<out_type> _work;
};

} // namespace

/**
 * This function performs left folds on values stored in a log file and
 * returns the reduced value.
 *
 * It is used most often with an action log to recreate a the final state
 * of something. For example it can be a list of modifications to the index
 * that gets folded into the final compacted version, or a list of segment
 * operations that gets folded into the final shape of a log.
 *
 * Current uses of this are:
 *  1. segment list compaction in a log
 *  2. segment index compaction
 *  3. crc checksum verification
 */
template<typename Ret, typename Op>
seastar::future<Ret> foldl_log(seastar::file logfile, Ret&& init, Op&& op) {
    static_assert(
      detail::functor_traits<Op>::arity == 2,
      "Op is expected to be a binary function.");

    static_assert(
      std::is_same_v<typename detail::functor_traits<Op>::return_type, Ret>,
      "Op is expected to have the same return type as the init value");

    // as this is a fold operation, the data in the log are of the same type
    // as the second argument to Op, so we will always be calling Op as:
    // init = op(std::move(init, next_from_log) until the log is exhausted.
    using reduced_type = typename detail::functor_traits<Op>::return_type;
    using element_type = typename detail::functor_traits<Op>::template arg_t<1>;
    using consumer_type = reducing_consumer<reduced_type, element_type, Op>;

    // this logic keeps asking the parser to parse and read
    // more bytes from the underlying input stream until it
    // is exhausted and it reports zero read bytes.
    auto read_loop = [](continuous_batch_parser& parser) {
        return repeat([&parser] {
            return parser.consume().then([](size_t consumed_bytes) {
                return make_ready_future<stop_iteration>(
                  consumed_bytes == 0 ? stop_iteration::yes
                                      : stop_iteration::no);
            });
        });
    };

    // creates a parser that feeds records from the input stream to the
    // reducing consumer, then invokes the read loop. When read loop completes
    // then it returns the accumulated reduced value from the consumer, which is
    // the final result of the log reduction.
    auto consumer_loop = [read_loop = std::move(read_loop)](
                           consumer_type& consumer,
                           seastar::input_stream<char>& istream) {
        continuous_batch_parser p(consumer, istream);
        return seastar::do_with(std::move(p), read_loop).then([&consumer] {
            // once the read loop it complete, move out the accumulated value
            // and pack it in a future for the caller to consume.
            return std::move(consumer).release();
        });
    };

    // The accumulating consumer will read each record and interpret it
    // as "element_type", then accumulate reduce results in its internal
    // state.
    auto consumer = consumer_type(
      std::forward<Ret>(init), std::forward<Op>(op));
    auto istream = seastar::make_file_input_stream(std::move(logfile), 0);

    return seastar::do_with(
      std::move(consumer), std::move(istream), consumer_loop);
}

template<typename Ret, typename Op>
seastar::future<Ret> foldl_log(seastar::sstring logfile, Ret&& init, Op&& op) {
    return seastar::open_file_dma(std::move(logfile), seastar::open_flags::ro)
      .then([init = std::forward<Ret>(init),
             op = std::forward<Op>(op)](seastar::file f) mutable {
          return foldl_log(f, std::forward<Ret>(init), std::forward<Op>(op));
      });
}

} // namespace storage
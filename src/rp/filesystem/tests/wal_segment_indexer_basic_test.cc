#include <seastar/core/app-template.hh>
#include <seastar/core/distributed.hh>
#include <seastar/core/lowres_clock.hh>
#include <seastar/core/sstring.hh>
#include <smf/compression.h>
#include <smf/log.h>

#include "ioutil/priority_manager.h"
#include "wal_generated.h"
#include "wal_opts.h"
#include "wal_reader_node.h"
#include "wal_requests.h"
#include "wal_segment_indexer.h"

// test only
#include "wal_topic_test_input.h"

using namespace rp;  // NOLINT
// creating a namespace with `-` tests the regexes
static const seastar::sstring kNS = "empty-ns007";
// creating a topic with `_` tests the regexes
static const seastar::sstring kTopic = "dummy_topic";

void
add_opts(boost::program_options::options_description_easy_init o) {
  namespace po = boost::program_options;
  o("write-ahead-log-dir", po::value<std::string>(), "log directory");
}

void
print_record(int64_t offset, const rp::wal_binary_record *i) {
  const char *record_begin = reinterpret_cast<const char *>(i->data()->Data());
  rp::wal_header hdr;
  std::memcpy(&hdr, record_begin, sizeof(hdr));
  const char *key_begin = record_begin + sizeof(hdr);
  LOG_TRACE("Updating key: {} with offset: {}",
            seastar::sstring(key_begin, hdr.key_size()), offset);
}
void
print_reply(std::vector<uint8_t> &vec) {
  static auto lz4 = smf::codec::make_unique(smf::codec_type::lz4,
                                            smf::compression_level::fastest);

  char *record_begin = reinterpret_cast<char *>(vec.data());
  rp::wal_header hdr;
  std::memcpy(&hdr, record_begin, sizeof(hdr));
  char *compressed_value_begin = record_begin + sizeof(hdr) + hdr.key_size();

  seastar::temporary_buffer<char> idx =
    lz4->uncompress(compressed_value_begin, hdr.value_size());

  auto reply = flatbuffers::GetRoot<rp::wal_segment_index_fragment>(idx.get());

  for (auto k : *reply->keys()) {
    auto str = seastar::sstring((char *)k->key()->Data(), k->key()->size());
    LOG_INFO("Key: `{}` offset: {}, hash: {}", str, k->offset(), k->hash());
  }
  LOG_INFO("Largest offset stored in index: {}, lens_bytes: {}, total keys: {}",
           reply->largest_offset(), reply->lens_bytes(), reply->keys()->size());
}

smf::fbs_typed_buf<wal_put_request>
gen_puts(int32_t batch, int32_t partition, int32_t rand_bytes) {
  smf::random rand;
  auto ptr = std::make_unique<wal_put_requestT>();
  ptr->ns = xxhash_64(kNS.c_str(), kNS.size());
  ptr->topic = xxhash_64(kTopic.c_str(), kTopic.size());

  auto idx = std::make_unique<wal_put_partition_recordsT>();
  idx->partition = partition;
  idx->records.reserve(batch);
  for (auto j = 0; j < batch; ++j) {
    seastar::sstring key = rand.next_alphanum(rand_bytes);
    seastar::sstring value = rand.next_str(rand_bytes);
    idx->records.push_back(rp::wal_segment_record::coalesce(
      key.data(), key.size(), value.data(), value.size()));
  }
  ptr->partition_puts.push_back(std::move(idx));

  return smf::fbs_typed_buf<wal_put_request>(
    smf::native_table_as_buffer<wal_put_request>(*ptr));
}

int
main(int args, char **argv, char **env) {
  std::cout.setf(std::ios::unitbuf);
  seastar::app_template app;

  try {
    add_opts(app.add_options());

    return app.run(args, argv, [&] {
      smf::app_run_log_level(seastar::log_level::trace);
      auto &config = app.configuration();
      auto dir = config["write-ahead-log-dir"].as<std::string>();

      const seastar::sstring index_filename = dir + "/0.wal.index";

      LOG_THROW_IF(dir.empty(), "Empty write-ahead-log-dir");
      LOG_INFO("log_segment test dir: {}", dir);

      return seastar::do_with(
               rp::wal_opts(dir.c_str(), std::chrono::milliseconds(5)),
               [&](auto &wopts) {
                 auto indexer =
                   seastar::make_lw_shared<rp::wal_segment_indexer>(
                     index_filename, wopts,
                     rp::priority_manager::get().default_priority());

                 return indexer->open()
                   .then([indexer] {
                     return seastar::do_with(
                       gen_puts(1000, 0, 1), [indexer](auto &put) {
                         return seastar::do_with(
                           wal_core_mapping::core_assignment(put.get()),
                           std::size_t(0), int64_t(0),
                           [indexer](auto &reqs, std::size_t &idx, auto &acc) {
                             return seastar::do_with(
                               std::move(reqs[idx++]),
                               [indexer, &acc](auto &r) {
                                 return seastar::do_for_each(
                                   r.begin(), r.end(),
                                   [indexer, &acc](auto i) mutable {
                                     auto offset = acc;
                                     print_record(offset, i);
                                     acc += i->data()->size();
                                     return indexer->index(offset, i);
                                   });
                               });
                           });
                       });
                   })
                   .then([indexer] {
                     return indexer->close().finally([indexer] {});
                   });
               })
        .then([index_filename] { return seastar::file_size(index_filename); })
        .then([index_filename](int64_t sz) {
          LOG_INFO("Sucess Writing Index file: {} index size: {}",
                   index_filename, sz);
          auto reader = seastar::make_lw_shared<rp::wal_reader_node>(
            0, sz, seastar::lowres_system_clock::now(), index_filename);
          return reader->open()
            .then([reader, writer_size = sz] {
              rp::wal_get_requestT r;
              r.ns = xxhash_64(kNS.c_str(), kNS.size());
              r.topic = xxhash_64(kTopic.c_str(), kTopic.size());
              r.partition = 0;
              r.offset = 0;  // begin
              r.max_bytes = writer_size;
              auto readq = smf::fbs_typed_buf<rp::wal_get_request>(
                smf::native_table_as_buffer<rp::wal_get_request>(r));

              auto ret = seastar::make_lw_shared<rp::wal_read_reply>(
                r.ns, r.topic, 0, 0, true);
              return seastar::do_with(std::move(readq),
                                      [reader, ret](auto &tbuf) {
                                        auto r =
                                          rp::wal_core_mapping::core_assignment(
                                            tbuf.get());
                                        return reader->get(ret.get(), r);
                                      })
                .then([ret, writer_size] {
                  int64_t sz = std::accumulate(
                    ret->reply().gets.begin(), ret->reply().gets.end(),
                    int64_t(0),
                    [](int64_t acc, auto &n) { return acc + n->data.size(); });

                  LOG_THROW_IF(sz != writer_size,
                               "========>>>>>>> Size {} did not match "
                               "size on disk {}, and writer wrote: {}",
                               sz, ret->on_disk_size(), writer_size);

                  LOG_INFO("Indexer wrote: {} and Reader read: {}", writer_size,
                           sz);
                  // wal binary record of reply
                  auto &bindx = ret->reply().gets[0];
                  print_reply(bindx->data);
                  return seastar::make_ready_future<>();
                });
            })
            .then([reader] { reader->close(); })
            .finally([reader] {});
        })
        .then([] { return seastar::make_ready_future<int>(0); });
    });
  } catch (const std::exception &e) {
    std::cerr << "Fatal exception: " << e.what() << std::endl;
  }
}

#include "filesystem/tests/gen_create_topic_buf.h"
#include "filesystem/tests/wal_topic_test_input.h"
#include "filesystem/wal_core_mapping.h"
#include "filesystem/wal_opts.h"
#include "filesystem/wal_pretty_print_utils.h"
#include "filesystem/wal_requests.h"
#include "filesystem/write_ahead_log.h"

#include <seastar/core/app-template.hh>
#include <seastar/core/distributed.hh>
#include <seastar/core/sleep.hh>
#include <seastar/core/sstring.hh>

#include <smf/fbs_typed_buf.h>
#include <smf/log.h>

#include <flatbuffers/minireflect.h>

#include <chrono>
#include <set>

// creating a namespace with `-` tests the regexes
static const sstring kNS = "empty-ns007";
// creating a topic with `_` tests the regexes
static const sstring kTopic = "dummy_topic";

void add_opts(boost::program_options::options_description_easy_init o) {
    namespace po = boost::program_options;
    o("write-ahead-log-dir",
      po::value<std::string>()->default_value("."),
      "log directory");
}

future<std::vector<std::unique_ptr<wal_get_requestT>>>
do_writes(uint32_t core, write_ahead_log& w) {
    return do_with(
      wal_topic_test_input(
        kNS,
        kTopic,
        // partitions
        smp::count * 2,
        // type (can be compaction)
        wal_topic_type::wal_topic_type_regular,
        // map of properties for topic
        {{"prop-for-topic", "maybe-store-access-keys"}}),
      std::vector<std::unique_ptr<wal_get_requestT>>{},
      [&w, core](auto& input, auto& offsets) {
          return do_with(
                   input.create_requests(),
                   std::size_t(0),
                   [&w, core](auto& creqs, auto& cidx) {
                       wal_create_request c = std::move(creqs[cidx++]);
                       while (c.runner_core != core) {
                           c = std::move(creqs[cidx++]);
                       }
                       LOG_INFO(
                         "CREATE:: ({}) - runner core: {}, and core: {}",
                         creqs.size(),
                         c.runner_core,
                         core);

                       return w.create(std::move(c)).then([core](auto r) {
                           LOG_INFO("Topic created on core: {}", core);
                           return make_ready_future<>();
                       });
                   })
            .then([&input, &w, &offsets, core] {
                return do_with(
                         input.write_requests(),
                         std::size_t(0),
                         [&](auto& wreqs, std::size_t& idx) {
                             wal_write_request write = std::move(wreqs[idx++]);
                             while (write.runner_core != core) {
                                 write = std::move(wreqs[idx++]);
                             }
                             LOG_INFO(
                               "WRITE:: runner core: {}, and core: {}",
                               write.runner_core,
                               core);
                             return w.append(std::move(write))
                               .then([&](auto write_reply) mutable {
                                   LOG_INFO("Write reply: {}", *write_reply);
                                   for (auto& it : *write_reply) {
                                       auto r
                                         = std::make_unique<wal_get_requestT>();
                                       r->ns = xxhash_64(
                                         kNS.c_str(), kNS.size());
                                       r->topic = xxhash_64(
                                         kTopic.c_str(), kTopic.size());
                                       r->partition = it.second->partition;
                                       r->offset = it.second->start_offset;
                                       r->max_bytes = it.second->end_offset
                                                      - it.second->start_offset;
                                       offsets.push_back(std::move(r));
                                   }
                               });
                         })
                  .then([] {
                      // wait for flush period
                      return sleep(std::chrono::milliseconds(50));
                  });
            })
            .then([&]() mutable { return std::move(offsets); });
      });
}

future<> do_reads(
  uint32_t core,
  write_ahead_log& w,
  std::vector<std::unique_ptr<wal_get_requestT>>&& gets) {
    return do_with(std::move(gets), [core, &w](auto& offsets) {
        return do_for_each(
          offsets.begin(), offsets.end(), [&](auto& req_ptr) mutable {
              auto readq = smf::fbs_typed_buf<wal_get_request>(
                smf::native_table_as_buffer<wal_get_request>(*req_ptr));
              // perform the read next!
              return do_with(
                       std::move(readq),
                       [&w](smf::fbs_typed_buf<wal_get_request>& tbuf) {
                           auto r = wal_core_mapping::core_assignment(
                             tbuf.get());
                           LOG_INFO("Making get request: {}", r);
                           return w.get(r);
                       })
                .then([](std::unique_ptr<wal_read_reply> r) {
                    LOG_THROW_IF(r->reply().gets.size() == 0, "Bad reads");
                    auto sz = std::accumulate(
                      r->reply().gets.begin(),
                      r->reply().gets.end(),
                      int64_t(0),
                      [](auto acc, auto& n) { return acc + n->data.size(); });

                    // This ONLY tests appending writes of course, so
                    // comment out if you start changing the test to write
                    // in the middle
                    LOG_THROW_IF(
                      sz != r->on_disk_size(),
                      "Failed read (likely bad append): {}, size {} did not "
                      "match size on disk {}",
                      *r,
                      sz,
                      r->on_disk_size());
                    LOG_INFO(
                      "All reader checks pass.  next_offset={}, total "
                      "gets={}",
                      r->reply().next_offset,
                      r->reply().gets.size());
                    return make_ready_future<>();
                });
          });
    });
}

future<> do_one_request(uint32_t core, write_ahead_log& w) {
    LOG_INFO("Performing work on core: {}", core);
    return do_writes(core, w).then([core, &w](auto&& to_read) {
        return do_reads(core, w, std::move(to_read));
    });
}

int main(int args, char** argv, char** env) {
    std::cout.setf(std::ios::unitbuf);

    DLOG_DEBUG("About to start the client");
    app_template app;
    distributed<write_ahead_log> w;

    try {
        add_opts(app.add_options());

        return app.run(args, argv, [&] {
            smf::app_run_log_level(log_level::trace);
            DLOG_DEBUG("setting up exit hooks");
            engine().at_exit([&] { return w.stop(); });
            DLOG_DEBUG("about to start the wal.h");
            auto& config = app.configuration();
            auto dir = config["write-ahead-log-dir"].as<std::string>();

            return w.start(wal_opts(dir, std::chrono::milliseconds(2)))
              .then([&] { return w.invoke_on_all(&write_ahead_log::open); })
              .then([&] {
                  return do_for_each(
                    boost::counting_iterator<uint32_t>(0),
                    boost::counting_iterator<uint32_t>(smp::count),
                    [&](auto core) mutable {
                        return w.invoke_on(
                          core, [core](write_ahead_log& localwal) {
                              return do_one_request(core, localwal);
                          });
                    });
              })
              .then([] { return make_ready_future<int>(0); });
        });
    } catch (const std::exception& e) {
        std::cerr << "Fatal exception: " << e.what() << std::endl;
    }
}

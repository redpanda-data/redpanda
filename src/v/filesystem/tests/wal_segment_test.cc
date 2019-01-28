#include <seastar/core/app-template.hh>
#include <seastar/core/distributed.hh>
#include <seastar/core/fstream.hh>
#include <seastar/core/sstring.hh>

#include "ioutil/priority_manager.h"
#include "ioutil/readfile.h"
#include <smf/log.h>

#include "wal_segment.h"

static const char *kFileName = "fiorito.txt";
static const std::string segment_name = std::string("wal_segment_") + kFileName;
static const std::string ostream_name = std::string("ostream_") + kFileName;

static const char *kPoem = "How do I love thee? Let me count the ways."
                           "I love thee to the depth and breadth and height"
                           "My soul can reach, when feeling out of sight"
                           "For the ends of being and ideal grace."
                           "I love thee to the level of every day's"
                           "Most quiet need, by sun and candle-light."
                           "I love thee freely, as men strive for right."
                           "I love thee purely, as they turn from praise."
                           "I love thee with the passion put to use"
                           "In my old griefs, and with my childhood's faith."
                           "I love thee with a love I seemed to lose"
                           "With my lost saints. I love thee with the breath,"
                           "Smiles, tears, of all my life; and, if God choose,"
                           "I shall but love thee better after death.";

seastar::future<>
skip_first_page(seastar::sstring base_dir) {
  // This test is only useful insofar as breaking on systems
  // that do not support sparse files
  //
  return open_file_dma(base_dir + "/leFiorito.txt",
                       seastar::open_flags::rw | seastar::open_flags::create |
                         seastar::open_flags::truncate)
    .then([](seastar::file fx) mutable {
      auto f = seastar::make_lw_shared<seastar::file>(std::move(fx));
      return seastar::do_with(seastar::semaphore(4), [f](auto &limit) {
        return seastar::do_for_each(
                 boost::counting_iterator<uint32_t>(0),
                 boost::counting_iterator<uint32_t>(10),
                 [f, &limit](auto &c) mutable {
                   // ON PURPOSE - testing to see if you can skip the first
                   // page
                   if (c == 0) { return seastar::make_ready_future<>(); }

                   return limit.wait(1).then([c, f, &limit] {
                     const auto alignment = f->disk_write_dma_alignment();
                     std::unique_ptr<char[], seastar::free_deleter> buf =
                       seastar::allocate_aligned_buffer<char>(alignment,
                                                              alignment);
                     std::memset(buf.get(), '-', alignment);

                     f->dma_write(c * alignment, buf.get(), alignment,
                                  v::priority_manager::get().default_priority())
                       .then([alignment](auto sz) {
                         LOG_THROW_IF(sz != alignment,
                                      "Wrote some garbage size of: {}", sz);
                         return seastar::make_ready_future<>();
                       })
                       .finally([&limit] { limit.signal(1); });
                   });
                 })
          .then([&limit] { return limit.wait(4); })
          .then([f] {
            return f->flush().then([f] { return f->close().finally([f] {}); });
          });
      });
    });
}

seastar::future<>
write_poem_ostream(seastar::sstring base_dir, uint32_t max_appends = 100) {
  return open_file_dma(base_dir + "/" + ostream_name,
                       seastar::open_flags::rw | seastar::open_flags::create |
                         seastar::open_flags::truncate)
    .then([max_appends](seastar::file f) mutable {
      auto fstrm = seastar::make_lw_shared<seastar::output_stream<char>>(
        seastar::make_file_output_stream(std::move(f)));
      return seastar::do_for_each(
               boost::counting_iterator<uint32_t>(0),
               boost::counting_iterator<uint32_t>(max_appends),
               [fx = fstrm](auto &c) mutable {
                 return fx->write(kPoem, std::strlen(kPoem));
               })
        .then([fx = fstrm] {
          return fx->flush().then(
            [fx] { return fx->close().finally([fx] {}); });
        });
    });
}

seastar::future<>
write_poem_wal_segment(seastar::sstring base_dir, uint32_t max_appends = 100) {
  auto ws = seastar::make_lw_shared<v::wal_segment>(
    base_dir + "/" + segment_name,
    v::priority_manager::get().streaming_write_priority(),
    std::numeric_limits<int32_t>::max() /*max file in bytes*/,
    1024 * 1024 /*1MB*/, 4 /* concurrency on writes*/);
  return ws->open()
    .then([ws, max_appends] {
      return seastar::do_for_each(
        boost::counting_iterator<uint32_t>(0),
        boost::counting_iterator<uint32_t>(max_appends), [ws](auto &c) mutable {
          return ws->append(kPoem, std::strlen(kPoem));
        });
    })
    .then([ws] { return ws->close().finally([ws] {}); });
}

void
add_opts(boost::program_options::options_description_easy_init o) {
  namespace po = boost::program_options;
  o("write-ahead-log-dir", po::value<std::string>(), "log directory");

  o("max-page-test", po::value<uint32_t>()->default_value(10),
    "if false, no server_hdr.hgrm will be printed");
}

int
main(int args, char **argv, char **env) {
  seastar::app_template app;
  try {
    add_opts(app.add_options());

    return app.run(args, argv, [&] {
      smf::app_run_log_level(seastar::log_level::trace);
      auto &config = app.configuration();

      const uint32_t max_page_comparison =
        config["max-page-test"].as<uint32_t>();
      auto dir = config["write-ahead-log-dir"].as<std::string>();
      LOG_THROW_IF(dir.empty(), "Empty write-ahead-log-dir");
      LOG_INFO("log_segment test dir: {}", dir);

      return skip_first_page(dir)
        .then([max_page_comparison, dir] {
          return write_poem_ostream(dir, max_page_comparison);
        })
        .then([max_page_comparison, dir] {
          return write_poem_wal_segment(dir, max_page_comparison);
        })
        .then([dir] {
          return v::readfile(dir + "/" + ostream_name).then([dir](auto obuf) {
            return v::readfile(dir + "/" + segment_name)
              .then([ostream_buf = std::move(obuf), &dir](auto segment) {
                LOG_THROW_IF(ostream_buf.size() != segment.size(),
                             "Size of files is not the same. "
                             "ostream_buf:{} != segment:{}",
                             ostream_buf.size(), segment.size());
                LOG_THROW_IF(
                  std::memcmp(ostream_buf.get(), segment.get(), segment.size()),
                  "our wal_segment writes and seastar's "
                  "ostream<char> do not "
                  "have identical outputs. severe error");
                return seastar::make_ready_future<int>(0);
              });
          });
        });
    });
  } catch (const std::exception &e) {
    std::cerr << "Fatal exception: " << e.what() << std::endl;
  }
}

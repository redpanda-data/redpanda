#include "wal_generated.h"
#include "wal_segment_record.h"

#include <seastar/core/app-template.hh>
#include <seastar/core/distributed.hh>
#include <seastar/core/sstring.hh>

#include <smf/log.h>
#include <smf/random.h>

void small_tests() {
    smf::random rand;
    for (auto i = 1; i < 256; ++i) {
        auto keyinput = rand.next_alphanum(i);
        auto valinput = rand.next_alphanum(i);

        // The test!
        // effectively - round trip the encoding decoding
        //
        auto x = wal_segment_record::coalesce(
          keyinput.data(),
          keyinput.size(),
          valinput.data(),
          valinput.size(),
          wal_compression_type::wal_compression_type_none);

        /// Since we didn't enable compression :)
        LOG_THROW_IF(
          x->data.size()
            != sizeof(wal_header) + keyinput.size() + valinput.size(),
          "Invalid record size encoding. Got: {}, expected: {}",
          x->data.size(),
          keyinput.size() + valinput.size());

        auto [kbuf, vbuf] = wal_segment_record::extract_from_bin(
          (const char*)x->data.data(), x->data.size());

        // get it in comparable format and test
        seastar::sstring keyout(kbuf.get(), kbuf.size());
        seastar::sstring valout(vbuf.get(), vbuf.size());

        // roundtrip assertions :)
        LOG_THROW_IF(
          keyinput != keyout,
          "Could not decode keys. Input: {}, decoded: {}",
          keyinput,
          keyout);
        LOG_THROW_IF(
          valinput != valout,
          "Could not decode values. Input: {}, decoded: {}",
          valinput,
          valout);
    }
}

void compression_tests() {
    smf::random rand;
    std::vector<std::pair<int, wal_compression_type>> fixtures{
      {1, wal_compression_type::wal_compression_type_lz4},
      {10, wal_compression_type::wal_compression_type_lz4},
      {1024, wal_compression_type::wal_compression_type_lz4},
      {10240, wal_compression_type::wal_compression_type_lz4},
      {1, wal_compression_type::wal_compression_type_zstd},
      {10, wal_compression_type::wal_compression_type_zstd},
      {1024, wal_compression_type::wal_compression_type_zstd},
      {10240, wal_compression_type::wal_compression_type_zstd},
    };
    for (auto [i, ctype] : fixtures) {
        auto keyinput = rand.next_alphanum(i);
        auto valinput = rand.next_alphanum(i);
        auto x = wal_segment_record::coalesce(
          keyinput.data(),
          keyinput.size(),
          valinput.data(),
          valinput.size(),
          ctype);
        auto [kbuf, vbuf] = wal_segment_record::extract_from_bin(
          (const char*)x->data.data(), x->data.size());
        // get it in comparable format and test
        seastar::sstring keyout(kbuf.get(), kbuf.size());
        seastar::sstring valout(vbuf.get(), vbuf.size());
        // roundtrip assertions :)
        LOG_THROW_IF(
          keyinput != keyout,
          "Could not decode keys. Input: {}, decoded: {}",
          keyinput,
          keyout);
        LOG_THROW_IF(
          valinput != valout,
          "Could not decode values. Input: {}, decoded: {}",
          valinput,
          valout);
    }
}

int main(int args, char** argv, char** env) {
    // set only to debug things
    std::cout.setf(std::ios::unitbuf);
    seastar::app_template app;
    return app.run(args, argv, [&] {
        smf::app_run_log_level(seastar::log_level::trace);
        small_tests();
        compression_tests();
        return seastar::make_ready_future<int>(0);
    });
}

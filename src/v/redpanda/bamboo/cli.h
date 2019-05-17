#pragma once

#include <memory>

#include <boost/program_options.hpp>
#include <smf/random.h>

// main api
#include "redpanda/api/client.h"

class cli {
 public:
  explicit cli(const boost::program_options::variables_map *cfg);
  ~cli();

  seastar::future<> one_write();

  seastar::future<> one_read();

  seastar::future<> open();

  seastar::future<> stop();

  const boost::program_options::variables_map &options() const;

  const boost::program_options::variables_map *opts;

  api::client *api() const;

  uint64_t
  id() const {
    return id_;
  }

 private:
  smf::random rand_;
  std::unique_ptr<api::client> api_;
  uint64_t id_;
  int32_t write_key_sz_;
  int32_t write_val_sz_;
  int32_t write_batch_sz_;
  int32_t partition_pref_;
};


#include "kafka/requests/fetch_request.h"
#include "redpanda/tests/fixture.h"
#include "resource_mgmt/io_priority.h"

#include <seastar/core/smp.hh>

SEASTAR_THREAD_TEST_CASE(partition_iterator) {
    /*
     * extract topic partitions from the request
     */
    auto transform = [](const kafka::fetch_request& request) {
        std::vector<model::topic_partition> res;
        std::transform(
          request.cbegin(),
          request.cend(),
          std::back_inserter(res),
          [&res](const kafka::fetch_request::const_iterator::value_type& v) {
              if (v.new_topic) {
                  if (!res.empty()) {
                      BOOST_TEST(res.back().topic != v.topic->name);
                  }
              } else {
                  BOOST_TEST(!res.empty());
                  BOOST_TEST(res.back().topic == v.topic->name);
              }
              return model::topic_partition{
                .topic = v.topic->name,
                .partition = v.partition->id,
              };
          });
        return res;
    };

    {
        // no topics -> empty
        kafka::fetch_request req;
        auto parts = transform(req);
        BOOST_TEST(parts.empty());
    }

    {
        // 1 topic, no partitions -> empty
        kafka::fetch_request req;
        req.topics.push_back({.name = model::topic("t0")});
        auto parts = transform(req);
        BOOST_TEST(parts.empty());
    }

    {
        // 2 topics, no partitions -> empty
        kafka::fetch_request req;
        req.topics.push_back({.name = model::topic("t0")});
        req.topics.push_back({.name = model::topic("t1")});
        auto parts = transform(req);
        BOOST_TEST(parts.empty());
    }

    {
        // 1 topic, 1 partition
        kafka::fetch_request req;
        req.topics.push_back({
          .name = model::topic("t0"),
          .partitions = {{.id = model::partition_id(100)}},
        });
        auto parts = transform(req);
        BOOST_TEST(parts.size() == 1);
        BOOST_TEST(parts[0].topic == model::topic("t0"));
        BOOST_TEST(parts[0].partition == 100);
    }

    {
        // 1 topic, 2 partitions
        kafka::fetch_request req;
        req.topics.push_back(
          {.name = model::topic("t0"),
           .partitions = {{.id = model::partition_id(100)},
                          {.id = model::partition_id(101)}}});
        auto parts = transform(req);
        BOOST_TEST(parts.size() == 2);
        BOOST_TEST(parts[0].topic == model::topic("t0"));
        BOOST_TEST(parts[0].partition == 100);
        BOOST_TEST(parts[1].topic == model::topic("t0"));
        BOOST_TEST(parts[1].partition == 101);
    }

    {
        // 2 topics, 2/1 partition
        kafka::fetch_request req;
        req.topics.push_back(
          {.name = model::topic("t0"),
           .partitions = {{.id = model::partition_id(100)},
                          {.id = model::partition_id(101)}}});
        req.topics.push_back(
          {.name = model::topic("t1"),
           .partitions = {{.id = model::partition_id(102)}}});
        auto parts = transform(req);
        BOOST_TEST(parts.size() == 3);
        BOOST_TEST(parts[0].topic == model::topic("t0"));
        BOOST_TEST(parts[0].partition == 100);
        BOOST_TEST(parts[1].topic == model::topic("t0"));
        BOOST_TEST(parts[1].partition == 101);
        BOOST_TEST(parts[2].topic == model::topic("t1"));
        BOOST_TEST(parts[2].partition == 102);
    }

    {
        // 4 topics, 2/{}/{}/2 partition
        kafka::fetch_request req;
        req.topics.push_back(
          {.name = model::topic("t0"),
           .partitions = {{.id = model::partition_id(100)},
                          {.id = model::partition_id(101)}}});
        req.topics.push_back({.name = model::topic("t1")});
        req.topics.push_back({.name = model::topic("t2")});
        req.topics.push_back(
          {.name = model::topic("t3"),
           .partitions = {{.id = model::partition_id(102)},
                          {.id = model::partition_id(103)}}});
        auto parts = transform(req);
        BOOST_TEST(parts.size() == 4);
        BOOST_TEST(parts[0].topic == model::topic("t0"));
        BOOST_TEST(parts[0].partition == 100);
        BOOST_TEST(parts[1].topic == model::topic("t0"));
        BOOST_TEST(parts[1].partition == 101);
        BOOST_TEST(parts[2].topic == model::topic("t3"));
        BOOST_TEST(parts[2].partition == 102);
        BOOST_TEST(parts[3].topic == model::topic("t3"));
        BOOST_TEST(parts[3].partition == 103);
    }
}

// TODO: this sort of factory should eventually go into a kafka fixture that
// builds on top of the redpanda application fixture.
static kafka::request_context make_request_context(application& app) {
    kafka::request_header header;
    auto encoder_context = kafka::request_context(
      app.metadata_cache,
      app.cntrl_dispatcher.local(),
      std::move(header),
      iobuf(),
      std::chrono::milliseconds(0),
      app.group_router.local(),
      app.shard_table.local(),
      app.partition_manager);

    iobuf buf;
    kafka::fetch_request request;
    kafka::response_writer writer(buf);
    request.encode(encoder_context, writer);

    return kafka::request_context(
      app.metadata_cache,
      app.cntrl_dispatcher.local(),
      std::move(header),
      std::move(buf),
      std::chrono::milliseconds(0),
      app.group_router.local(),
      app.shard_table.local(),
      app.partition_manager);
}

// TODO: when we have a more precise log builder tool we can make these finer
// grained tests. for now the test is coarse grained based on the random batch
// builder.
FIXTURE_TEST(read_from_ntp_max_bytes, redpanda_thread_fixture) {
    auto do_read = [this](model::ntp ntp, size_t max_bytes) {
        kafka::fetch_config config{
          .start_offset = model::offset(0),
          .max_bytes = max_bytes,
          .timeout = model::no_timeout,
        };
        auto rctx = make_request_context(app);
        auto octx = kafka::op_context(
          std::move(rctx), default_smp_service_group());
        auto resp = kafka::read_from_ntp(octx, ntp, config).get0();
        return resp;
    };

    auto ntp = make_data();

    auto zero = do_read(ntp, 0).record_set.size_bytes();
    auto one = do_read(ntp, 1).record_set.size_bytes();
    auto maxlimit = do_read(ntp, std::numeric_limits<size_t>::max())
                      .record_set.size_bytes();

    BOOST_TEST(zero > 0); // read something
    BOOST_TEST(zero == one);
    BOOST_TEST(one < maxlimit); // read more
}

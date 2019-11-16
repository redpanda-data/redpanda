#include "librdkafka/rdkafkacpp.h"
#include "redpanda/tests/fixture.h"
#include "test_utils/fixture.h"

#include <cppkafka/cppkafka.h>

seastar::future<> get_metadata(v::ThreadPool& tp) {
    return tp.submit([]() {
        cppkafka::Configuration config = {
          {"metadata.broker.list", "127.0.0.1:9092"}};

        cppkafka::Producer producer(config);
        cppkafka::Metadata metadata = producer.get_metadata();

        if (!metadata.get_topics().empty()) {
            throw new std::runtime_error("expected topic set to be empty");
        }
    });
}

FIXTURE_TEST(get_metadadata, redpanda_test_fixture) {
    get_metadata(thread_pool).get();
}

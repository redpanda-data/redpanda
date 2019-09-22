#define BOOST_TEST_MODULE model
#include "model/fundamental.h"

#include <boost/test/unit_test.hpp>

#include <utility>

BOOST_AUTO_TEST_CASE(ntp_path_test) {
    auto ntp = model::namespaced_topic_partition{
      model::ns("sf"),
      model::topic_partition{model::topic("richmond"),
                             model::partition_id(94118)}};

    BOOST_CHECK_EQUAL("sf/richmond/94118", ntp.path());
}

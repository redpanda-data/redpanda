#include "base/seastarx.h"
#include "ssx/semaphore.h"
#include "utils/rwlock.h"

#include <seastar/core/loop.hh>
#include <seastar/core/sleep.hh>
#include <seastar/testing/thread_test_case.hh>

SEASTAR_THREAD_TEST_CASE(test_rwlock_r_blocks_w) {
    ssx::rwlock rwlock;
    auto runit = rwlock.attempt_read_lock();
    BOOST_REQUIRE(runit);
    auto wunit = rwlock.attempt_write_lock();
    BOOST_REQUIRE(!wunit);
}

SEASTAR_THREAD_TEST_CASE(test_rwlock_w_blocks_r) {
    ssx::rwlock rwlock;
    auto wunit = rwlock.attempt_write_lock();
    BOOST_REQUIRE(wunit);
    auto runit = rwlock.attempt_read_lock();
    BOOST_REQUIRE(!runit);
}

SEASTAR_THREAD_TEST_CASE(test_rwlock_r_permits_r) {
    ssx::rwlock rwlock;
    auto unit1 = rwlock.attempt_read_lock();
    BOOST_REQUIRE(unit1);
    auto unit2 = rwlock.attempt_read_lock();
    BOOST_REQUIRE(unit2);
}

SEASTAR_THREAD_TEST_CASE(test_rwlock_w_blocks_w) {
    ssx::rwlock rwlock;
    auto unit1 = rwlock.attempt_write_lock();
    BOOST_REQUIRE(unit1);
    auto unit2 = rwlock.attempt_write_lock();
    BOOST_REQUIRE(!unit2);
}

void some_processing(ssx::rwlock_unit&& unit) {
    ssx::rwlock_unit unit3(std::move(unit));
}

SEASTAR_THREAD_TEST_CASE(test_rwlock_unit_move) {
    ssx::rwlock rwlock;
    auto unit1 = rwlock.attempt_write_lock();
    BOOST_REQUIRE(unit1);
    some_processing(std::move(unit1.value()));

    auto unit2 = rwlock.attempt_write_lock();
    BOOST_REQUIRE(unit2);
}

SEASTAR_THREAD_TEST_CASE(test_rwlock_assign) {
    ssx::rwlock rwlock;
    auto unit1 = rwlock.attempt_write_lock();
    BOOST_REQUIRE(unit1);
    BOOST_REQUIRE(unit1.value());

    ssx::rwlock_unit unit2 = std::move(unit1.value());

    BOOST_REQUIRE(!unit1.value());
    BOOST_REQUIRE(unit2);
}

#pragma once

#include <seastar/util/gcc6-concepts.hh>

#define CONCEPT(...) GCC6_CONCEPT(__VA_ARGS__)

# -DHONEY_BADGER

[honey badger](https://www.youtube.com/watch?v=4r7wHMg5Yjg)
is our failure injector framework.

It is composed of 4 main components:

1) A `luajit` script named `honey_badger.lua` in the directory where you run the binary

2) Linking against `<name>_hbadger` enabled libraries (see our `rp_hbadger_lib`)

3) Code taged w/ `HBADGER(module, test)` name

4) Any binary that has linked against `<name>_hbadger` libs 

## Overview 

### 1) The lua script

```lua

print(jit.version)
-- return a tuple of
-- 1) Wether or not to inject a failure
-- 2) Error code
-- 3) Error category
-- 4) Explanation
function honey_badger_fn (filename, line, cpp_module, cpp_func)
  is_exception = true
  error_code = 66
  category = cpp_module
  msg = cpp_func .. ": Generated Failure From Static Function"
  return is_exception, error_code, category, msg
end


```

### 2) Linking against <name>_hbadger libs

To enable hbadger symbols and luajit library linking just use 
this cmake function `rp_hbadger_lib` which will create 2 versions
of the same library, one with postfix `<name>_hbadger`

Here is an example from our filesystem hbadger enabled library

```cmake

rp_hbadger_lib(
  STATIC
  NAME rpfs
  SOURCES ${rpfs_sources}
  INCLUDES ${CMAKE_CURRENT_BINARY_DIR}
  INCLUDES ${PROJECT_SOURCE_DIR}/src/v
  COMPILE_OPTIONS -Wall -Werror -fconcepts
  LIBRARIES smf re2
  )

```

It should be no more work to define a honey badger lib than not.
Please use it in your code


### 3) Code tagged with `HBADGER(module, test)`

```cpp

#include "hbadger.h"

void
foo() {
  HBADGER(filesystem, foo);
  // HBADGER for this test will *always* throw
  std::cout << "Lua allowed me to live!" << std::endl;
}


```

This snippet of code when compiled with `habadger libs` will 
make calls to the lua script named `honey_badger.lua` as 
described in `1)`.

If the lua script returns a tuple with `is_exception=true` 
this code will throw a `std::runtime_error`

For example (real example from a test):

```cpp

seastar - Exiting on unhandled exception: std::runtime_error (honey_badger_failure{Code: 66, Category: filesystem, Details: wal_segment::append: Inject Permanent Failure})

```

### 4) Any binary linked against an `<name>_hbadger` enable lib

```cpp

rp_test(
  HBADGER_TEST
  BINARY_NAME wal_segment_indexer_append_fail
  SOURCES wal_segment_indexer_basic_test.cc
  LIBRARIES rpfs_hbadger
  INPUT_FILES
  "${CMAKE_CURRENT_SOURCE_DIR}/hb_fail_wal_segment_append.lua=honey_badger.lua"
  INCLUDES
    ${PROJECT_SOURCE_DIR}/src/third_party/smf/src/third_party
  ARGS "-c 1 --write-ahead-log-dir=.  2>/dev/null | grep 'wal_segment::append: Inject Permanent Failure'"
)

```

This produces a binary `wal_segment_indexer_append_fail_hbadger` linked 
to the `rpfs_hbadger` library


# PROFIT!

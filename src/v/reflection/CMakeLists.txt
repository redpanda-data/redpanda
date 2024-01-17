v_cc_library (
  NAME reflection
  HDRS 
    "ariy.h"
    "for_each_field.h"
    "to_tuple.h"
    "adl.h"
  DEPS 
    Seastar::seastar
    absl::btree
    v::bytes
) 

add_subdirectory(test)

v_cc_library (
  NAME reflection
  HDRS 
    "ariy.h"
    "for_each_field.h"
    "to_tuple.h"
    "adl.h"
  DEPS 
    Seastar::seastar
) 

add_subdirectory(test)

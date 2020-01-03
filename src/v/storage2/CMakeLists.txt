add_library(storage2 STATIC
     fileset.cc
     vassert.cc)

 target_link_libraries(storage2 PUBLIC
   Seastar::seastar
   bytes
   config
   rphashing
   finjector)

 add_subdirectory(tests)
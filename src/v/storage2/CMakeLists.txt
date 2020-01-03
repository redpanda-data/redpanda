add_library(storage2 STATIC
     indices.cc
     fileset.cc
     vassert.cc
     offset_index.cc
     timestamp_index.cc
     segment.cc
     segment_writer.cc
     segment_appender.cc
     partition.cc)

 target_link_libraries(storage2 PUBLIC
   Seastar::seastar
   bytes
   config
   rphashing
   finjector)

 add_subdirectory(tests)
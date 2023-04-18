find_path (Avro_INCLUDE_DIR
  NAMES avro/AvroParse.hh
  PATH_SUFFIXES avro)

if (BUILD_SHARED_LIBS)
  find_library (Avro_LIBRARY NAMES avrocpp)
else()
  find_library (Avro_LIBRARY NAMES avrocpp_s)
endif()

mark_as_advanced (
  Avro_INCLUDE_DIR
  Avro_LIBRARY)

include (FindPackageHandleStandardArgs)

find_package_handle_standard_args (Avro
  REQUIRED_VARS
    Avro_LIBRARY
    Avro_INCLUDE_DIR)

set (Avro_INCLUDE_DIRS ${Avro_INCLUDE_DIR})
set (Avro_LIBRARIES ${Avro_LIBRARY})

if (Avro_FOUND AND NOT (TARGET Avro::avro))
  add_library (Avro::avro UNKNOWN IMPORTED)
  set_target_properties (Avro::avro
    PROPERTIES
      IMPORTED_LOCATION ${Avro_LIBRARIES}
      INTERFACE_INCLUDE_DIRECTORIES ${Avro_INCLUDE_DIRS})
endif ()

find_library(LIBCXX_LIBRARY NAMES c++ cxx) 

find_path(LIBCXX_INCLUDE_DIR
  NAMES "c++/v1/algorithm")

mark_as_advanced(
    LIBCXX_LIBRARY
    LIBCXX_INCLUDE_DIR)

include (FindPackageHandleStandardArgs)

find_package_handle_standard_args(LibCxx
      REQUIRED_VARS
        LIBCXX_LIBRARY
        LIBCXX_INCLUDE_DIR)
    

if (LIBCXX_LIBRARY AND NOT (TARGET Libcxx::libcxx))
    set(LIBCXX_INCLUDE_DIR ${LIBCXX_INCLUDE_DIR}/c++/v1/)
    add_library (Libcxx::libcxx UNKNOWN IMPORTED)
    set_target_properties (Libcxx::libcxx
     PROPERTIES
       IMPORTED_LOCATION ${LIBCXX_LIBRARY}
       INTERFACE_INCLUDE_DIRECTORIES ${LIBCXX_INCLUDE_DIR})
endif()

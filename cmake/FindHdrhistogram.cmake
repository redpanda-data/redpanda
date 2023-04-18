find_path (Hdrhistogram_INCLUDE_DIR
  NAMES hdr_histogram.h
  PATH_SUFFIXES hdr)

if (BUILD_SHARED_LIBS)
  find_library (Hdrhistogram_LIBRARY
    NAMES hdr_histogram)
else()
  find_library (Hdrhistogram_LIBRARY
    NAMES hdr_histogram_static)
endif()

mark_as_advanced (
  Hdrhistogram_INCLUDE_DIR
  Hdrhistogram_LIBRARY)

include (FindPackageHandleStandardArgs)

find_package_handle_standard_args (Hdrhistogram
  REQUIRED_VARS
    Hdrhistogram_LIBRARY
    Hdrhistogram_INCLUDE_DIR)

set (Hdrhistogram_INCLUDE_DIRS ${Hdrhistogram_INCLUDE_DIR})
set (Hdrhistogram_LIBRARIES ${Hdrhistogram_LIBRARY})

if (Hdrhistogram_FOUND AND NOT (TARGET Hdrhistogram::hdr_histogram))
  add_library (Hdrhistogram::hdr_histogram UNKNOWN IMPORTED)

  set_target_properties (Hdrhistogram::hdr_histogram
    PROPERTIES
      IMPORTED_LOCATION ${Hdrhistogram_LIBRARIES}
      INTERFACE_INCLUDE_DIRECTORIES ${Hdrhistogram_INCLUDE_DIRS})
endif ()

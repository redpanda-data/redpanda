add_library (errors STATIC
    errors.cc)

    
target_link_libraries(errors
    PUBLIC
      Seastar::seastar
      cluster
    )
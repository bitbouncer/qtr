add_executable(qtr-parquet-schema-gen-cpp qtr-parquet-schema-gen-cpp.cpp generator.cpp type_conv.cpp)
target_link_libraries(qtr-parquet-schema-gen-cpp ${ALL_LIBS})
INSTALL(TARGETS qtr-parquet-schema-gen-cpp RUNTIME DESTINATION bin)

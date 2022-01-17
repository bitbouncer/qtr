add_executable(test_parquet test_parquet.cpp)
target_link_libraries(test_parquet  gtest ${ALL_LIBS})

add_executable(test_decimal test_decimal.cpp)
target_link_libraries(test_decimal gtest ${ALL_LIBS} )

add_executable(code-gen-test_x code-gen-test/main.cpp code-gen-test/test1_v1.cpp)
target_link_libraries(code-gen-test_x gtest ${ALL_LIBS} )



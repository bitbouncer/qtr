add_executable(qtr-parquet2parquet  qtr-parquet2parquet.cpp)
target_link_libraries(qtr-parquet2parquet ${ALL_LIBS})
INSTALL(TARGETS qtr-parquet2parquet RUNTIME DESTINATION bin)

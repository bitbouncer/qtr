add_executable(qtr-csv2parquet qtr-csv2parquet.cpp)
target_link_libraries(qtr-csv2parquet ${ALL_LIBS})
INSTALL(TARGETS qtr-csv2parquet RUNTIME DESTINATION bin)

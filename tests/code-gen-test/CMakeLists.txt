include_directories(${CMAKE_SOURCE_DIR}/../..//include)
set(CMAKE_CXX_STANDARD 17)

add_definitions(-D_FILE_OFFSET_BITS=64 -D_REENTRANT -DEXTERNAL_LOCKS -DMULTITHREAD)
add_definitions(-fPIC)
add_definitions(-Wno-deprecated)
add_definitions(-march=haswell)

set(BOOST_LIBS boost_program_options boost_iostreams boost_filesystem boost_system boost_regex z bz2 pthread rt c)
SET(AWS_S3_LIBS aws-cpp-sdk-s3 aws-cpp-sdk-core aws-c-event-stream aws-checksums aws-c-common)
SET(ARROW_LIBS arrow_flight arrow arrow_dataset parquet)
SET(PROTOBUF_LIBS protobuf gpr upb address_sorting re2 crypto ssl)
SET(ABSL_LIBS absl_debugging_internal
        absl_time
        absl_time_zone
        absl_cord
        absl_status
        absl_synchronization
        absl_graphcycles_internal
        absl_symbolize
        absl_demangle_internal
        absl_dynamic_annotations
        absl_leak_check
        absl_stacktrace
        absl_malloc_internal
        absl_strings
        absl_strings_internal
        absl_str_format_internal
        absl_raw_logging_internal
        absl_int128
        absl_throw_delegate
        absl_base
        absl_spinlock_wait
        absl_flags
        absl_flags_internal
        absl_bad_optional_access)

SET(PROMETHEUS_CPP_LIBS prometheus-cpp-push prometheus-cpp-core curl)

#${PROMETHEUS_CPP_LIBS}
SET(EXT_LIBS ${ARROW_LIBS} ${AWS_S3_LIBS} ${BOOST_LIBS} date-tz snappy glog gflags stdc++fs dl pq)
set(ALL_LIBS qtr ${EXT_LIBS})

add_executable(autogen_test1 main.cpp test1_v1.cpp test1_v1.h schema_test_all.cpp schema_test_all.h)
target_link_libraries(autogen_test1 ${ALL_LIBS})

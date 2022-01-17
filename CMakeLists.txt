cmake_minimum_required(VERSION 3.16)
project(qtr)

set(CMAKE_CXX_STANDARD 17)
list(APPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_LIST_DIR}/cmake")
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

#GLOBAL DEFINITIONS
add_definitions(-DRAPIDJSON_HAS_STDSTRING)

set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -Wall -Wpedantic -Wreturn-type -Ofast")
set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -Wall  -Wpedantic -Wreturn-type -g -O0")
#set(CMAKE_CXX_CLANG_TIDY "clang-tidy")

set(LIBRARY_OUTPUT_PATH ${CMAKE_SOURCE_DIR}/lib)
set(EXECUTABLE_OUTPUT_PATH ${CMAKE_SOURCE_DIR}/bin)

include_directories(${CMAKE_SOURCE_DIR}/include ${CMAKE_SOURCE_DIR} PUBLIC)

INSTALL(DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/include/ DESTINATION include)

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

add_subdirectory(src/qtr)

#${PROMETHEUS_CPP_LIBS}
SET(EXT_LIBS ${ARROW_LIBS} ${AWS_S3_LIBS} ${BOOST_LIBS} date-tz snappy glog gflags stdc++fs dl pq)
set(ALL_LIBS qtr ${EXT_LIBS})

add_subdirectory(programs)
add_subdirectory(tests)


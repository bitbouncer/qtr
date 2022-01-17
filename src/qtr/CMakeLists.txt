FILE(GLOB qtr_SRCS
        *.cpp
        internal/*.cpp
        io/*.cpp
        parquet/*.cpp
        schema/*.cpp
        utils/*.cpp)

ADD_LIBRARY(qtr STATIC ${qtr_SRCS})
INSTALL(TARGETS qtr LIBRARY DESTINATION lib ARCHIVE DESTINATION lib)




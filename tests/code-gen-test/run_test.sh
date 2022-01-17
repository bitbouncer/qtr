#!/bin/bash
../../bin/qtr-parquet-schema-gen-cpp ../test_data/test1_v1.parquet-schema.json .

rm -rf build
mkdir build && cd build
cmake ..

make



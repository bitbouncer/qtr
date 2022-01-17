#!/bin/sh
set -ef
#
docker pull registry.bitbouncer.com:5000/gcc-base-build

TAG_NAME=${1:-qtr:latest}

export BASE_NAME=qtr
export BUILD_CONTAINER_IMAGE=${BASE_NAME}-build
export EXTRACT_CONTAINER=${BASE_NAME}-extract

rm -rf ./extract
mkdir -p ./extract/bin
mkdir -p ./extract/lib
mkdir -p ./extract/lib64
echo "removing old extract container"
docker rm -f $EXTRACT_CONTAINER || true

oldpath=`pwd`
cd ..
  docker build --file docker/Dockerfile.build --tag $BUILD_CONTAINER_IMAGE .
cd $oldpath

#run tests in build container before accepting image
docker run --rm $BUILD_CONTAINER_IMAGE bin/test_parquet /src/tests/test_data
docker run --rm $BUILD_CONTAINER_IMAGE bin/test_decimal 

docker create --name $EXTRACT_CONTAINER $BUILD_CONTAINER_IMAGE

docker cp $EXTRACT_CONTAINER:/usr/local/lib                                      ./extract
find ./extract -name "*.a" -exec rm -rf {} \;

docker cp $EXTRACT_CONTAINER:/usr/local/bin/qtr-parquet-schema-gen-cpp           ./extract/bin
docker cp $EXTRACT_CONTAINER:/usr/local/bin/qtr-csv2parquet                      ./extract/bin
docker cp $EXTRACT_CONTAINER:/usr/local/bin/qtr-parquet2parquet                  ./extract/bin

docker cp $EXTRACT_CONTAINER:/src/runDeps                                        ./extract/runDeps

docker build -f Dockerfile --no-cache -t$TAG_NAME .

docker rm -f $EXTRACT_CONTAINER  || true
docker rmi -f $BUILD_CONTAINER_IMAGE  || true

rm -rf ./extract



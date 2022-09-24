#!/bin/bash

#  Copyright 2019 U.C. Berkeley RISE Lab
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

args=( -j -b -t )
containsElement() {
  local e match="$1"
  shift
  for e; do [[ "$e" == "$match" ]] && return 0; done
  return 1
}

while getopts ":j:b:tgcn" opt; do
  case $opt in
   j )
     MAKE_THREADS=$OPTARG
     if containsElement $OPTARG "${args[@]}"
     then
       echo "Missing argument to flag $opt"
       exit 1
     else
       echo "make set to run on $OPTARG threads" >&2
     fi
     ;;
   b )
     TYPE=$OPTARG
     if containsElement $OPTARG "${args[@]}"
     then
       echo "Missing argument to flag $opt"
       exit 1
     else
       echo "build type set to $OPTARG" >&2
     fi
     ;;
   t )
     TEST="-DBUILD_TEST=ON"
     echo "Testing enabled..."
     ;;
   g )
     COMPILER="/usr/bin/g++"
     RUN_FORMAT=""
     echo "Compiler set to GNU g++..."
     ;;
   c )
     CLOVER_COMPILE="-DCLOVER_KVS_COMPILE=ON"
     echo "Clover kvs compile enabled..."
     ;;
   n )
     SHARED_NOTHING_COMPILE="-DSHARED_NOTHING_KVS_COMPILE=ON"
     echo "Shared nothing compile enabled..."
     ;;
   \? )
     echo "Invalid option: -$OPTARG" >&2
     exit 1
     ;;
  esac
done

if [[ -z "$MAKE_THREADS" ]]; then MAKE_THREADS=16; fi
if [[ -z "$TYPE" ]]; then TYPE=Release; fi
if [[ -z "$TEST" ]]; then TEST=""; fi
if [[ -z "$COMPILER" ]]; then
  COMPILER="/usr/bin/clang++"
  RUN_FORMAT="yes"
fi
if [[ -z "$CLOVER_COMPILE" ]]; then CLOVER_COMPILE=""; fi
if [[ -z "$SHARED_NOTHING_COMPILE" ]]; then SHARED_NOTHING_COMPILE=""; fi

cd src/kvs/libbloom/
make -j
cd ../../../

#rm -rf build
mkdir build
cd build
rm CMakeCache.txt

cmake -std=c++11 "-GUnix Makefiles" -DCMAKE_BUILD_TYPE=$TYPE -DCMAKE_CXX_COMPILER=$COMPILER $TEST $CLOVER_COMPILE $SHARED_NOTHING_COMPILE ..

make -j${MAKE_THREADS}

if [[ "$TYPE" = "Debug" ]] && [[ ! -z "$RUN_FORMAT" ]]; then
  make clang-format
fi

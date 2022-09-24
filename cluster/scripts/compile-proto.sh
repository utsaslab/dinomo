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

if [ -z "$(command -v protoc)" ]; then
  echo "The protoc tool is required before you can run Hydro locally."
  echo "Please install protoc manually, or use the scripts in" \
    "DINOMO/common to install dependencies before proceeding."
  exit 1
fi

cd $DINOMO_HOME/cluster

# Set up the shared Python package to put compile Protobuf definitions in.
rm -rf $DINOMO_HOME/cluster/dinomo/shared/proto
mkdir $DINOMO_HOME/cluster/dinomo/shared/proto
touch $DINOMO_HOME/cluster/dinomo/shared/proto/__init__.py

cd $DINOMO_HOME
# Compile shared Protobufs.
protoc -I=common/proto --python_out=$DINOMO_HOME/cluster/dinomo/shared/proto shared.proto

# Compile the Protobufs to receive Anna metadata.
protoc -I=include/proto --python_out=$DINOMO_HOME/cluster/dinomo/shared/proto metadata.proto

cd $DINOMO_HOME/cluster

# NOTE: This is a hack. We have to do this because the protobufs are not
# packaged properly (in the protobuf definitions). This isn't an issue for C++
# builds, because all the header files are in one place, but it breaks our
# Python imports. Consider how to fix this in the future.
if [[ "$OSTYPE" = "darwin"* ]]; then
  sed -i '' "s/import shared_pb2/from . import shared_pb2/g" $(find dinomo/shared/proto | grep pb2 | grep -v pyc | grep -v internal)
else
  # We assume other linux distributions
  sed -i "s|import shared_pb2|from . import shared_pb2|g" $(find dinomo/shared/proto | grep pb2 | grep -v pyc | grep -v internal)
fi

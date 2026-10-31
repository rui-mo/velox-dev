# Copyright (c) Facebook, Inc. and its affiliates.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# shellcheck shell=bash

# Environment of the Spark Connect server used as the reference DB by the Velox
# Spark fuzzers. setup.sh copies this file to $SPARK_HOME/conf and replaces
# @SPARK_FUZZER_HOME@.
export TZ=America/Los_Angeles
export SPARK_LOG_DIR=@SPARK_FUZZER_HOME@/logs
export SPARK_PID_DIR=@SPARK_FUZZER_HOME@/run

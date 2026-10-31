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

# Common settings of the Spark fuzzer environment. Sourced by the other scripts.
# The SPARK_FUZZER_* variables override the defaults. SPARK_HOME and
# SPARK_VERSION are ignored so that the scripts never change another Spark
# installation.

SPARK_FUZZER_HOME=${SPARK_FUZZER_HOME:-$HOME/spark-fuzzer}
SPARK_VERSION=${SPARK_FUZZER_SPARK_VERSION:-4.3.0}
SPARK_HOME=${SPARK_FUZZER_SPARK_HOME:-$SPARK_FUZZER_HOME/spark-$SPARK_VERSION-bin-hadoop3}
SPARK_CONNECT_PORT=${SPARK_CONNECT_PORT:-15002}

# Spark 4.3 runs on Java 17, 21 and 25. Prefer Java 21: like Velox, its
# Double.toString and Float.toString return the shortest digits that round-trip.
# Java 17 sometimes returns more digits (JDK-4511638), e.g. for 19% of floats in
# scientific notation. Set SPARK_FUZZER_JAVA_HOME to use another Java.
if [[ -n ${SPARK_FUZZER_JAVA_HOME:-} ]]; then
  JAVA_HOME=$SPARK_FUZZER_JAVA_HOME
else
  for candidate in "$SPARK_FUZZER_HOME"/jdk/jdk-21* /usr/lib/jvm/java-21-openjdk* \
    /usr/lib/jvm/temurin-21-jdk* "${JAVA_HOME:-}" /usr/lib/jvm/java-17-openjdk*; do
    if [[ -n $candidate && -x $candidate/bin/java ]]; then
      JAVA_HOME=$candidate
      break
    fi
  done
fi

export SPARK_VERSION SPARK_FUZZER_HOME SPARK_HOME SPARK_CONNECT_PORT JAVA_HOME

#!/usr/bin/env bash
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

# Sets up the Spark Connect server used as the reference DB by the Velox Spark
# fuzzers in $SPARK_FUZZER_HOME, ~/spark-fuzzer by default:
#   - Java 21, downloaded from Adoptium unless it is installed.
#   - Spark with the configs in conf/.
#   - A Python venv with the Spark Connect client for 'spark_fuzzer.py sql'.
# Steps that are done are skipped. Rerun it after changing the configs, then
# restart the server with 'spark-server.sh restart'.
#
# Usage: setup.sh

set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=scripts/spark-fuzzer/env.sh
source "$SCRIPT_DIR/env.sh"
DOWNLOADS=$SPARK_FUZZER_HOME/downloads

fail() {
  echo "ERROR: $*" >&2
  exit 1
}

check_prerequisites() {
  local tool
  [[ $(uname -s) == Linux ]] || fail "Only Linux is supported."
  [[ $SPARK_FUZZER_HOME == /* ]] || fail "SPARK_FUZZER_HOME must be an absolute path."
  for tool in curl tar sha256sum sha512sum python3; do
    command -v "$tool" >/dev/null || fail "$tool is not installed."
  done
  mkdir -p "$DOWNLOADS"
}

# Prints the major version of the Java in $1.
java_version() {
  "$1/bin/java" -version 2>&1 | sed -nE '1s/.*version "([0-9]+).*/\1/p'
}

# Downloads URL $1 to file $2 unless the file exists.
download() {
  if [[ -f $2 ]]; then
    return
  fi
  echo "Downloading $1."
  if ! curl -fL --retry 3 -o "$2.part" "$1"; then
    rm -f "$2.part"
    return 1
  fi
  mv "$2.part" "$2"
}

# Fails and removes file $1 if its checksum isn't $3. $2 is sha256 or sha512.
verify_checksum() {
  if [[ $("${2}sum" "$1" | cut -d' ' -f1) != "$3" ]]; then
    rm -f "$1"
    fail "The checksum of $1 doesn't match. Removed it. Rerun to download it again."
  fi
}

install_java() {
  if [[ -n ${SPARK_FUZZER_JAVA_HOME:-} ]]; then
    echo "Using Java $(java_version "$JAVA_HOME") in $JAVA_HOME."
    return
  fi
  if [[ -x ${JAVA_HOME:-}/bin/java && $(java_version "$JAVA_HOME") == 21 ]]; then
    echo "Using Java 21 in $JAVA_HOME."
    return
  fi
  local arch metadata name checksum link
  case $(uname -m) in
  x86_64) arch=x64 ;;
  aarch64) arch=aarch64 ;;
  *) fail "Set SPARK_FUZZER_JAVA_HOME to a Java 21 for $(uname -m)." ;;
  esac
  metadata=$DOWNLOADS/temurin-21-$arch.json
  curl -fsSL --retry 3 -o "$metadata" \
    "https://api.adoptium.net/v3/assets/latest/21/hotspot?architecture=$arch&image_type=jdk&os=linux&vendor=eclipse"
  read -r name checksum link < <(python3 -c '
import json, sys
package = json.load(open(sys.argv[1]))[0]["binary"]["package"]
print(package["name"], package["checksum"], package["link"])
' "$metadata")
  download "$link" "$DOWNLOADS/$name" || fail "Couldn't download $link."
  verify_checksum "$DOWNLOADS/$name" sha256 "$checksum"
  mkdir -p "$SPARK_FUZZER_HOME/jdk"
  tar -xzf "$DOWNLOADS/$name" -C "$SPARK_FUZZER_HOME/jdk"
  # env.sh prefers the Java in $SPARK_FUZZER_HOME/jdk.
  unset JAVA_HOME
  # shellcheck source=scripts/spark-fuzzer/env.sh
  source "$SCRIPT_DIR/env.sh"
  [[ $(java_version "$JAVA_HOME") == 21 ]] || fail "Couldn't install Java 21."
  echo "Installed Java 21 in $JAVA_HOME."
}

install_spark() {
  if [[ -x $SPARK_HOME/sbin/start-connect-server.sh ]]; then
    echo "Using Spark in $SPARK_HOME."
    return
  fi
  local name=spark-$SPARK_VERSION-bin-hadoop3.tgz server checksum
  # Older releases are only on the archive server.
  for server in https://dlcdn.apache.org/spark https://archive.apache.org/dist/spark; do
    download "$server/spark-$SPARK_VERSION/$name" "$DOWNLOADS/$name" && break
  done
  [[ -f $DOWNLOADS/$name ]] || fail "Couldn't download $name."
  # The checksums are not on the CDN.
  for server in https://downloads.apache.org/spark https://archive.apache.org/dist/spark; do
    download "$server/spark-$SPARK_VERSION/$name.sha512" "$DOWNLOADS/$name.sha512" && break
  done
  [[ -f $DOWNLOADS/$name.sha512 ]] || fail "Couldn't download $name.sha512."
  # The file contains '<checksum>  <name>', or '<name>: <checksum>' with
  # uppercase hex digits in groups of 8 for older releases.
  checksum=$(sed -E 's/^[^:]*: //' "$DOWNLOADS/$name.sha512" | tr -d ' \n' |
    tr 'A-F' 'a-f' | grep -oE '^[0-9a-f]{128}' || true)
  [[ -n $checksum ]] || fail "Couldn't read the checksum in $DOWNLOADS/$name.sha512."
  verify_checksum "$DOWNLOADS/$name" sha512 "$checksum"
  mkdir -p "$SPARK_HOME"
  tar -xzf "$DOWNLOADS/$name" -C "$SPARK_HOME" --strip-components=1
  echo "Installed Spark $SPARK_VERSION in $SPARK_HOME."
}

install_configs() {
  local file
  for file in "$SCRIPT_DIR"/conf/*; do
    sed "s|@SPARK_FUZZER_HOME@|$SPARK_FUZZER_HOME|g" "$file" \
      >"$SPARK_HOME/conf/$(basename "$file")"
  done
  mkdir -p "$SPARK_FUZZER_HOME"/{logs,run,tmp,spark-warehouse}
  echo "Installed the configs in $SPARK_HOME/conf."
}

# Returns whether Python $1 is 3.11 or later, which pyspark-client 4.3 needs.
is_supported_python() {
  "$1" -c 'import sys; sys.exit(sys.version_info < (3, 11))' 2>/dev/null
}

# Prints the path of a supported Python. Downloads one with uv if none is
# installed.
find_python() {
  local python
  for python in python3 python3.13 python3.12 python3.11; do
    if command -v "$python" >/dev/null && is_supported_python "$python"; then
      command -v "$python"
      return
    fi
  done
  if command -v uv >/dev/null && uv python install --quiet 3.12 >&2; then
    uv python find 3.12 || true
  fi
}

install_python_client() {
  local venv=$SPARK_FUZZER_HOME/venv python
  if [[ ! -x $venv/bin/pip ]] || ! is_supported_python "$venv/bin/python"; then
    python=$(find_python)
    if [[ -z $python ]]; then
      echo "WARNING: Skipped the Spark Connect Python client, which needs Python" \
        "3.11 or later. Install it or uv. 'spark_fuzzer.py sql' won't work." >&2
      return
    fi
    rm -rf "$venv"
    if ! "$python" -m venv "$venv"; then
      rm -rf "$venv"
      fail "Couldn't create a Python venv. Install the venv module, e.g. python3-venv."
    fi
  fi
  # PySpark doesn't fully support pandas 3 yet.
  "$venv/bin/pip" install --quiet --disable-pip-version-check \
    "pyspark-client==$SPARK_VERSION" "pandas<3"
  echo "Installed pyspark-client $SPARK_VERSION in $venv."
}

check_prerequisites
install_java
install_spark
install_configs
install_python_client
echo "Done. Start the Spark Connect server with '$SCRIPT_DIR/spark-server.sh start'" \
  "or restart it with '$SCRIPT_DIR/spark-server.sh restart'."

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

# Manages the Spark Connect server used as the reference DB by the Velox Spark
# fuzzers.
#
# Usage: spark-server.sh start|stop|restart|status|log

set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=scripts/spark-fuzzer/env.sh
source "$SCRIPT_DIR/env.sh"

is_up() {
  (echo >"/dev/tcp/localhost/$SPARK_CONNECT_PORT") >/dev/null 2>&1
}

# Prints the PIDs of the Spark Connect servers of $SPARK_HOME that listen on
# $SPARK_CONNECT_PORT. Scans /proc because the PID file may be stale, e.g. after
# the server crashed, and ss, lsof and fuser are not always installed.
server_pids() {
  local sockets cmdline pid
  # In /proc/net/tcp*, field 2 is the local address, field 4 the state (0A is
  # LISTEN) and field 10 the inode of the socket.
  sockets=$(awk -v port="$(printf ':%04X$' "$SPARK_CONNECT_PORT")" \
    '$4 == "0A" && $2 ~ port { printf "socket:[%s]\n", $10 }' \
    /proc/net/tcp /proc/net/tcp6 2>/dev/null)
  if [[ -z $sockets ]]; then
    return
  fi
  for cmdline in $(grep -lsF SparkConnectServer /proc/[0-9]*/cmdline); do
    if [[ $({ tr '\0' ' ' <"$cmdline"; } 2>/dev/null) != *"$SPARK_HOME/jars/"*SparkConnectServer* ]]; then
      continue
    fi
    pid=${cmdline#/proc/}
    pid=${pid%/cmdline}
    if find /proc/"$pid"/fd -maxdepth 1 -type l -printf '%l\n' 2>/dev/null |
      grep -qxFf <(echo "$sockets"); then
      echo "$pid"
    fi
  done
}

any_alive() {
  local pid
  for pid in "$@"; do
    if kill -0 "$pid" 2>/dev/null; then
      return 0
    fi
  done
  return 1
}

log_file() {
  ls -t "$SPARK_FUZZER_HOME"/logs/*SparkConnectServer*.out 2>/dev/null | head -1
}

start() {
  if is_up; then
    echo "Spark Connect server is already running on port $SPARK_CONNECT_PORT."
    return
  fi
  if [[ ! -x $SPARK_HOME/sbin/start-connect-server.sh ]]; then
    echo "Spark is not installed in $SPARK_HOME. Run setup.sh first." >&2
    exit 1
  fi
  # A stale PID file makes Spark refuse to start the server.
  rm -f "$SPARK_FUZZER_HOME"/run/*SparkConnectServer*.pid
  "$SPARK_HOME"/sbin/start-connect-server.sh \
    --conf "spark.connect.grpc.binding.port=$SPARK_CONNECT_PORT"
  for _ in $(seq 1 120); do
    if is_up; then
      # Wait for the executor JVM to start.
      sleep 5
      echo "Spark Connect server is running on port $SPARK_CONNECT_PORT."
      return
    fi
    sleep 1
  done
  echo "Spark Connect server didn't start. See $(log_file)." >&2
  exit 1
}

stop() {
  local pids
  pids=$(server_pids)
  if [[ -z $pids ]]; then
    if is_up; then
      echo "Port $SPARK_CONNECT_PORT is used by a process other than a Spark" \
        "Connect server of $SPARK_HOME. Stop it manually." >&2
      exit 1
    fi
    echo "Spark Connect server is not running."
    return
  fi
  echo "Stopping Spark Connect server (PID ${pids//$'\n'/, })."
  # shellcheck disable=SC2086
  kill $pids
  for _ in $(seq 1 60); do
    # shellcheck disable=SC2086
    if ! any_alive $pids; then
      rm -f "$SPARK_FUZZER_HOME"/run/*SparkConnectServer*.pid
      return
    fi
    sleep 1
  done
  echo "Spark Connect server (PID ${pids//$'\n'/, }) didn't stop. Kill it" \
    "manually." >&2
  exit 1
}

case ${1:-} in
start) start ;;
stop) stop ;;
restart)
  stop
  start
  ;;
status)
  if is_up; then
    echo "Spark Connect server is running on port $SPARK_CONNECT_PORT."
  else
    echo "Spark Connect server is not running."
    exit 1
  fi
  ;;
log) log_file ;;
*)
  echo "Usage: $0 start|stop|restart|status|log" >&2
  exit 1
  ;;
esac

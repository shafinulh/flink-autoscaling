#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="${FROCKSDB_SRC:-/opt/frocksdb}"

if [[ ! -d "${ROOT_DIR}" ]]; then
  echo "ERROR: ${ROOT_DIR} not found. Set FROCKSDB_SRC or use /opt/frocksdb." >&2
  exit 1
fi

cd "${ROOT_DIR}"

# Make sure Snappy headers are available before building:
#   sudo apt update
#   sudo apt install -y libsnappy-dev
make clean-rocksjava
make -j12 DEBUG_LEVEL=0 USE_RTTI=1 SNAPPY=1 rocksdbjava EXTRA_CXXFLAGS="-I. -include cstdint"

# Install the JNI jar into local Maven cache so Flink can resolve the
# com.ververica:frocksdbjni dependency without pulling the upstream binary.
#   mvn install:install-file \
#     -Dfile=/opt/frocksdb/java/target/rocksdbjni-6.20.3-linux64.jar \
#     -DgroupId=com.ververica \
#     -DartifactId=frocksdbjni \
#     -Dversion=6.20.3-ververica-2.0 \
#     -Dpackaging=jar

#!/usr/bin/env bash
set -euo pipefail

# Start the HDFS cluster
cd "$(dirname "$0")"
docker compose up -d

echo "Waiting for NameNode to leave safe mode..."
for i in $(seq 1 60); do
    if docker compose exec namenode hdfs dfsadmin -safemode get 2>/dev/null | grep -q "OFF"; then
        echo "NameNode is ready."
        break
    fi
    if [ "$i" -eq 60 ]; then
        echo "ERROR: NameNode did not leave safe mode in time."
        exit 1
    fi
    sleep 2
done

echo "Creating test data..."

# 1 KB file
docker compose exec namenode bash -c 'dd if=/dev/urandom bs=1024 count=1 2>/dev/null > /tmp/testfile-small'
# 10 MB file (multi-block)
docker compose exec namenode bash -c 'dd if=/dev/urandom bs=1048576 count=10 2>/dev/null > /tmp/testfile-medium'
# 512 bytes (exactly one checksum chunk)
docker compose exec namenode bash -c 'dd if=/dev/urandom bs=512 count=1 2>/dev/null > /tmp/testfile-exact'

echo "Uploading to HDFS..."
docker compose exec namenode hdfs dfs -mkdir -p /test
docker compose exec namenode hdfs dfs -put -f /tmp/testfile-small /test/small.dat
docker compose exec namenode hdfs dfs -put -f /tmp/testfile-medium /test/medium.dat
docker compose exec namenode hdfs dfs -put -f /tmp/testfile-exact /test/exact.dat

echo "Saving local copies for comparison..."
docker compose cp namenode:/tmp/testfile-small ./testdata-small.dat
docker compose cp namenode:/tmp/testfile-medium ./testdata-medium.dat
docker compose cp namenode:/tmp/testfile-exact ./testdata-exact.dat

echo ""
echo "Block locations:"
docker compose exec namenode hdfs fsck /test -files -blocks -locations 2>/dev/null | grep -E "(^/test|Block|_blk_)"

echo ""
echo "Setup complete. Set HDFS_TEST_ENABLED=true to run integration tests."
echo "To tear down: docker compose down -v"

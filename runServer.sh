#!/bin/bash
mkdir server/target/tp-server-1.0-SNAPSHOT
tar -xzf server/target/tp-server-1.0-SNAPSHOT-bin.tar.gz -C server/target
chmod +x server/target/tp-server-1.0-SNAPSHOT/run-server.sh
cd server/target/tp-server-1.0-SNAPSHOT
./run-server.sh "$@"

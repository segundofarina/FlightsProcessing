#!/bin/bash
mkdir client/target/tp-client-1.0-SNAPSHOT
tar -xzf client/target/tp-client-1.0-SNAPSHOT-bin.tar.gz -C client/target
chmod +x client/target/tp-client-1.0-SNAPSHOT/run-client.sh
cd client/target/tp-client-1.0-SNAPSHOT
./run-client.sh "$@"

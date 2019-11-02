#!/bin/bash

mvn clean install

cd ./client/target
tar -xzf rmi-client-1.0-SNAPSHOT.tar.gz
chmod -R +x ./rmi-client-1.0-SNAPSHOT

cd ../../server/target
tar -xzf rmi-server-1.0-SNAPSHOT.tar.gz
chmod -R +x ./rmi-server-1.0-SNAPSHOT

cd ../../
chmod -R +x ./run-server.sh

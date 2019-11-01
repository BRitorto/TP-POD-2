#!/bin/bash

cd ./server/target/rmi-server-1.0-SNAPSHOT
chmod -R +x ./run-server.sh
sh ./run-server.sh $1
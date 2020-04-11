#!/usr/bin/env bash

python3 -m src.HydraBroker &

# Wait for broker to boot up:
sleep 2

export NGSI_ENDPOINT=http://localhost:5000/
export USERNAME=john
export PASSWORD=doe

python3 -m unittest tests

kill `cat hydrabroker.pid`
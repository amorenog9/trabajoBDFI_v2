#!/bin/bash

mongod &
./resources/import_distances.sh &
sleep infinity

#!/usr/bin/env bash

./start-master.sh &
./start-worker.sh spark://bdfi-v2.europe-southwest1-a.c.lively-welder-367712.internal:7077 &
../bin/spark-submit --master spark://bdfi-v2.europe-southwest1-a.c.lively-welder-367712.internal:7077 --class "es.upm.dit.ging.predictor.MakePrediction" --packages org.mongodb.spark:mongo-spark-connector_2.12:3.0.1,org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2 /home/user1/trabajoBDFI/practica_big_data_2019/flight_prediction/target/scala-2.12/flight_prediction_2.12-0.1.jar



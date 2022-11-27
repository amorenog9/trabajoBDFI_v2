#!/usr/bin/env bash

airflow webserver --port 8084 & 
airflow scheduler


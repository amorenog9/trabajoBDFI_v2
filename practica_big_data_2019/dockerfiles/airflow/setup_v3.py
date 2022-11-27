import sys, os, re

from airflow import DAG
from airflow.operators.bash import BashOperator

from datetime import datetime, timedelta
import iso8601

PROJECT_HOME = os.getenv("PROJECT_HOME")
SPARK_HOME = os.getenv("SPARK_HOME")
SPARK_URL= os.getenv("SPARK_URL")



default_args = {
  'owner': 'airflow',
  'depends_on_past': False,
  'start_date': iso8601.parse_date("2016-12-01"),
  'retries': 3,
  'retry_delay': timedelta(minutes=5),
}

training_dag = DAG(
  'agile_data_science_batch_prediction_model_training_cluster',
  default_args=default_args,
  schedule_interval=None
)

# We use the same two commands for all our PySpark tasks
pyspark_bash_command = """{{ params.spark_path }}/bin/spark-submit --master {{ params.master }} --class "es.upm.dit.ging.predictor.MakePrediction" --packages org.mongodb.spark:mongo-spark-connector_2.12:3.0.1,org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2 {{ params.base_path }}/flight_prediction/target/scala-2.12/flight_prediction_2.12-0.1.jar"""

# Gather the training data for our classifier

extract_features_operator = BashOperator(
  task_id = "pyspark_extract_features_cluster",
  bash_command = pyspark_bash_command,
  params = {
    "master": "{}".format(SPARK_URL),
    "base_path": "{}".format(PROJECT_HOME),
    "spark_path": "{}".format(SPARK_HOME)
  },
  dag=training_dag
)


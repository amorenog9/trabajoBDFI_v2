FROM openjdk:8

WORKDIR /home/user1/trabajoBDFI


# Spark
RUN wget https://archive.apache.org/dist/spark/spark-3.1.2/spark-3.1.2-bin-hadoop2.7.tgz 
RUN tar -xzvf spark-3.1.2-bin-hadoop2.7.tgz

# Clonar repositorio
RUN	git clone https://github.com/jgonzori3/Trabajo-BDFI.git
# Copiamos el directorio en /app/practica_big_data_2019
RUN	mv Trabajo-BDFI/practica_big_data_2019 . 


WORKDIR /home/user1/trabajoBDFI/spark-3.1.2-bin-hadoop2.7/bin

CMD ./spark-submit --master local[*] --class "es.upm.dit.ging.predictor.MakePrediction" --packages org.mongodb.spark:mongo-spark-connector_2.12:3.0.1,org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2 /home/user1/trabajoBDFI/practica_big_data_2019/flight_prediction/target/scala-2.12/flight_prediction_2.12-0.1.jar



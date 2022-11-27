FROM openjdk:8
WORKDIR /app

RUN apt-get update 
RUN apt-get upgrade -y 

# Descargamos zookeper en /app
RUN wget https://dlcdn.apache.org/zookeeper/zookeeper-3.8.0/apache-zookeeper-3.8.0-bin.tar.gz
RUN tar -xvzf apache-zookeeper-3.8.0-bin.tar.gz


# Descargamos kafka en /app
RUN wget https://archive.apache.org/dist/kafka/3.0.0/kafka_2.12-3.0.0.tgz
RUN tar -xvzf kafka_2.12-3.0.0.tgz


WORKDIR /app/kafka_2.12-3.0.0

CMD bin/zookeeper-server-start.sh config/zookeeper.properties

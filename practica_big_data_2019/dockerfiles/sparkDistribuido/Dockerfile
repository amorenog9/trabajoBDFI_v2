FROM openjdk:8

WORKDIR /home/user1/trabajoBDFI

# Spark
RUN wget https://archive.apache.org/dist/spark/spark-3.1.2/spark-3.1.2-bin-hadoop2.7.tgz 
RUN tar -xzvf spark-3.1.2-bin-hadoop2.7.tgz


# Clonar repositorio
RUN	git clone https://github.com/jgonzori3/trabajoBDFI.git
# Copiamos el directorio en /app/practica_big_data_2019
RUN	mv trabajoBDFI/practica_big_data_2019 . 

COPY spark-env.sh /home/user1/trabajoBDFI/spark-3.1.2-bin-hadoop2.7/conf

WORKDIR /home/user1/trabajoBDFI/spark-3.1.2-bin-hadoop2.7/conf

RUN chmod 777 spark-env.sh

WORKDIR /home/user1/trabajoBDFI/spark-3.1.2-bin-hadoop2.7/sbin

COPY script.sh /home/user1/trabajoBDFI/spark-3.1.2-bin-hadoop2.7/sbin

RUN chmod 777 script.sh

CMD ./script.sh














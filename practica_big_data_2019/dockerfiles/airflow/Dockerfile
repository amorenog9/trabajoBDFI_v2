
#FROM python:3.8

FROM ubuntu:20.04

ENV JAVA_HOME /usr/lib/jvm/java-8-openjdk-amd64/

RUN apt-get update -y \
&& apt-get install -y software-properties-common \
&& add-apt-repository ppa:deadsnakes/ppa \
&& apt-get install openjdk-8-jdk -y \
&& apt-get install python3-pip -y \
&& export JAVA_HOME \
&& apt-get clean \
&& rm -rf /var/lib/apt/lists/*

RUN apt update
RUN apt install wget


WORKDIR /home/user1/trabajoBDFI



RUN	apt-get update 
RUN	apt-get upgrade -y 

RUN	apt-get install git -y 

RUN	git clone https://github.com/jgonzori3/trabajoBDFI.git

# Copiamos el directorio en /app/practica_big_data_2019
RUN	mv trabajoBDFI/practica_big_data_2019 . 



# Spark
RUN wget https://archive.apache.org/dist/spark/spark-3.1.2/spark-3.1.2-bin-hadoop2.7.tgz 
RUN tar -xzvf spark-3.1.2-bin-hadoop2.7.tgz

WORKDIR /home/user1/trabajoBDFI/practica_big_data_2019/resources/airflow


# INSTALAMOS APACHE AIRFLOW ENTRE OTRAS DEPENDENCIAS NECESARIAS
RUN pip install -r requirements.txt -c constraints.txt

# Con esto se crea la carpeta airflow y la bbdd inicial, donde vamos a meter el DAG. 
# La carpeta airflow va a estar en /root (se ha detectado viendo las trazas que daba el comando airflow db init)
RUN airflow db init

WORKDIR /root/airflow

RUN mkdir dags
RUN mkdir plugins

# Lo añadimos porque a veces habia problemas de escritura en carpeta
RUN chmod 777 dags
RUN chmod 777 plugins

# Ejemplo github
COPY setup.py /root/airflow/dags/

# Spark local
COPY setup_v2.py /root/airflow/dags/

# Spark cluster
COPY setup_v3.py /root/airflow/dags/



WORKDIR /root/airflow/dags
RUN chmod 777 setup.py

# Usuario para iniciar web
RUN airflow users create \
    --username admin \
    --firstname Jack \
    --lastname  Sparrow\
    --role Admin \
    --email example@mail.org\
    --password password 

COPY script.sh /home/user1/trabajoBDFI

WORKDIR /home/user1/trabajoBDFI

RUN chmod 777 script.sh

CMD ./script.sh









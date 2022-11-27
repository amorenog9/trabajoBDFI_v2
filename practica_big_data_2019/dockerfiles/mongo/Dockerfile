FROM mongo:4.4
WORKDIR /app

RUN apt-get update 
RUN	apt-get upgrade -y 



RUN apt-get install git -y 

RUN	git clone https://github.com/jgonzori3/trabajoBDFI.git

# Copiamos el directorio en /app/practica_big_data_2019
RUN	mv trabajoBDFI/practica_big_data_2019 . 

WORKDIR /app/practica_big_data_2019


COPY . /app/practica_big_data_2019

RUN chmod +x ./start.sh

CMD ./start.sh

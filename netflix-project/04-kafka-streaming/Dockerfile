FROM jedha/confluent-image

WORKDIR /home/app

RUN apt-get update
# GNU compiler
RUN apt-get -y install gcc
# SQL base setup
RUN apt-get -y install libpq-dev

RUN conda install -y python=3.11.3

COPY requirements.txt .

RUN pip install --upgrade pip
RUN pip install --upgrade -r requirements.txt

COPY . /home/app/

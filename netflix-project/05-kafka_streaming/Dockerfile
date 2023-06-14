FROM jedha/confluent-image

WORKDIR /home/app

RUN apt-get update
# GNU compiler
RUN apt-get -y install gcc
# SQL base setup
RUN apt-get -y install libpq-dev


COPY requirements.txt .

RUN pip install --upgrade pip
RUN pip install -r requirements.txt

COPY . /home/app/

ENTRYPOINT ["python", "producer.py"]
FROM debian
MAINTAINER Mark van der Net <info@oscity.nl>

RUN apt-get -y update
RUN apt-get -y install python python-dev python-pip libev-dev nano gcc libpq-dev curl
RUN apt-get -y install libaio1
RUN apt-get -y install python-mysqldb
RUN pip install psycopg2 bjoern

ADD requirements.txt /
RUN pip install -r requirements.txt

WORKDIR /app/

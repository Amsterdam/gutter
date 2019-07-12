FROM debian
MAINTAINER Mark van der Net <info@oscity.nl>

RUN apt-get -y update
RUN apt-get -y install python3 python3-dev python3-pip libev-dev nano gcc libpq-dev curl
RUN apt-get -y install libaio1
RUN apt-get -y install python-mysqldb

# in order to install 'mysqlclient' python lib (https://github.com/PyMySQL/mysqlclient-python):
RUN apt-get -y install default-libmysqlclient-dev

RUN pip3 install psycopg2 bjoern

ADD requirements.txt /
RUN pip3 install -r requirements.txt

WORKDIR /app/

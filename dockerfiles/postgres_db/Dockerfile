FROM mdillon/postgis:9.6-alpine
#FROM amsterdam/postgres11

MAINTAINER Mark van der Net <m.vandernet@amsterdam.nl>

# create schema on startup
COPY ./create_gutter_schema.sql /docker-entrypoint-initdb.d/create_gutter_schema.sql
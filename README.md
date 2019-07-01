# Gutter

![gutter-logo](images/logo.png)

An API generator packed with ETL pipelines, data syncing and an analytics engine.

Created within the CTO of the Municipality of Amsterdam.

## Getting started

### Run locally

```
$ docker-compose up (-d for deamonization)
```

This will start the Gutter API, phpPgAdmin and Postgres.
The first two have a web interface and can be accessed through a web browser:

Gutter: http://localhost:8004

phpPgAdmin: http://localhost:8002


### Admin API

In order to use the Admin API an user with admin rights is required. When starting Gutter a default admin user is created. The credentials of this user can be found and changed in the docker-compose environment variables `GUTTER_ADMIN_USER` and `GUTTER_ADMIN_PASSWORD`.

#### Login (as admin user) to Gutter to receive token
```
$ curl --header "Content-Type: application/json" --request POST --data "{
    \"username\": \"admin\",
    \"password\": \"amsterdam\"
}" http://localhost:8004/login
```

#### Create API as a Service

- First set the token as environment variable (token can also be set directly in curl):
```
$ GUTTER_TOKEN=*copy the token received at login here*
```

- Create endpoint with curl command:
```
$ curl --header "Content-Type: application/json" --header "Authorization: Bearer $GUTTER_TOKEN" --request POST --data "{
    \"title\": \"my_new_endpoint\",
    \"properties\": {
            \"id\":          {\"type\": \"string\"},                                                                
            \"description\": {\"type\": \"string\"},
            \"some_number\": {\"type\": \"number\"}
        }
}" http://localhost:8004/admin/endpoints

```

#### Create new API user

Also a new user can be created through the Admin API. Provide an admin-token (see 'Login' & 'Create API as a Service') and post some JSON with `email` and `password` defined.

```
$ curl --header "Content-Type: application/json" --header "Authorization: Bearer $GUTTER_TOKEN" --request POST --data "{
    \"email\": \"new_api_user\",
    \"password\": \"secure_password\"
}" http://localhost:8004/admin/users

```

#### Some more endpoints

```
$ curl --header "Content-Type: application/json" --header "Authorization: Bearer $GUTTER_TOKEN" --request GET <endpoint_url>
```

- Get all endpoints: `<base_url>/admin/endpoints`
- Get a single endpoint: `<base_url>/admin/endpoints/<name_endpoint>`
- Reload endpoints: `<base_url>/admin/reload`
- Check time of last reload: `<base_url>/admin/started`


## Library parts

#### Currently
* flow.GutterFlow - ETL manager to get data into Gutter, define a pipeline (with source) and insert/update data to GutterStore
* datastore.GutterStore - Manager to store data with metadata with schema definitions
* apicentral.ApiCentral - Manage API's based on the schemas in datastore

#### Future
* analytics.Analytics - Schedule analytics jobs to be ran periodically using Celery Beat
* cms.GutterCMS - Manager data in GutterStore


## Common entities definitions

| Name | Description |
| ---- | ----------- |
| Schema definition | A JSON SCHEMA definition of the data
| Database schema | Location of table in database
| Model | An operationalized schema with connection, database as SQLAlchemy class
| Row | An instantiated model with data
| Pipeline | The way the (meta)data flow from source to target
| Pipeline source | Database source { name, url, user, password, type, port, schema, table }
| Map (source_target_map) | A way to map from source to target ( basically a key,value in which value someday might contain functions and multiple inputs/models ) - map can contain quasi python code like: 'field1 + field2' or 'lower(field1)'
| Pipeline Map (map_source_target) | Maps incoming datarows to that of gutter row and output: { 'propname1_source' : 'propname1_target' } etc.



## Built with
* [Postgres](https://www.postgresql.org/) - Data persistence
* [Python](https://www.python.org/) - Platform
* [Bjoern](https://github.com/jonashaag/bjoern) - WSGI
* [Flask](http://flask.pocoo.org/) - Web app
* [SQLAlchemy](https://www.sqlalchemy.org/) - ORM
* [Flask-RESTful](https://flask-restful.readthedocs.io) - API

## Authors

- Mark van der Net
- Stan Guldemond
# Movies API
Movies API with FastAPI

## Prerequisites
To use these files, you must first have the following installed:

- [Docker](https://docs.docker.com/engine/installation/)
- [docker-compose](https://docs.docker.com/compose/install/)

## How to use

The following steps will run a local instance of the Movies API using 
the default configuration file (docker-compose.yml):
1. Clone this repository
2. Update your REDIS_PASSWORD in .env or docker-compose.yml
3. Run the `docker-compose up` command 
```bash
git clone https://github.com/vlad397/FastApi
cd  FastApi
docker-compose up
```
4. Open local API docs [http://127.0.0.1/api/openapi](http://127.0.0.1/api/openapi)